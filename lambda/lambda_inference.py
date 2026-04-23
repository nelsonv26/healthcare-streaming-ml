import json
import boto3
import pickle
import os
import logging
import tempfile
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

S3_PROCESSED_BUCKET = os.environ['S3_PROCESSED_BUCKET']
MODEL_KEY           = os.environ['MODEL_KEY']
DYNAMO_TABLE        = os.environ['DYNAMODB_TABLE_AWS']

# Cache del modelo en memoria (se reutiliza entre invocaciones)
_model_cache = None

def load_model():
    global _model_cache
    if _model_cache is None:
        logger.info(f"Cargando modelo desde S3: {MODEL_KEY}")
        with tempfile.NamedTemporaryFile() as tmp:
            s3.download_file(S3_PROCESSED_BUCKET, MODEL_KEY, tmp.name)
            with open(tmp.name, 'rb') as f:
                _model_cache = pickle.load(f)
        logger.info("Modelo cargado en cache")
    return _model_cache

def build_features(event: dict) -> list:
    """Construye el vector de features igual que en el notebook."""
    anesthesia_map = {'LOCAL': 1, 'SEDATION': 2, 'GENERAL': 3}
    procedure_map  = {
        'RHINOPLASTY': 0, 'LIPOSUCTION': 1, 'BREAST_AUGMENTATION': 2,
        'ABDOMINOPLASTY': 3, 'BLEPHAROPLASTY': 4, 'FACELIFT': 5, 'OTHER': 6
    }
    clearance_map  = {'APPROVED': 0, 'PENDING': 1, 'REJECTED': 2}
    channel_map    = {'DIGITAL': 0, 'PAPER': 1, 'VIDEOCALL': 2, 'HYBRID': 3}
    gender_map     = {'M': 0, 'F': 1, 'OTHER': 2}

    return [
        event.get('patient_age', 0),
        event.get('patient_bmi', 0),
        event.get('estimated_duration_min', 0),
        event.get('consent_to_surgery_hours', 0),
        event.get('missing_fields_count', 0),
        event.get('previous_surgeries', 0),
        event.get('risk_acknowledgement', 3),
        int(bool(event.get('is_minor', False))),
        int(bool(event.get('smoker', False))),
        int(bool(event.get('diabetic', False))),
        int(bool(event.get('hypertensive', False))),
        int(bool(event.get('has_allergies', False))),
        int(bool(event.get('pre_op_labs_completed', True))),
        int(bool(event.get('consent_signed', True))),
        int(bool(event.get('witness_present', True))),
        event.get('surgery_complexity', 1),
        procedure_map.get(event.get('procedure_type', 'OTHER'), 6),
        anesthesia_map.get(event.get('anesthesia_type', 'LOCAL'), 1),
        clearance_map.get(event.get('pre_op_clearance', 'APPROVED'), 0),
        channel_map.get(event.get('consent_channel', 'PAPER'), 1),
        gender_map.get(event.get('patient_gender', 'F'), 1),
        event.get('kaggle_pain_level', 0),
    ]

def lambda_handler(event, context):
    """
    Acepta dos modos:
    1. Invocación directa con un evento de consentimiento
    2. Trigger de SQS con batch de eventos
    """
    try:
        model_data = load_model()
        model      = model_data['model']

        # Detectar si viene de SQS o invocación directa
        records = []
        if 'Records' in event:
            for record in event['Records']:
                records.append(json.loads(record['body']))
        else:
            records = [event]

        results = []
        for consent_event in records:
            features = build_features(consent_event)
            proba    = model.predict_proba([features])[0][1]
            label    = int(model.predict([features])[0])

            result = {
                'consent_id':        consent_event.get('consent_id'),
                'ml_risk_score':     round(float(proba) * 100, 1),
                'ml_prediction':     label,
                'ml_risk_label':     'HIGH_RISK' if label == 1 else 'LOW_RISK',
                'predicted_at':      datetime.now(timezone.utc).isoformat(),
            }

            # Actualizar DynamoDB con la predicción ML
            table = dynamodb.Table(DYNAMO_TABLE)
            table.update_item(
                Key={'consent_id': consent_event.get('consent_id')},
                UpdateExpression='SET ml_risk_score = :s, ml_prediction = :p, ml_risk_label = :l',
                ExpressionAttributeValues={
                    ':s': str(result['ml_risk_score']),
                    ':p': result['ml_prediction'],
                    ':l': result['ml_risk_label'],
                }
            )

            results.append(result)
            logger.info(f"Predicción: {result['consent_id'][:8]}... → {result['ml_risk_label']} ({result['ml_risk_score']}%)")

        return {'statusCode': 200, 'predictions': results}

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return {'statusCode': 500, 'error': str(e)}