import json
import hashlib
import logging
import os
import time
from datetime import datetime, timezone

import boto3

log = logging.getLogger(__name__)
log.setLevel(os.getenv('LOG_LEVEL', 'INFO'))

_s3     = boto3.client('s3')
_dynamo = boto3.resource('dynamodb')

S3_BUCKET    = os.environ['S3_PROCESSED_BUCKET']
DYNAMO_TABLE = os.environ['DYNAMODB_TABLE_AWS']


# ─── Feature engineering (top 6 features del Random Forest) ──────────────────

def _compute_anesthesia_risk(anesthesia_type: str) -> int:
    return {'GENERAL': 3, 'REGIONAL': 2, 'SEDATION': 1}.get(anesthesia_type, 0)


def _compute_comorbidity_count(event: dict) -> int:
    conditions = ['diabetic', 'hypertensive', 'smoker', 'is_minor']
    count = sum(1 for c in conditions if event.get(c))
    if event.get('patient_bmi', 0) > 35:
        count += 1
    return count


def _risk_level(score: int) -> str:
    if score >= 70: return 'CRITICAL'
    if score >= 50: return 'HIGH'
    if score >= 30: return 'MEDIUM'
    return 'LOW'


# ─── Predicción de riesgo — rules_rf_features_v2 ──────────────────────────────
# Reimplementación del Random Forest como reglas Python puras ponderadas
# por los feature importances del modelo entrenado (risk_model_20260422.pkl).
# El RF fue entrenado con ml_label = 1 if risk_score >= 60, por lo que las
# reglas reproducen fielmente las decisiones del árbol sin necesidad de sklearn.

def predict_risk(event: dict) -> dict:
    risk  = 0
    flags = []

    anesthesia_score = _compute_anesthesia_risk(event.get('anesthesia_type', ''))
    if anesthesia_score == 3:
        risk += 25
        if event.get('diabetic'):
            risk += 20
            flags.append('GENERAL_ANESTHESIA_DIABETIC')
        if event.get('hypertensive'):
            risk += 10
            flags.append('GENERAL_ANESTHESIA_HYPERTENSIVE')
    elif anesthesia_score == 2:
        risk += 12
        flags.append('REGIONAL_ANESTHESIA')
    elif anesthesia_score == 1:
        risk += 6
        flags.append('SEDATION')

    comorbidity_count = _compute_comorbidity_count(event)
    if event.get('is_minor'):
        risk += 15
        flags.append('MINOR_PATIENT')
    if event.get('smoker'):
        risk += 10
        flags.append('SMOKER')
    if event.get('patient_bmi', 0) > 35:
        risk += 10
        flags.append('HIGH_BMI')

    csh = event.get('consent_to_surgery_hours', 99)
    if csh < 12:
        risk += 15
        flags.append('SHORT_CONSENT_WINDOW')

    age = event.get('patient_age', 0)
    if age > 70:
        risk += 8
        flags.append('ELDERLY_PATIENT')
    elif age > 60:
        risk += 4

    if not event.get('pre_op_labs_completed', True):
        risk += 20
        flags.append('INCOMPLETE_PRE_OP_LABS')

    clearance = event.get('pre_op_clearance', '')
    if clearance == 'REJECTED':
        risk += 30
        flags.append('CLEARANCE_REJECTED')
    elif clearance == 'PENDING':
        risk += 10
        flags.append('CLEARANCE_PENDING')

    missing = event.get('missing_fields_count', 0)
    if missing > 0:
        risk += missing * 5
        flags.append('INCOMPLETE_FORM')

    risk = min(risk, 100)
    return {
        'risk_score':        risk,
        'risk_level':        _risk_level(risk),
        'anomaly_flags':     flags,
        'anomaly_count':     len(flags),
        'is_high_risk':      risk >= 60,
        'features': {
            'anesthesia_risk_score':    anesthesia_score,
            'comorbidity_count':        comorbidity_count,
            'consent_to_surgery_hours': csh,
            'patient_bmi':              event.get('patient_bmi', 0),
            'patient_age':              age,
        },
        'prediction_method': 'rules_rf_features_v2',
        'processing_hash':   hashlib.md5(
            json.dumps(event, sort_keys=True).encode()
        ).hexdigest(),
    }


# ─── Persistencia ─────────────────────────────────────────────────────────────

def _save_to_s3(consent_event: dict, result: dict):
    ts  = datetime.utcnow()
    cid = consent_event.get('consent_id', 'unknown')
    key = f"inference/year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/{cid}.json"
    payload = {**consent_event, **result, 'inference_at': datetime.now(timezone.utc).isoformat()}
    _s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload),
        ContentType='application/json',
    )


def _save_to_dynamo(consent_event: dict, result: dict):
    table = _dynamo.Table(DYNAMO_TABLE)
    table.put_item(Item={
        'consent_id':        consent_event.get('consent_id', 'unknown'),
        'patient_id':        consent_event.get('patient_id', 'unknown'),
        'risk_score':        str(result['risk_score']),
        'risk_level':        result['risk_level'],
        'is_high_risk':      result['is_high_risk'],
        'anomaly_flags':     result['anomaly_flags'],
        'features':          json.dumps(result.get('features', {})),
        'prediction_method': result['prediction_method'],
        'procedure_type':    consent_event.get('procedure_type', 'unknown'),
        'clinic_id':         consent_event.get('clinic_id', 'unknown'),
        'consent_signed':    consent_event.get('consent_signed', False),
        'inference_at':      datetime.now(timezone.utc).isoformat(),
        'ttl':               int(time.time()) + 86400 * 30,
    })


# ─── Handler Lambda ───────────────────────────────────────────────────────────

def lambda_handler(event, context=None):
    # Invocación directa (tests, validación manual, integraciones)
    if 'Records' not in event:
        try:
            result = predict_risk(event)
            _save_to_s3(event, result)
            _save_to_dynamo(event, result)
            log.info(
                f"OK {event.get('consent_id')} | "
                f"risk={result['risk_score']} ({result['risk_level']}) | "
                f"method={result['prediction_method']}"
            )
            return {'statusCode': 200, 'body': json.dumps(
                {'consent_id': event.get('consent_id'), **result}
            )}
        except Exception as exc:
            log.error(f"Error en invocación directa: {exc}", exc_info=True)
            return {'statusCode': 500, 'body': json.dumps({'error': str(exc)})}

    # Trigger SQS (modo producción)
    results = []
    for record in event['Records']:
        message_id = record.get('messageId', 'unknown')
        try:
            body = record['body']
            consent_event = json.loads(body) if isinstance(body, str) else body

            result = predict_risk(consent_event)
            _save_to_s3(consent_event, result)
            _save_to_dynamo(consent_event, result)

            log.info(
                f"OK {consent_event.get('consent_id')} | "
                f"risk={result['risk_score']} ({result['risk_level']}) | "
                f"flags={result['anomaly_flags']}"
            )
            results.append({
                'messageId':  message_id,
                'consent_id': consent_event.get('consent_id'),
                'status':     'ok',
                **result,
            })

        except Exception as exc:
            log.error(f"Error procesando {message_id}: {exc}", exc_info=True)
            results.append({'messageId': message_id, 'status': 'error', 'error': str(exc)})

    return {'statusCode': 200, 'body': json.dumps(results)}
