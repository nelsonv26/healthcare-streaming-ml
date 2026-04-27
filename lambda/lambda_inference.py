import json
import hashlib
import logging
import os
import time
from datetime import datetime, timezone

import boto3

log = logging.getLogger(__name__)
log.setLevel(os.getenv('LOG_LEVEL', 'INFO'))

_s3      = boto3.client('s3')
_dynamo  = boto3.resource('dynamodb')

S3_BUCKET   = os.environ['S3_PROCESSED_BUCKET']
DYNAMO_TABLE = os.environ['DYNAMODB_TABLE_AWS']


# ─── Predicción de riesgo ─────────────────────────────────────────────────────
# Reimplementación del modelo Random Forest como reglas Python puras.
# Equivalente funcional: el modelo fue entrenado con etiquetas generadas por
# este mismo algoritmo determinístico (ml_label = 1 if risk_score >= 60).

def _risk_level(score: int) -> str:
    if score >= 70: return 'CRITICAL'
    if score >= 50: return 'HIGH'
    if score >= 30: return 'MEDIUM'
    return 'LOW'


def predict_risk(event: dict) -> dict:
    risk  = 0
    flags = []

    if event.get('anesthesia_type') == 'GENERAL':
        risk += 25
        if event.get('diabetic'):
            risk += 20
            flags.append('GENERAL_ANESTHESIA_DIABETIC')
        if event.get('hypertensive'):
            risk += 10
            flags.append('GENERAL_ANESTHESIA_HYPERTENSIVE')

    if event.get('is_minor'):
        risk += 15
        flags.append('MINOR_PATIENT')

    if event.get('smoker'):
        risk += 10
        flags.append('SMOKER')

    if event.get('patient_bmi', 0) > 35:
        risk += 10
        flags.append('HIGH_BMI')

    if event.get('consent_to_surgery_hours', 99) < 12:
        risk += 15
        flags.append('SHORT_CONSENT_WINDOW')

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
        'prediction_method': 'rules_v1',
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
        'prediction_method': result['prediction_method'],
        'procedure_type':    consent_event.get('procedure_type', 'unknown'),
        'clinic_id':         consent_event.get('clinic_id', 'unknown'),
        'consent_signed':    consent_event.get('consent_signed', False),
        'inference_at':      datetime.now(timezone.utc).isoformat(),
        'ttl':               int(time.time()) + 86400 * 30,
    })


# ─── Handler Lambda (disparado por SQS) ──────────────────────────────────────

def handler(event, context):
    results = []

    for record in event.get('Records', []):
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
                'messageId': message_id,
                'consent_id': consent_event.get('consent_id'),
                'status': 'ok',
                **result,
            })

        except Exception as exc:
            log.error(f"Error procesando {message_id}: {exc}", exc_info=True)
            results.append({'messageId': message_id, 'status': 'error', 'error': str(exc)})

    return {'statusCode': 200, 'body': json.dumps(results)}
