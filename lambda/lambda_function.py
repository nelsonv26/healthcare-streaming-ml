import json
import time
import logging
import hashlib
import boto3
import os
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3       = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sqs      = boto3.client('sqs')

S3_RAW_BUCKET     = os.environ['S3_RAW_BUCKET']
S3_PROCESSED_BUCKET = os.environ['S3_PROCESSED_BUCKET']
DYNAMO_TABLE      = os.environ['DYNAMODB_TABLE_AWS']
SQS_PROCESSED_URL = os.environ['SQS_PROCESSED_URL']
SQS_DLQ_URL       = os.environ['SQS_DLQ_URL']

REQUIRED_FIELDS = [
    'consent_id','patient_id','patient_age','procedure_type',
    'consent_signed','anesthesia_type','clinic_id','surgeon_id',
    'scheduled_date','pre_op_clearance',
]

def validate(event: dict) -> tuple:
    errors = []
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            errors.append(f"missing_field:{field}")
    if errors:
        return False, errors
    if not (0 < event['patient_age'] < 120):
        errors.append("invalid_age")
    if event.get('is_minor') and not event.get('legal_guardian_required'):
        errors.append("minor_without_guardian_flag")
    return len(errors) == 0, errors

def enrich(event: dict) -> dict:
    enriched = event.copy()
    enriched['processed_at'] = datetime.now(timezone.utc).isoformat()
    risk, flags = 0, []

    if event.get('anesthesia_type') == 'GENERAL':
        risk += 25
        if event.get('diabetic'):     risk += 20; flags.append('GENERAL_ANESTHESIA_DIABETIC')
        if event.get('hypertensive'): risk += 10; flags.append('GENERAL_ANESTHESIA_HYPERTENSIVE')
    if event.get('is_minor'):         risk += 15; flags.append('MINOR_PATIENT')
    if event.get('smoker'):           risk += 10; flags.append('SMOKER')
    if event.get('patient_bmi', 0) > 35: risk += 10; flags.append('HIGH_BMI')
    if event.get('consent_to_surgery_hours', 999) < 12: risk += 15; flags.append('SHORT_CONSENT_WINDOW')
    if not event.get('pre_op_labs_completed'): risk += 20; flags.append('INCOMPLETE_PRE_OP_LABS')
    if event.get('pre_op_clearance') == 'REJECTED': risk += 30; flags.append('CLEARANCE_REJECTED')
    elif event.get('pre_op_clearance') == 'PENDING': risk += 10; flags.append('CLEARANCE_PENDING')
    if event.get('missing_fields_count', 0) > 0:
        risk += event['missing_fields_count'] * 5; flags.append('INCOMPLETE_FORM')

    enriched['risk_score']    = min(risk, 100)
    enriched['risk_level']    = _risk_level(risk)
    enriched['anomaly_flags'] = flags
    enriched['anomaly_count'] = len(flags)
    enriched['is_high_risk']  = risk >= 60
    enriched['processing_hash'] = hashlib.md5(
        json.dumps(event, sort_keys=True).encode()
    ).hexdigest()
    return enriched

def _risk_level(score: int) -> str:
    if score >= 70: return 'CRITICAL'
    if score >= 50: return 'HIGH'
    if score >= 30: return 'MEDIUM'
    return 'LOW'

def save_s3(bucket: str, event: dict):
    ts  = datetime.utcnow()
    key = f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/{event['consent_id']}.json"
    s3.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(event, default=str),
        ContentType='application/json',
    )

def save_dynamo(event: dict):
    table = dynamodb.Table(DYNAMO_TABLE)
    table.put_item(Item={
        'consent_id':     event['consent_id'],
        'patient_id':     event['patient_id'],
        'risk_score':     str(event['risk_score']),
        'risk_level':     event['risk_level'],
        'is_high_risk':   event['is_high_risk'],
        'anomaly_flags':  event['anomaly_flags'],
        'procedure_type': event['procedure_type'],
        'clinic_id':      event['clinic_id'],
        'processed_at':   event['processed_at'],
        'consent_signed': event['consent_signed'],
        'ttl':            int(time.time()) + 86400 * 30,
    })

def lambda_handler(event, context):
    """Handler principal — recibe eventos de SQS trigger."""
    processed, failed = 0, 0

    for record in event.get('Records', []):
        try:
            raw_event = json.loads(record['body'])

            # 1. Guardar raw
            save_s3(S3_RAW_BUCKET, raw_event)

            # 2. Validar
            is_valid, errors = validate(raw_event)
            if not is_valid:
                sqs.send_message(
                    QueueUrl=SQS_DLQ_URL,
                    MessageBody=json.dumps({'event': raw_event, 'errors': errors}, default=str),
                )
                failed += 1
                continue

            # 3. Enriquecer
            enriched = enrich(raw_event)

            # 4. Persistir
            save_s3(S3_PROCESSED_BUCKET, enriched)
            save_dynamo(enriched)

            # 5. Publicar procesado
            sqs.send_message(
                QueueUrl=SQS_PROCESSED_URL,
                MessageBody=json.dumps(enriched, default=str),
            )

            processed += 1
            logger.info(f"OK {enriched['consent_id'][:8]} | risk={enriched['risk_score']} ({enriched['risk_level']})")

        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
            failed += 1

    return {'processed': processed, 'failed': failed}