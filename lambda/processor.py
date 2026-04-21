import json
import time
import logging
import hashlib
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from boto3 import client as boto3_client
from botocore.exceptions import ClientError
import boto3
from dotenv import load_dotenv
import os

load_dotenv()
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'))
log = logging.getLogger(__name__)


# ─── Clientes ────────────────────────────────────────────────────────────────

def get_s3():
    return boto3_client(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        region_name='us-east-1',
    )

def get_dynamo():
    return boto3.resource(
        'dynamodb',
        endpoint_url=os.getenv('DYNAMODB_ENDPOINT', 'http://localhost:8000'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'local'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'local'),
        region_name=os.getenv('DYNAMODB_REGION', 'us-east-1'),
    )


# ─── Setup inicial: buckets + tabla DynamoDB ──────────────────────────────────

def ensure_infrastructure():
    s3 = get_s3()
    for bucket in [
        os.getenv('MINIO_BUCKET_RAW', 'healthcare-raw'),
        os.getenv('MINIO_BUCKET_PROCESSED', 'healthcare-processed'),
        'healthcare-dlq',
    ]:
        try:
            s3.create_bucket(Bucket=bucket)
            log.info(f"Bucket creado: {bucket}")
        except Exception:
            pass  # ya existe

    dynamo = get_dynamo()
    table_name = os.getenv('DYNAMODB_TABLE', 'consent-state')
    try:
        dynamo.create_table(
            TableName=table_name,
            KeySchema=[{'AttributeName': 'consent_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'consent_id', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST',
        )
        log.info(f"Tabla DynamoDB creada: {table_name}")
    except ClientError as e:
        if 'ResourceInUseException' not in str(e):
            raise


# ─── Validación ───────────────────────────────────────────────────────────────

REQUIRED_FIELDS = [
    'consent_id', 'patient_id', 'patient_age', 'procedure_type',
    'consent_signed', 'anesthesia_type', 'clinic_id', 'surgeon_id',
    'scheduled_date', 'pre_op_clearance',
]

def validate(event: dict) -> tuple[bool, list[str]]:
    errors = []

    # Campos obligatorios
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            errors.append(f"missing_field:{field}")

    if errors:
        return False, errors

    # Reglas de negocio
    if not (0 < event['patient_age'] < 120):
        errors.append("invalid_age")

    if event['is_minor'] and not event['legal_guardian_required']:
        errors.append("minor_without_guardian_flag")

    if not event['consent_signed']:
        errors.append("unsigned_consent")

    if event['risk_acknowledgement'] not in range(1, 6):
        errors.append("invalid_risk_score")

    if event['consent_to_surgery_hours'] < 1:
        errors.append("consent_window_too_short")

    return len(errors) == 0, errors


# ─── Enriquecimiento ──────────────────────────────────────────────────────────

def enrich(event: dict) -> dict:
    enriched = event.copy()
    enriched['processed_at'] = datetime.now(timezone.utc).isoformat()

    # Risk score (0-100)
    risk = 0
    flags = []

    if event['anesthesia_type'] == 'GENERAL':
        risk += 25
        if event['diabetic']:
            risk += 20
            flags.append('GENERAL_ANESTHESIA_DIABETIC')
        if event['hypertensive']:
            risk += 10
            flags.append('GENERAL_ANESTHESIA_HYPERTENSIVE')

    if event['is_minor']:
        risk += 15
        flags.append('MINOR_PATIENT')

    if event['smoker']:
        risk += 10
        flags.append('SMOKER')

    if event['patient_bmi'] > 35:
        risk += 10
        flags.append('HIGH_BMI')

    if event['consent_to_surgery_hours'] < 12:
        risk += 15
        flags.append('SHORT_CONSENT_WINDOW')

    if not event['pre_op_labs_completed']:
        risk += 20
        flags.append('INCOMPLETE_PRE_OP_LABS')

    if event['pre_op_clearance'] == 'REJECTED':
        risk += 30
        flags.append('CLEARANCE_REJECTED')
    elif event['pre_op_clearance'] == 'PENDING':
        risk += 10
        flags.append('CLEARANCE_PENDING')

    if event['missing_fields_count'] > 0:
        risk += event['missing_fields_count'] * 5
        flags.append('INCOMPLETE_FORM')

    enriched['risk_score']      = min(risk, 100)
    enriched['risk_level']      = _risk_level(risk)
    enriched['anomaly_flags']   = flags
    enriched['anomaly_count']   = len(flags)
    enriched['is_high_risk']    = risk >= 60
    enriched['processing_hash'] = hashlib.md5(
        json.dumps(event, sort_keys=True).encode()
    ).hexdigest()

    return enriched

def _risk_level(score: int) -> str:
    if score >= 70: return 'CRITICAL'
    if score >= 50: return 'HIGH'
    if score >= 30: return 'MEDIUM'
    return 'LOW'


# ─── Persistencia ─────────────────────────────────────────────────────────────

def save_to_s3_raw(s3, event: dict):
    bucket = os.getenv('MINIO_BUCKET_RAW', 'healthcare-raw')
    ts     = datetime.utcnow()
    key    = f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/{event['consent_id']}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(event), ContentType='application/json')

def save_to_s3_processed(s3, event: dict):
    bucket = os.getenv('MINIO_BUCKET_PROCESSED', 'healthcare-processed')
    ts     = datetime.utcnow()
    key    = f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/{event['consent_id']}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(event), ContentType='application/json')

def save_to_dynamo(dynamo, event: dict):
    table = dynamo.Table(os.getenv('DYNAMODB_TABLE', 'consent-state'))
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
        'ttl':            int(time.time()) + 86400 * 30,  # 30 días
    })

def save_to_dlq(s3, event: dict, errors: list):
    bucket = 'healthcare-dlq'
    ts     = datetime.utcnow()
    key    = f"{ts.date()}/{event.get('consent_id', 'unknown')}.json"
    payload = {'event': event, 'errors': errors, 'timestamp': ts.isoformat()}
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload), ContentType='application/json')
    log.warning(f"DLQ: {event.get('consent_id')} → {errors}")


# ─── Loop principal ───────────────────────────────────────────────────────────

def main():
    ensure_infrastructure()

    s3     = get_s3()
    dynamo = get_dynamo()

    consumer = KafkaConsumer(
        os.getenv('KAFKA_TOPIC_RAW', 'consent-events-raw'),
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id=os.getenv('KAFKA_CONSUMER_GROUP', 'consent-processor'),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
    )

    producer = KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
    )

    topic_out = os.getenv('KAFKA_TOPIC_PROCESSED', 'consent-events-processed')
    log.info(f"Processor arriba — escuchando: consent-events-raw")

    for message in consumer:
        try:
            raw_event = message.value

            # 1. Guardar raw (siempre, antes de cualquier validación)
            save_to_s3_raw(s3, raw_event)

            # 2. Validar
            is_valid, errors = validate(raw_event)
            if not is_valid:
                save_to_dlq(s3, raw_event, errors)
                continue

            # 3. Enriquecer
            enriched = enrich(raw_event)

            # 4. Persistir procesado
            save_to_s3_processed(s3, enriched)
            save_to_dynamo(dynamo, enriched)

            # 5. Publicar evento procesado
            producer.send(topic_out, key=enriched['consent_id'], value=enriched)

            log.info(
                f"OK {enriched['consent_id']} | "
                f"{enriched['procedure_type']} | "
                f"risk={enriched['risk_score']} ({enriched['risk_level']}) | "
                f"flags={enriched['anomaly_flags']}"
            )

        except Exception as e:
            log.error(f"Error procesando mensaje: {e}", exc_info=True)

if __name__ == '__main__':
    main()