import json
import os
import boto3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
    aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
    aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
    region_name='us-east-1',
)

RAW_BUCKET       = os.getenv('MINIO_BUCKET_RAW', 'healthcare-raw')
PROCESSED_BUCKET = os.getenv('MINIO_BUCKET_PROCESSED', 'healthcare-processed')


def list_raw_keys(prefix='') -> list:
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])
    return keys


def read_event(key: str) -> dict:
    response = s3.get_object(Bucket=RAW_BUCKET, Key=key)
    return json.loads(response['Body'].read())


def transform(event: dict) -> dict:
    """Aplica transformaciones y genera features para ML."""
    out = event.copy()

    # Feature: franja etaria
    age = event.get('patient_age', 0)
    if age < 18:   out['age_group'] = 'minor'
    elif age < 30: out['age_group'] = 'young_adult'
    elif age < 50: out['age_group'] = 'adult'
    elif age < 65: out['age_group'] = 'middle_aged'
    else:          out['age_group'] = 'senior'

    # Feature: categoría de BMI
    bmi = event.get('patient_bmi', 0)
    if bmi < 18.5:  out['bmi_category'] = 'underweight'
    elif bmi < 25:  out['bmi_category'] = 'normal'
    elif bmi < 30:  out['bmi_category'] = 'overweight'
    else:           out['bmi_category'] = 'obese'

    # Feature: comorbilidades count
    out['comorbidity_count'] = sum([
        event.get('diabetic', False),
        event.get('hypertensive', False),
        event.get('smoker', False),
        event.get('has_allergies', False),
    ])

    # Feature: riesgo anestesia
    anesthesia_risk = {'LOCAL': 1, 'SEDATION': 2, 'GENERAL': 3}
    out['anesthesia_risk_score'] = anesthesia_risk.get(
        event.get('anesthesia_type', 'LOCAL'), 1
    )

    # Feature: urgencia del consentimiento
    hours = event.get('consent_to_surgery_hours', 0)
    if hours < 4:    out['consent_urgency'] = 'critical'
    elif hours < 24: out['consent_urgency'] = 'urgent'
    elif hours < 72: out['consent_urgency'] = 'normal'
    else:            out['consent_urgency'] = 'scheduled'

    # Feature: score de completitud del formulario
    total_fields   = 28
    missing        = event.get('missing_fields_count', 0)
    out['form_completeness_pct'] = round((total_fields - missing) / total_fields * 100, 1)

    # Feature: label para ML (1 = alto riesgo, 0 = normal)
    risk = event.get('risk_score', 0)
    out['ml_label'] = 1 if risk >= 60 else 0

    out['glue_processed_at'] = datetime.utcnow().isoformat()
    return out


def save_processed(key: str, event: dict):
    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=key,
        Body=json.dumps(event),
        ContentType='application/json',
    )


def run():
    print(f"[Glue Job] Iniciando batch processing...")
    keys   = list_raw_keys()
    ok, ko = 0, 0

    for key in keys:
        try:
            raw       = read_event(key)
            processed = transform(raw)
            save_processed(key, processed)
            ok += 1
        except Exception as e:
            print(f"[ERROR] {key}: {e}")
            ko += 1

    print(f"[Glue Job] Completado — OK: {ok} | Errores: {ko}")


if __name__ == '__main__':
    run()