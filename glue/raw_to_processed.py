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


def calculate_risk(event: dict) -> tuple:
    risk, flags = 0, []
    if event.get('anesthesia_type') == 'GENERAL':
        risk += 25
        if event.get('diabetic'):    risk += 20; flags.append('GENERAL_ANESTHESIA_DIABETIC')
        if event.get('hypertensive'):risk += 10; flags.append('GENERAL_ANESTHESIA_HYPERTENSIVE')
    if event.get('is_minor'):                    risk += 15; flags.append('MINOR_PATIENT')
    if event.get('smoker'):                      risk += 10; flags.append('SMOKER')
    if event.get('patient_bmi', 0) > 35:         risk += 10; flags.append('HIGH_BMI')
    if event.get('consent_to_surgery_hours', 999) < 12: risk += 15; flags.append('SHORT_CONSENT_WINDOW')
    if not event.get('pre_op_labs_completed'):   risk += 20; flags.append('INCOMPLETE_PRE_OP_LABS')
    if event.get('pre_op_clearance') == 'REJECTED': risk += 30; flags.append('CLEARANCE_REJECTED')
    elif event.get('pre_op_clearance') == 'PENDING': risk += 10; flags.append('CLEARANCE_PENDING')
    if event.get('missing_fields_count', 0) > 0:
        risk += event['missing_fields_count'] * 5; flags.append('INCOMPLETE_FORM')
    return min(risk, 100), flags


def risk_level(score: int) -> str:
    if score >= 70: return 'CRITICAL'
    if score >= 50: return 'HIGH'
    if score >= 30: return 'MEDIUM'
    return 'LOW'


def transform(event: dict) -> dict:
    out = event.copy()

    risk_score, flags = calculate_risk(event)
    out['risk_score']    = risk_score
    out['risk_level']    = risk_level(risk_score)
    out['anomaly_flags'] = flags
    out['anomaly_count'] = len(flags)
    out['is_high_risk']  = risk_score >= 60

    age = event.get('patient_age', 0)
    if age < 18:   out['age_group'] = 'minor'
    elif age < 30: out['age_group'] = 'young_adult'
    elif age < 50: out['age_group'] = 'adult'
    elif age < 65: out['age_group'] = 'middle_aged'
    else:          out['age_group'] = 'senior'

    bmi = event.get('patient_bmi', 0)
    if bmi < 18.5:  out['bmi_category'] = 'underweight'
    elif bmi < 25:  out['bmi_category'] = 'normal'
    elif bmi < 30:  out['bmi_category'] = 'overweight'
    else:           out['bmi_category'] = 'obese'

    out['comorbidity_count'] = sum([
        bool(event.get('diabetic')),
        bool(event.get('hypertensive')),
        bool(event.get('smoker')),
        bool(event.get('has_allergies')),
    ])

    anesthesia_risk = {'LOCAL': 1, 'SEDATION': 2, 'GENERAL': 3}
    out['anesthesia_risk_score'] = anesthesia_risk.get(
        event.get('anesthesia_type', 'LOCAL'), 1)

    hours = event.get('consent_to_surgery_hours', 0)
    if hours < 4:    out['consent_urgency'] = 'critical'
    elif hours < 24: out['consent_urgency'] = 'urgent'
    elif hours < 72: out['consent_urgency'] = 'normal'
    else:            out['consent_urgency'] = 'scheduled'

    total_fields = 28
    missing = event.get('missing_fields_count', 0)
    out['form_completeness_pct'] = round((total_fields - missing) / total_fields * 100, 1)
    out['ml_label'] = 1 if risk_score >= 60 else 0
    out['glue_processed_at'] = datetime.utcnow().isoformat()
    return out


def list_raw_keys() -> list:
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=RAW_BUCKET):
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])
    return keys


def run():
    print("[Glue Job] Limpiando processed...")
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=PROCESSED_BUCKET):
        for obj in page.get('Contents', []):
            s3.delete_object(Bucket=PROCESSED_BUCKET, Key=obj['Key'])

    print("[Glue Job] Procesando raw...")
    keys = list_raw_keys()
    ok, ko = 0, 0
    for key in keys:
        try:
            raw = json.loads(s3.get_object(Bucket=RAW_BUCKET, Key=key)['Body'].read())
            processed = transform(raw)
            s3.put_object(
                Bucket=PROCESSED_BUCKET,
                Key=key,
                Body=json.dumps(processed),
                ContentType='application/json',
            )
            ok += 1
        except Exception as e:
            print(f"[ERROR] {key}: {e}")
            ko += 1

    print(f"[Glue Job] OK: {ok} | Errores: {ko}")


if __name__ == '__main__':
    run()