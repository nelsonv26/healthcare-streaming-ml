import json
import time
import uuid
import random
import logging
import boto3
from datetime import datetime, timedelta
from faker import Faker
from dotenv import load_dotenv
import os

load_dotenv()
fake = Faker('es_CO')
logging.basicConfig(level='INFO')
log = logging.getLogger(__name__)

sqs = boto3.client('sqs', region_name='us-east-1')
QUEUE_URL = os.getenv('SQS_RAW_URL')

PROCEDURES  = ['RHINOPLASTY','LIPOSUCTION','BREAST_AUGMENTATION',
                'ABDOMINOPLASTY','BLEPHAROPLASTY','FACELIFT','OTHER']
ANESTHESIA  = ['LOCAL','GENERAL','SEDATION']
BLOOD_TYPES = ['A+','A-','B+','B-','AB+','AB-','O+','O-']
CLEARANCE   = ['APPROVED','PENDING','REJECTED']
ALLERGIES   = ['penicilina','ibuprofeno','látex','aspirina','morfina','yodo']

def make_event() -> dict:
    age      = random.randint(16, 72)
    is_minor = age < 18
    has_allerg = random.random() < 0.25
    sched_delta = random.randint(2, 720)

    return {
        "consent_id":               str(uuid.uuid4()),
        "schema_version":           "1.0",
        "event_timestamp":          int(datetime.utcnow().timestamp() * 1000),
        "patient_id":               str(uuid.uuid4()),
        "patient_age":              age,
        "patient_gender":           random.choice(['M','F','OTHER']),
        "patient_bmi":              round(random.uniform(17.5, 42.0), 1),
        "blood_type":               random.choice(BLOOD_TYPES),
        "smoker":                   random.random() < 0.18,
        "diabetic":                 random.random() < 0.12,
        "hypertensive":             random.random() < 0.20,
        "has_allergies":            has_allerg,
        "allergy_list":             random.sample(ALLERGIES, random.randint(1,3)) if has_allerg else [],
        "previous_surgeries":       random.randint(0, 5),
        "clinic_id":                f"CLINIC-{random.randint(1,10):03d}",
        "surgeon_id":               f"SURG-{random.randint(1,30):03d}",
        "procedure_type":           random.choice(PROCEDURES),
        "anesthesia_type":          random.choice(ANESTHESIA),
        "scheduled_date":           (datetime.utcnow() + timedelta(hours=sched_delta)).strftime('%Y-%m-%d'),
        "estimated_duration_min":   random.randint(30, 360),
        "consent_signed":           random.random() < 0.92,
        "witness_present":          random.random() < 0.75,
        "is_minor":                 is_minor,
        "legal_guardian_required":  is_minor,
        "risk_acknowledgement":     random.randint(1, 5),
        "pre_op_labs_completed":    random.random() < 0.85,
        "pre_op_clearance":         random.choice(CLEARANCE),
        "consent_to_surgery_hours": round(sched_delta, 2),
        "missing_fields_count":     random.choices([0,1,2,3], weights=[70,15,10,5])[0],
    }

def main():
    rate = float(os.getenv('EVENTS_PER_SECOND', 2))
    log.info(f"Producer AWS arriba → SQS @ {rate} eventos/seg")

    try:
        while True:
            event = make_event()
            sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(event),
                MessageGroupId='consent-events',
            ) if 'fifo' in QUEUE_URL.lower() else sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(event),
            )
            log.info(f"Enviado: {event['consent_id']} | {event['procedure_type']}")
            time.sleep(1 / rate)
    except KeyboardInterrupt:
        log.info("Producer detenido.")

if __name__ == '__main__':
    main()