import json
import time
import uuid
import random
import logging
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()
fake = Faker('es_CO')
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'))
log = logging.getLogger(__name__)

PROCEDURES     = ['RHINOPLASTY','LIPOSUCTION','BREAST_AUGMENTATION',
                  'ABDOMINOPLASTY','BLEPHAROPLASTY','FACELIFT','OTHER']
ANESTHESIA     = ['LOCAL','GENERAL','SEDATION']
BLOOD_TYPES    = ['A+','A-','B+','B-','AB+','AB-','O+','O-']
CLEARANCE      = ['APPROVED','PENDING','REJECTED']
ALLERGIES_POOL = ['penicilina','ibuprofeno','látex','aspirina','morfina','yodo']
NATIONALITIES    = ['CO','MX','VE','EC','PE','AR','US','ES']
CONSENT_CHANNELS = ['DIGITAL','PAPER','VIDEOCALL','HYBRID']

COMPLEXITY_MAP = {
    'RHINOPLASTY': 2, 'LIPOSUCTION': 3, 'BREAST_AUGMENTATION': 3,
    'ABDOMINOPLASTY': 4, 'BLEPHAROPLASTY': 2, 'FACELIFT': 4, 'OTHER': 1
}

def make_event() -> dict:
    age         = random.randint(16, 72)
    is_minor    = age < 18
    bmi         = round(random.uniform(17.5, 42.0), 1)
    diabetic    = random.random() < 0.12
    hypertensive= random.random() < 0.20
    anesthesia  = random.choice(ANESTHESIA)
    has_allerg  = random.random() < 0.25
    signed      = random.random() < 0.92
    labs_done   = random.random() < 0.85
    sched_delta = random.randint(2, 720)          # horas hasta la cirugía
    missing     = random.choices([0, 1, 2, 3], weights=[70, 15, 10, 5])[0]

    return {
        "consent_id":              str(uuid.uuid4()),
        "schema_version":          "1.0",
        "event_timestamp":         int(datetime.utcnow().timestamp() * 1000),
        "patient_id":              str(uuid.uuid4()),
        "patient_age":             age,
        "patient_gender":          random.choice(['M', 'F', 'OTHER']),
        "patient_bmi":             bmi,
        "blood_type":              random.choice(BLOOD_TYPES),
        "smoker":                  random.random() < 0.18,
        "diabetic":                diabetic,
        "hypertensive":            hypertensive,
        "has_allergies":           has_allerg,
        "allergy_list":            random.sample(ALLERGIES_POOL, random.randint(1,3)) if has_allerg else [],
        "previous_surgeries":      random.randint(0, 5),
        "clinic_id":               f"CLINIC-{random.randint(1,10):03d}",
        "surgeon_id":              f"SURG-{random.randint(1,30):03d}",
        "procedure_type":          (procedure := random.choice(PROCEDURES)),
        "anesthesia_type":         anesthesia,
        "scheduled_date":          (datetime.utcnow() + timedelta(hours=sched_delta)).strftime('%Y-%m-%d'),
        "estimated_duration_min":  random.randint(30, 360),
        "consent_signed":          signed,
        "witness_present":         random.random() < 0.75,
        "is_minor":                is_minor,
        "legal_guardian_required": is_minor,
        "risk_acknowledgement":    random.randint(1, 5),
        "pre_op_labs_completed":   labs_done,
        "pre_op_clearance":        random.choice(CLEARANCE),
        "consent_to_surgery_hours":round(sched_delta, 2),
        "missing_fields_count":    missing,
        "patient_nationality": random.choice(NATIONALITIES),
"consent_channel":     random.choice(CONSENT_CHANNELS),
"surgery_complexity":  COMPLEXITY_MAP.get(procedure, 1),
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
    )
    topic = os.getenv('KAFKA_TOPIC_RAW', 'consent-events-raw')
    rate  = float(os.getenv('EVENTS_PER_SECOND', 2))

    log.info(f"Producer arriba → topic: {topic} @ {rate} eventos/seg")

    try:
        while True:
            event = make_event()
            producer.send(topic, key=event['consent_id'], value=event)
            log.info(f"Enviado: {event['consent_id']} | {event['procedure_type']} | riesgo potencial: {_risk_flag(event)}")
            time.sleep(1 / rate)
    except KeyboardInterrupt:
        log.info("Producer detenido.")
    finally:
        producer.flush()
        producer.close()

def _risk_flag(e: dict) -> str:
    """Mini heurística de riesgo para logging."""
    flags = []
    if e['is_minor'] and not e['legal_guardian_required']: flags.append('MINOR_NO_GUARDIAN')
    if e['anesthesia_type'] == 'GENERAL' and e['diabetic']:  flags.append('GENERAL+DIABETIC')
    if not e['consent_signed']:                               flags.append('UNSIGNED')
    if e['consent_to_surgery_hours'] < 4:                     flags.append('SHORT_WINDOW')
    return ','.join(flags) if flags else 'OK'

if __name__ == '__main__':
    main()