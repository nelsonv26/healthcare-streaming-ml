import pandas as pd
import json
import os
import uuid
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv('../.env')

KAGGLE_FILE = 'kaggle/Anesthesia_Dataset.csv'
OUTPUT_FILE = 'sample_data/kaggle_enriched_events.json'

PROCEDURE_MAP = {
    'Cosmetic':       'LIPOSUCTION',
    'Cardiovascular': 'OTHER',
    'Orthopedic':     'OTHER',
    'Neurological':   'OTHER',
}

ANESTHESIA_MAP = {
    'General': 'GENERAL',
    'Local':   'LOCAL',
}

def parse_duration(duration_str) -> int:
    try:
        return int(str(duration_str).replace('min','').strip())
    except:
        return 60

def build_event(row: pd.Series) -> dict:
    age      = int(row['Age'])
    is_minor = age < 18

    return {
        "consent_id":               str(uuid.uuid4()),
        "schema_version":           "2.0",
        "event_timestamp":          int(datetime.utcnow().timestamp() * 1000),
        "patient_id":               f"KAGGLE-{row['PatientID']}",
        "patient_age":              age,
        "patient_gender":           'M' if str(row['Gender']).lower() == 'male' else 'F',
        "patient_bmi":              float(row['BMI']),
        "patient_nationality":      "US",
        "blood_type":               random.choice(['A+','A-','B+','O+','O-','AB+']),
        "smoker":                   random.random() < 0.18,
        "diabetic":                 random.random() < 0.12,
        "hypertensive":             random.random() < 0.20,
        "has_allergies":            random.random() < 0.25,
        "allergy_list":             [],
        "previous_surgeries":       random.randint(0, 3),
        "clinic_id":                f"CLINIC-{random.randint(1,10):03d}",
        "surgeon_id":               f"SURG-{random.randint(1,30):03d}",
        "procedure_type":           PROCEDURE_MAP.get(str(row['SurgeryType']), 'OTHER'),
        "anesthesia_type":          ANESTHESIA_MAP.get(str(row['AnesthesiaType']), 'LOCAL'),
        "consent_channel":          random.choice(['DIGITAL','PAPER','VIDEOCALL']),
        "scheduled_date":           (datetime.utcnow() + timedelta(days=random.randint(1,30))).strftime('%Y-%m-%d'),
        "estimated_duration_min":   parse_duration(row['SurgeryDuration']),
        "consent_signed":           True,
        "witness_present":          random.random() < 0.75,
        "is_minor":                 is_minor,
        "legal_guardian_required":  is_minor,
        "risk_acknowledgement":     min(int(row['PainLevel']) // 2 + 1, 5),
        "pre_op_labs_completed":    True,
        "pre_op_clearance":         'APPROVED' if row['Outcome'] == 0 else 'PENDING',
        "consent_to_surgery_hours": random.randint(12, 200),
        "missing_fields_count":     0,
        "surgery_complexity":       3 if str(row['AnesthesiaType']) == 'General' else 1,

        # Campos originales de Kaggle — para ML
        "kaggle_pain_level":        int(row['PainLevel']),
        "kaggle_complications":     str(row['Complications']),
        "kaggle_outcome":           int(row['Outcome']),
        "kaggle_surgery_type":      str(row['SurgeryType']),
        "ml_label":                 int(row['Outcome']),  # outcome real de Kaggle
        "source":                   "kaggle",
    }

def main():
    print(f"Leyendo {KAGGLE_FILE}...")
    df = pd.read_csv(KAGGLE_FILE)
    print(f"Filas: {len(df)} | Columnas: {list(df.columns)}")

    events = []
    for _, row in df.iterrows():
        try:
            events.append(build_event(row))
        except Exception as e:
            print(f"Error en fila {row.get('PatientID','?')}: {e}")

    os.makedirs('sample_data', exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(events, f, indent=2, default=str)

    # Stats
    outcomes = [e['ml_label'] for e in events]
    print(f"\n✓ Eventos generados: {len(events)}")
    print(f"✓ Sin complicaciones (0): {outcomes.count(0)}")
    print(f"✓ Con complicaciones (1): {outcomes.count(1)}")
    print(f"✓ Guardado en: {OUTPUT_FILE}")

if __name__ == '__main__':
    main()