import json
import os
import logging
from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level='INFO')
log = logging.getLogger(__name__)

# Ventana deslizante de 60 eventos por clínica
WINDOW_SIZE = 60


class AnomalyDetector:

    def __init__(self):
        # Ventanas por clínica
        self.clinic_windows:  dict[str, deque] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))
        self.surgeon_windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

    def process(self, event: dict) -> list[dict]:
        alerts = []
        clinic_id  = event.get('clinic_id', 'UNKNOWN')
        surgeon_id = event.get('surgeon_id', 'UNKNOWN')
        risk_score = event.get('risk_score', 0)

        self.clinic_windows[clinic_id].append(event)
        self.surgeon_windows[surgeon_id].append(event)

        # ── Anomalía 1: tasa de riesgo alto por clínica ──────────
        clinic_w    = self.clinic_windows[clinic_id]
        high_risk_n = sum(1 for e in clinic_w if e.get('risk_score', 0) >= 60)
        high_risk_rate = high_risk_n / len(clinic_w)
        if high_risk_rate > 0.4 and len(clinic_w) >= 10:
            alerts.append({
                'alert_type':  'HIGH_RISK_RATE_CLINIC',
                'clinic_id':   clinic_id,
                'rate':        round(high_risk_rate, 2),
                'window_size': len(clinic_w),
                'severity':    'HIGH',
            })

        # ── Anomalía 2: consentimientos sin firmar consecutivos ───
        unsigned_streak = 0
        for e in reversed(clinic_w):
            if not e.get('consent_signed', True):
                unsigned_streak += 1
            else:
                break
        if unsigned_streak >= 3:
            alerts.append({
                'alert_type':     'UNSIGNED_CONSENT_STREAK',
                'clinic_id':      clinic_id,
                'streak':         unsigned_streak,
                'severity':       'CRITICAL',
            })

        # ── Anomalía 3: cirujano con demasiados casos críticos ────
        surgeon_w  = self.surgeon_windows[surgeon_id]
        critical_n = sum(1 for e in surgeon_w if e.get('risk_level') == 'CRITICAL')
        if critical_n >= 5 and len(surgeon_w) >= 10:
            alerts.append({
                'alert_type':  'SURGEON_CRITICAL_OVERLOAD',
                'surgeon_id':  surgeon_id,
                'critical_n':  critical_n,
                'severity':    'HIGH',
            })

        # ── Anomalía 4: ventana de consentimiento muy corta ───────
        if event.get('consent_to_surgery_hours', 999) < 2:
            alerts.append({
                'alert_type': 'CRITICAL_CONSENT_WINDOW',
                'consent_id': event.get('consent_id'),
                'hours':      event.get('consent_to_surgery_hours'),
                'severity':   'CRITICAL',
            })

        return alerts


def main():
    detector = AnomalyDetector()
    consumer = KafkaConsumer(
        os.getenv('KAFKA_TOPIC_PROCESSED', 'consent-events-processed'),
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id='anomaly-detector',
        auto_offset_reset='earliest',
    )

    log.info("Anomaly detector arriba — escuchando: consent-events-processed")

    for message in consumer:
        event  = message.value
        alerts = detector.process(event)

        if alerts:
            for alert in alerts:
                alert['detected_at'] = datetime.utcnow().isoformat()
                log.warning(f"ALERTA: {json.dumps(alert)}")
        else:
            log.info(
                f"OK {event.get('consent_id','?')[:8]}... "
                f"clinic={event.get('clinic_id')} "
                f"risk={event.get('risk_score', '?')}"
            )


if __name__ == '__main__':
    main()