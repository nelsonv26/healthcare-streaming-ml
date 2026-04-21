from kafka import KafkaConsumer
import json, os
from dotenv import load_dotenv
load_dotenv()

consumer = KafkaConsumer(
    'consent-events-raw',
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    consumer_timeout_ms=3000,
)

for i, msg in enumerate(consumer):
    if i == 0:
        print(json.dumps(msg.value, indent=2))
        break