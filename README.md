# Healthcare Streaming ML

Sistema de ingeniería de datos en tiempo real para procesar eventos de
consentimientos informados en clínicas de cirugía estética.

## Stack local (equivalencias AWS)

| Local              | AWS equivalente           |
|--------------------|---------------------------|
| Kafka              | Kinesis Data Streams      |
| Schema Registry    | Glue Schema Registry      |
| MinIO              | Amazon S3                 |
| DynamoDB Local     | Amazon DynamoDB           |
| Python scripts     | AWS Lambda                |
| Spark / Pandas     | AWS Glue ETL              |
| Jupyter            | Amazon SageMaker          |

## Estructura

```
healthcare-streaming-ml/
├── producer/           # Genera eventos con Faker → Kafka
├── schemas/            # Schemas Avro versionados
├── lambda/             # ETL: validación + enriquecimiento
├── kinesis_analytics/  # Detección de anomalías en streaming
├── glue/               # Batch ETL raw → processed
├── data/               # Datos de muestra y Kaggle
├── notebooks/          # Modelo ML en Jupyter
└── infra/docker/       # Stack completo con Docker Compose
```

## Arranque rápido

```bash
# 1. Levantar infraestructura
cd infra/docker
docker-compose up -d

# 2. Configurar variables
cp .env.example .env

# 3. Terminal 1 — Producer
cd producer && pip install -r requirements.txt
python generate_events.py

# 4. Terminal 2 — Processor (ETL)
cd lambda && pip install -r requirements.txt
python processor.py

# 5. Terminal 3 — Anomaly Detector
cd kinesis_analytics
python anomaly_detection.py

# 6. Batch ETL (cuando quieras procesar raw → processed)
cd glue
python raw_to_processed.py
```

## UIs locales

| Servicio        | URL                    | Credenciales         |
|-----------------|------------------------|----------------------|
| Kafka UI        | http://localhost:8080  | —                    |
| MinIO Console   | http://localhost:9001  | minioadmin/minioadmin|
| DynamoDB Admin  | http://localhost:8002  | —                    |

## Pipeline

```
Faker Producer
     ↓
Kafka (consent-events-raw)
     ↓
processor.py → MinIO raw + DynamoDB
     ↓
Kafka (consent-events-processed)
     ↓
anomaly_detection.py → alertas en tiempo real
     ↓
raw_to_processed.py → MinIO processed (features ML)
     ↓
ml_model.ipynb → modelo de riesgo
```