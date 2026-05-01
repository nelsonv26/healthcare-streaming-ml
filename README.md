# Healthcare Streaming ML

Sistema de ingeniería de datos en tiempo real para procesar consentimientos informados
en clínicas de cirugía estética. Incluye validación, enriquecimiento, predicción de
riesgo con ML y persistencia en S3 + DynamoDB.

## Arquitectura

### Pipeline local (desarrollo)

```
Faker Producer (generate_events.py)
        ↓  Kafka: consent-events-raw
processor.py — validación + enriquecimiento
        ↓  MinIO raw   ↓  DynamoDB   ↓  Kafka: consent-events-processed
anomaly_detection.py — alertas en tiempo real
        ↓
raw_to_proccesed.py — Batch ETL → MinIO processed (features ML)
        ↓
ml_model.ipynb — entrenamiento Random Forest
```

### Pipeline AWS (producción)

```
Producer local → SQS (healthcare-streaming-raw)
                        ↓
            Lambda processor (healthcare-streaming-processor)
                  ↓             ↓              ↓
              S3 raw      DynamoDB      SQS processed
                                              ↓
            Lambda inference (healthcare-streaming-inference)
              ↓ predict_risk() — rules_rf_features_v2
          S3 processed/inference/...   DynamoDB (risk_score, features)
```

## Stack tecnológico

| Local | AWS equivalente | Propósito |
|---|---|---|
| Kafka | SQS | Streaming de eventos |
| Schema Registry | Glue Schema Registry | Validación Avro |
| MinIO | Amazon S3 | Almacenamiento raw + processed |
| DynamoDB Local | Amazon DynamoDB | Estado de consentimientos |
| Python scripts | AWS Lambda | ETL + inferencia ML |
| Spark / Pandas | AWS Glue | Batch ETL |
| Jupyter | Amazon SageMaker | Entrenamiento del modelo |

## Estructura del proyecto

```
healthcare-streaming-ml/
├── producer/
│   ├── generate_events.py      # Genera eventos sintéticos con Faker → Kafka
│   └── requirements.txt
├── schemas/
│   ├── consent_v1.avsc         # Schema Avro v1
│   └── consent_v2.avsc         # Schema Avro v2 (nationality, consent_channel, surgery_complexity)
├── lambda/
│   ├── processor.py            # ETL: validación + enriquecimiento → MinIO + DynamoDB
│   ├── lambda_inference.py     # Inferencia ML (reglas Python puras, sin sklearn)
│   └── requirements.txt
├── kinesis_analytics/
│   └── anomaly_detection.py    # Detección de anomalías en streaming
├── glue/
│   └── raw_to_proccesed.py     # Batch ETL raw → processed (features ML)
├── notebooks/
│   └── ml_model.ipynb          # Entrenamiento Random Forest
├── infra/
│   ├── docker/
│   │   └── docker-compose.yml  # Stack completo: Kafka, MinIO, DynamoDB Local
│   └── terraform/              # Infraestructura AWS (IaC)
│       ├── main.tf
│       ├── variables.tf
│       ├── lambda.tf
│       ├── iam.tf
│       ├── sqs.tf
│       └── outputs.tf
└── .env.example                # Variables de entorno de referencia
```

---

## Pipeline local — Arranque rápido

### Prerrequisitos

- Docker y Docker Compose
- Python 3.11+
- Las variables de entorno configuradas (ver paso 1)

### Paso 1 — Variables de entorno

```bash
cp .env.example .env
# Edita .env si necesitas cambiar puertos o credenciales
```

### Paso 2 — Infraestructura local

```bash
cd infra/docker
docker-compose up -d
```

Espera ~15 segundos a que Kafka esté listo:

```bash
docker-compose ps        # todos los servicios deben estar "Up"
```

### Paso 3 — Dependencias Python

```bash
# Instalar una sola vez por entorno
pip install -r producer/requirements.txt
pip install -r lambda/requirements.txt
```

### Paso 4 — Iniciar los servicios (una terminal por proceso)

**Terminal 1 — Producer** (genera ~5 eventos/seg):
```bash
cd producer
python generate_events.py
```

**Terminal 2 — Processor ETL**:
```bash
cd lambda
python processor.py
```

**Terminal 3 — Anomaly Detector** (alertas en tiempo real):
```bash
cd kinesis_analytics
python anomaly_detection.py
```

**Terminal 4 — Batch ETL** (opcional, ejecutar cuando se desee):
```bash
cd glue
python raw_to_proccesed.py
```

### UIs locales

| Servicio | URL | Credenciales |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| DynamoDB Admin | http://localhost:8002 | — |

### Detener el stack

```bash
cd infra/docker
docker-compose down          # detiene contenedores
docker-compose down -v       # detiene + elimina volúmenes
```

---

## Pipeline AWS — Despliegue

### Prerrequisitos

- AWS CLI configurado (`aws configure`)
- Terraform >= 1.5
- Cuenta AWS con permisos sobre Lambda, S3, SQS, DynamoDB, IAM

### Paso 1 — Desplegar infraestructura

```bash
cd infra/terraform
terraform init
terraform plan
terraform apply
```

Recursos que crea Terraform:
- **Lambda processor** (`healthcare-streaming-processor`): SQS → ETL → S3 + DynamoDB
- **Lambda inference** (`healthcare-streaming-inference`): inferencia ML sin dependencias externas
- **SQS queues**: raw, processed, dlq
- **S3 buckets**: raw y processed
- **DynamoDB**: tabla `consent-state-aws`
- **IAM role**: permisos mínimos para ambas Lambdas

### Paso 2 — Enviar eventos al pipeline

```bash
# Obtener la URL de la cola SQS raw desde los outputs de Terraform
SQS_URL=$(terraform -chdir=infra/terraform output -raw sqs_raw_url)

# Enviar un evento de prueba
aws sqs send-message \
  --queue-url "$SQS_URL" \
  --message-body '{
    "consent_id": "demo-001",
    "patient_id": "P-123",
    "patient_age": 45,
    "patient_bmi": 38,
    "anesthesia_type": "GENERAL",
    "diabetic": true,
    "hypertensive": false,
    "smoker": false,
    "is_minor": false,
    "consent_to_surgery_hours": 6,
    "pre_op_labs_completed": false,
    "pre_op_clearance": "PENDING",
    "missing_fields_count": 1,
    "procedure_type": "LIPOSUCTION",
    "consent_signed": true,
    "risk_acknowledgement": 4,
    "clinic_id": "CL-01",
    "surgeon_id": "SR-01",
    "scheduled_date": "2026-06-01",
    "legal_guardian_required": false
  }'
```

---

## Validación en producción

### Verificar que la Lambda de inferencia está desplegada

```bash
aws lambda get-function \
  --function-name healthcare-streaming-inference \
  --query 'Configuration.[FunctionName,Runtime,Handler,CodeSize]'
```

Salida esperada:
```json
["healthcare-streaming-inference", "python3.11", "lambda_inference.lambda_handler", <bytes>]
```

### Invocar la Lambda directamente

```python
import boto3, json

client = boto3.client('lambda', region_name='us-east-1')

payload = {
    'consent_id': 'test-001',
    'anesthesia_type': 'GENERAL',
    'diabetic': True,
    'patient_bmi': 38,
    'consent_to_surgery_hours': 6,
    'pre_op_labs_completed': False,
    'pre_op_clearance': 'PENDING',
    'missing_fields_count': 1
}

response = client.invoke(
    FunctionName='healthcare-streaming-inference',
    Payload=json.dumps(payload)
)

result = json.loads(response['Payload'].read())
print(json.dumps(result, indent=2))
```

Resultado esperado:
```json
{
  "statusCode": 200,
  "body": {
    "consent_id": "test-001",
    "risk_score": 100,
    "risk_level": "CRITICAL",
    "is_high_risk": true,
    "prediction_method": "rules_rf_features_v2",
    "features": {
      "anesthesia_risk_score": 3,
      "comorbidity_count": 2,
      "consent_to_surgery_hours": 6,
      "patient_bmi": 38,
      "patient_age": 0
    }
  }
}
```

### Verificar resultado en S3

```bash
BUCKET=$(terraform -chdir=infra/terraform output -raw s3_processed_bucket)
aws s3 ls "s3://$BUCKET/inference/" --recursive | tail -5
```

### Verificar resultado en DynamoDB

```bash
TABLE=$(terraform -chdir=infra/terraform output -raw dynamodb_table)
aws dynamodb get-item \
  --table-name "$TABLE" \
  --key '{"consent_id": {"S": "test-001"}}' \
  --query 'Item.[risk_score,risk_level,is_high_risk,prediction_method]'
```

### Revisar logs en CloudWatch

```bash
aws logs tail /aws/lambda/healthcare-streaming-inference --follow
```

---

## Modelo de predicción de riesgo

La Lambda de inferencia implementa el modelo Random Forest entrenado en
`notebooks/ml_model.ipynb` como **reglas Python puras** (`rules_rf_features_v2`),
sin dependencias externas (sin sklearn, numpy ni scipy).

### Top features por importancia RF

| Ranking | Feature | Importancia | Puntaje máximo |
|---|---|---|---|
| 1 | `risk_score` (score base) | ~35% | 100 pts (capped) |
| 2 | `anesthesia_risk_score` | ~20% | GENERAL=+25, REGIONAL=+12, SEDATION=+6 |
| 3 | `comorbidity_count` | ~18% | diabetic+hypertensive+smoker+is_minor+bmi>35 |
| 4 | `consent_to_surgery_hours` | ~12% | <12h → +15 pts |
| 5 | `patient_bmi` | ~10% | >35 → +10 pts |
| 6 | `patient_age` | ~5% | >70 → +8 pts, >60 → +4 pts |

### Niveles de riesgo

| Nivel | Score | Acción recomendada |
|---|---|---|
| CRITICAL | ≥ 70 | Revisión obligatoria antes de cirugía |
| HIGH | 50–69 | Evaluación adicional requerida |
| MEDIUM | 30–49 | Monitoreo estándar |
| LOW | < 30 | Proceder normalmente |

---

## Variables de entorno

### Local (`.env`)

| Variable | Descripción | Default |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Broker Kafka local | `localhost:9092` |
| `KAFKA_TOPIC_RAW` | Topic de entrada | `consent-events-raw` |
| `MINIO_ENDPOINT` | Endpoint MinIO | `http://localhost:9000` |
| `MINIO_BUCKET_RAW` | Bucket raw | `healthcare-raw` |
| `DYNAMODB_ENDPOINT` | DynamoDB local | `http://localhost:8000` |
| `EVENTS_PER_SECOND` | Tasa del producer | `5` |

### AWS Lambda (`infra/terraform/variables.tf`)

| Variable Terraform | Descripción | Default |
|---|---|---|
| `aws_region` | Región AWS | `us-east-1` |
| `project_name` | Prefijo de recursos | `healthcare-streaming` |
| `environment` | Entorno | `dev` |

Variables de entorno que Terraform inyecta en las Lambdas:

| Variable | Descripción |
|---|---|
| `S3_PROCESSED_BUCKET` | Bucket S3 para resultados de inferencia |
| `DYNAMODB_TABLE_AWS` | Tabla DynamoDB en AWS |
| `MODEL_KEY` | Clave S3 del modelo pkl (reservado para futura reintegración) |
