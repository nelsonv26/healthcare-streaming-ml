# Lessons Learned — healthcare-streaming-ml

> Last updated: 2026-05-04

---

## 📌 Context

A real-time data engineering pipeline for informed consent processing in aesthetic surgery clinics. Built to demonstrate a full data stack: streaming ingestion, ETL enrichment, anomaly detection, ML training, and serverless inference — all mirrored between a local Docker environment and AWS. Worked on this over several months, evolving from a simple Kafka → S3 demo into a dual-track (local/cloud) production-ready system with a trained Random Forest deployed as Lambda.

---

## ✅ What Worked Well

- **Dual-environment design from day one.** Having a local Docker stack (Kafka ≈ SQS, MinIO ≈ S3, DynamoDB Local) that mirrors AWS made iteration extremely fast. You could build and test the entire pipeline without touching cloud services or incurring costs. When it came time to deploy to AWS, the architectural mapping was trivial.

- **Pure Python inference in Lambda.** Reimplementing the trained Random Forest as weighted rule logic (`rules_rf_features_v2`) — rather than loading a `.pkl` via a Lambda Layer — eliminated all dependency management pain. The model inference works without sklearn, numpy, or scipy. It's also easier to audit: the scoring logic is just readable Python conditionals.

- **Kaggle dataset integration.** Using a real anesthesia outcomes dataset to supplement Faker events improved the ML model significantly. The `integrate_kaggle.py` script maps the Kaggle schema into the consent event format cleanly, and the combined dataset (746 Faker + 300 Kaggle = 1046 events) gave enough signal to train a model with ROC-AUC 0.988.

- **Avro schema with backward compatibility.** Adding `patient_nationality`, `consent_channel`, and `surgery_complexity` in v2 with `default` values kept everything backward compatible. Consumers expecting v1 can read v2 events without errors. Versioning was handled correctly from the start.

- **DynamoDB TTL + PAY_PER_REQUEST.** Using `PAY_PER_REQUEST` billing and a 30-day TTL on consent records is exactly right for this use case. No capacity planning needed, no orphaned records accumulating indefinitely.

- **SQS Dead Letter Queue.** Routing validation failures to a DLQ instead of dropping them was a good call. Invalid events are preserved for later inspection and potential reprocessing. The DLQ also has its own S3 bucket for persistence.

---

## ❌ What Didn't Work / Mistakes Made

- **Glue job disabled mid-project.** `glue.tf.disabled` tells the whole story — the Glue job was built and Terraform-managed, then disabled because Lambda inference turned out to cover the same batch use case more cheaply. The Glue approach (Crawler + Catalog + Job) added significant complexity for what is ultimately a small dataset. Should have evaluated Lambda vs Glue earlier rather than building both.

- **sklearn Lambda Layer was planned but never shipped.** There are commented-out blocks in `lambda.tf` for a Lambda Layer that would package sklearn for the inference function. This was abandoned in favor of the pure-rules approach. The leftover commented code adds noise. Should have deleted it once the decision was made final.

- **`processor.py` and `lambda_function.py` duplicate risk scoring logic.** The same `calculate_risk()` / `enrich()` function appears in both the local Kafka processor and the Lambda handler — written independently with slight differences. This creates a maintenance problem: a rule change needs to be applied in multiple places. Should have extracted this into a shared module from the beginning.

- **No schema validation at the processor level.** Despite having Avro schemas registered in Schema Registry, the processor actually parses JSON. The Schema Registry integration is present (visible in docker-compose.yml) but not enforced in the consumer logic. The schemas exist as documentation/contract artifacts only.

- **`processor_aws.py` duplicates `lambda_function.py`.** There are two AWS-oriented processor files doing essentially the same thing — one as a long-polling script, one as a Lambda handler. This was probably created to support both deployment modes, but the `processor_aws.py` polling loop is now redundant since Lambda handles SQS events directly. Creates confusion about which to actually run.

---

## 🔁 What I'd Do Differently

- **Extract the risk scoring logic into a shared library.** A single `risk_engine.py` module imported by both the local processor and the Lambda handler would eliminate the dual-maintenance problem. Even a simple `shared/` directory in the repo would work.

- **Enforce schema validation at ingest time.** The Schema Registry is deployed but unused at runtime. Either remove it from the local stack to simplify, or actually wire the processors to validate Avro before consuming events. Having it present but inactive is misleading.

- **Delete dead code earlier.** The commented sklearn Lambda Layer blocks, the `glue.tf.disabled` file — these should have been cleaned up the moment the decision was made to go a different direction. Dead code in infrastructure files is especially confusing because it's not obvious whether it's intentional scaffolding or an incomplete feature.

- **Use a feature store or at least a shared constants file for feature names.** The list of ML features (`FEATURES = [...]`) appears in the notebook and is implicitly replicated in the `predict_risk()` function. A single source of truth for feature definitions would make future model retraining and rule updates cleaner.

- **Start the Kaggle integration earlier.** The real dataset improved the model substantially. Relying only on Faker-generated events for training produces an artificially clean distribution. Incorporating a real dataset should be the first step in any ML loop, not an afterthought.

---

## 💡 Key Technical Insights

- **Kafka advertised listeners require two separate addresses.** The docker-compose Kafka setup uses `PLAINTEXT://kafka:9092` for internal container-to-container communication and `PLAINTEXT_HOST://localhost:29092` for host machine access. Using only one address causes producers/consumers outside Docker to fail silently or with confusing connection errors.

- **DynamoDB requires strings for Decimal types when using boto3.** The `risk_score` field is stored as `str(event['risk_score'])` in DynamoDB rather than as a number. This is because boto3's DynamoDB resource doesn't handle Python `int`/`float` values from JSON parsing without type conversion — it raises `TypeError: Float types are not supported` unless you use `Decimal` or convert to string explicitly.

- **Lambda `source_code_hash` in Terraform prevents stale deployments.** Without `source_code_hash = data.archive_file.*.output_base64sha256`, Terraform won't detect Lambda code changes between applies unless you force a replace. The `archive_file` data source handles this automatically.

- **`[skip ci]` commit tags in GitHub Actions prevent sync loops.** When using a GitHub Action that commits back to the repo (e.g., for RAG knowledge base sync), the commit must include `[skip ci]` in the message to prevent the action from triggering itself infinitely.

- **The ROC-AUC of 0.988 on this dataset is suspiciously high.** The `ml_label` is derived from `risk_score >= 60`, and `risk_score` is computed from the same features used for training. The model is essentially learning to replicate a deterministic rule, not discovering latent signal. In a production system with real outcomes, performance would be significantly lower and the training target should be actual clinical complications, not a heuristic score.

---

## 🧱 Technical Debt & Open Issues

- **`glue.tf.disabled` file** — should be deleted or converted to a proper feature flag if Glue is still a future possibility.

- **Duplicate risk logic in `processor.py` and `lambda_function.py`** — needs extraction to a shared module before adding any new scoring rules.

- **`processor_aws.py` long-polling script** — redundant now that Lambda handles SQS. Should be removed or clearly marked as a debugging tool only.

- **No tests** — zero test coverage across the entire codebase. At minimum, `predict_risk()` in `lambda_inference.py` should have unit tests covering edge cases (GENERAL + diabetic + minor + rejected clearance should always be CRITICAL).

- **Schema Registry is deployed but not enforced** — either wire it into the processor or remove it from the stack.

- **`estimated_duration_min` and `previous_surgeries`** are generated by the producer but play no role in the risk model. They add noise to the ML feature space without contributing signal.

- **TTL is hardcoded to 30 days** in both `lambda_function.py` and `lambda_inference.py`. Should be an environment variable.

---

## 📚 References That Helped

- [Confluent Kafka Docker networking — advertised listeners explained](https://www.confluent.io/blog/kafka-listeners-explained/)
- [boto3 DynamoDB resource — handling decimal types](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html)
- [Terraform `archive_file` data source — source_code_hash pattern](https://registry.terraform.io/providers/hashicorp/archive/latest/docs/data-sources/file)
- [AWS Lambda deployment best practices — layers vs inline dependencies](https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html)
- [Kaggle Anesthesia Dataset](https://www.kaggle.com/) — used for ML training augmentation
