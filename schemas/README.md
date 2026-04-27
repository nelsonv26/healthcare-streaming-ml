# Schemas Avro

## Versionado

| Versión | Cambios |
|---------|---------|
| v1 | Schema inicial — campos base del consentimiento |
| v2 | Agrega `patient_nationality`, `consent_channel`, `surgery_complexity` |

## Compatibilidad

Todos los campos nuevos en v2 tienen `default` definido,
lo que garantiza **backward compatibility** — un consumer
que espera v1 puede leer eventos v2 sin errores.

## Registro en Schema Registry

```bash
# Local
curl -X POST http://localhost:8081/subjects/consent-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schema\": $(cat consent_v2.avsc | python -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}"
```