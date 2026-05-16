# otel_amqp

OpenTelemetry instrumentation for package:dart_amqp. Wraps AMQP (RabbitMQ) publishes and consumer messages in PRODUCER/CONSUMER spans following OTel stable messaging semantic conventions (messaging.system=rabbitmq, messaging.destination.name, messaging.operation.type).

Span names follow OTel stable semantic conventions:
`{operation.name} {target}` with no system prefix (the system is
already in `db.system.name` / `messaging.system`).

Part of the [Dartastic](https://dartastic.io) OpenTelemetry family.
