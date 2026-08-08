// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dart_amqp/dart_amqp.dart' as a;
import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_amqp_suppression.dart';

const _tracerName = 'otel_amqp';
const _system = 'rabbitmq';

// Stable messaging semconv keys not yet in the API's Messaging enum
// (which predates stable semconv). Tracked as task #143 to backfill.
const _destinationName = 'messaging.destination.name';
const _operationType = 'messaging.operation.type';
const _messageBodySize = 'messaging.message.body.size';
const _rabbitRoutingKey = 'messaging.rabbitmq.destination.routing_key';
const _rabbitDeliveryTag = 'messaging.rabbitmq.message.delivery_tag';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

Attributes _attrs({
  required String operationType,
  String? destinationName,
  String? routingKey,
  int? bodySize,
  int? deliveryTag,
  String? serverAddress,
  int? serverPort,
}) =>
    OTel.attributesFromMap(<String, Object>{
      Messaging.messagingSystem.key: _system,
      _operationType: operationType,
      if (destinationName != null) _destinationName: destinationName,
      if (routingKey != null) _rabbitRoutingKey: routingKey,
      if (bodySize != null) _messageBodySize: bodySize,
      if (deliveryTag != null) _rabbitDeliveryTag: deliveryTag,
      if (serverAddress != null) Server.serverAddress.key: serverAddress,
      if (serverPort != null) Server.serverPort.key: serverPort,
    });

void _recordError(Span span, Object e, StackTrace st) {
  span.addAttributes(
    OTel.attributes([
      OTel.attributeString(
        ErrorAttributes.errorType.key,
        e.runtimeType.toString(),
      ),
    ]),
  );
  span.recordException(e, stackTrace: st);
  span.setStatus(SpanStatusCode.Error, e.toString());
}

/// Runs [invoke] inside a PRODUCER-kind span named
/// `<destination> publish`, with `messaging.system=rabbitmq` and the
/// stable AMQP messaging semantic conventions.
///
/// [destinationName] is the exchange name (or queue name for direct
/// queue publishes). [routingKey] becomes
/// `messaging.rabbitmq.destination.routing_key`. [bodySize] becomes
/// `messaging.message.body.size`.
///
/// Span name follows stable semconv: `{destination} publish` — no
/// system prefix. The system is in the `messaging.system` attribute.
Future<R> tracedAmqpPublish<R>({
  required String destinationName,
  required Future<R> Function() invoke,
  String? routingKey,
  int? bodySize,
  String? serverAddress,
  int? serverPort,
}) async {
  if (amqpInstrumentationSuppressed()) return invoke();
  final span = _tracer().startSpan(
    '$destinationName publish',
    kind: SpanKind.producer,
    attributes: _attrs(
      operationType: 'publish',
      destinationName: destinationName,
      routingKey: routingKey,
      bodySize: bodySize,
      serverAddress: serverAddress,
      serverPort: serverPort,
    ),
  );
  try {
    return await invoke();
  } catch (e, st) {
    _recordError(span, e, st);
    rethrow;
  } finally {
    span.end();
  }
}

/// Runs [process] inside a CONSUMER-kind span named
/// `<destination> process` for a single AMQP message. Use this from
/// inside your `Consumer.listen` callback to create a child span per
/// delivery.
///
/// [destinationName] is the queue name. [routingKey] is the original
/// routing key (typically `message.routingKey`). [deliveryTag] becomes
/// `messaging.rabbitmq.message.delivery_tag`. [bodySize] becomes
/// `messaging.message.body.size`.
///
/// Span name follows stable semconv: `{destination} process`.
Future<R> tracedAmqpProcess<R>({
  required String destinationName,
  required Future<R> Function() process,
  String? routingKey,
  int? deliveryTag,
  int? bodySize,
  String? serverAddress,
  int? serverPort,
}) async {
  if (amqpInstrumentationSuppressed()) return process();
  final span = _tracer().startSpan(
    '$destinationName process',
    kind: SpanKind.consumer,
    attributes: _attrs(
      operationType: 'process',
      destinationName: destinationName,
      routingKey: routingKey,
      bodySize: bodySize,
      deliveryTag: deliveryTag,
      serverAddress: serverAddress,
      serverPort: serverPort,
    ),
  );
  try {
    return await process();
  } catch (e, st) {
    _recordError(span, e, st);
    rethrow;
  } finally {
    span.end();
  }
}

/// Traced publish on an `Exchange`. Drop-in replacement: append
/// `Traced` to `publish`.
///
/// Mirrors `Exchange.publish`'s positional+named signature for the
/// common case. Callers who need `MessageProperties` should use
/// [tracedAmqpPublish] directly so the span lifecycle wraps their
/// own `publish` call.
extension OTelExchange on a.Exchange {
  /// Traced `publish`. The exchange name becomes
  /// `messaging.destination.name`; [routingKey] becomes
  /// `messaging.rabbitmq.destination.routing_key`. Message size is
  /// recorded as `messaging.message.body.size` when [message] is a
  /// `List<int>` or `String`.
  Future<void> publishTraced(
    Object message,
    String routingKey, {
    bool mandatory = false,
    bool immediate = false,
  }) =>
      tracedAmqpPublish<void>(
        destinationName: name,
        routingKey: routingKey,
        bodySize: message is List<int>
            ? message.length
            : (message is String ? message.length : null),
        invoke: () async => publish(
          message,
          routingKey,
          mandatory: mandatory,
          immediate: immediate,
        ),
      );
}
