// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.
//
// Tests exercise the span/attribute machinery of `tracedAmqpPublish`
// and `tracedAmqpProcess` using a fake `invoke` callback — no real
// AMQP broker is needed.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_amqp/otel_amqp.dart';
import 'package:test/test.dart';

class _MemorySpanExporter implements SpanExporter {
  final List<Span> spans = [];
  bool _shutdown = false;
  @override
  Future<void> export(List<Span> s) async {
    if (_shutdown) return;
    spans.addAll(s);
  }

  @override
  Future<void> forceFlush() async {}

  @override
  Future<void> shutdown() async {
    _shutdown = true;
  }
}

Map<String, Object> _attrMap(Span span) => {
      for (final a in span.attributes.toList()) a.key: a.value,
    };

void main() {
  late _MemorySpanExporter exporter;

  setUp(() async {
    await OTel.reset();
    exporter = _MemorySpanExporter();
    await OTel.initialize(
      serviceName: 'otel_amqp-test',
      detectPlatformResources: false,
      spanProcessor: SimpleSpanProcessor(exporter),
    );
  });

  tearDown(() async {
    await OTel.shutdown();
    await OTel.reset();
  });

  group('tracedAmqpPublish', () {
    test('names span "<destination> publish" per OTel stable semconv',
        () async {
      await tracedAmqpPublish<void>(
        destinationName: 'orders',
        routingKey: 'order.created',
        bodySize: 256,
        invoke: () async {},
      );
      expect(exporter.spans, hasLength(1));
      final span = exporter.spans.single;
      // Stable semconv: span name is `{destination} {operation}` — no
      // system prefix. `messaging.system=rabbitmq` already carries it.
      expect(span.name, 'orders publish');
      expect(span.kind, SpanKind.producer);
      final attrs = _attrMap(span);
      expect(attrs['messaging.system'], 'rabbitmq');
      expect(attrs['messaging.destination.name'], 'orders');
      expect(attrs['messaging.operation.type'], 'publish');
      expect(
          attrs['messaging.rabbitmq.destination.routing_key'], 'order.created');
      expect(attrs['messaging.message.body.size'], 256);
    });

    test('server.address/port recorded when supplied', () async {
      await tracedAmqpPublish<void>(
        destinationName: 'events',
        serverAddress: 'rabbit.internal',
        serverPort: 5672,
        invoke: () async {},
      );
      final attrs = _attrMap(exporter.spans.single);
      expect(attrs['server.address'], 'rabbit.internal');
      expect(attrs['server.port'], 5672);
    });

    test('records exception and sets error status on throw', () async {
      await expectLater(
        tracedAmqpPublish<void>(
          destinationName: 'orders',
          invoke: () async => throw StateError('boom'),
        ),
        throwsA(isA<StateError>()),
      );
      final span = exporter.spans.single;
      expect(span.status, SpanStatusCode.Error);
      expect(_attrMap(span)['error.type'], 'StateError');
    });
  });

  group('tracedAmqpProcess', () {
    test('names span "<destination> process" with CONSUMER kind', () async {
      await tracedAmqpProcess<void>(
        destinationName: 'orders.in',
        routingKey: 'order.created',
        deliveryTag: 42,
        bodySize: 128,
        process: () async {},
      );
      final span = exporter.spans.single;
      expect(span.name, 'orders.in process');
      expect(span.kind, SpanKind.consumer);
      final attrs = _attrMap(span);
      expect(attrs['messaging.operation.type'], 'process');
      expect(attrs['messaging.rabbitmq.message.delivery_tag'], 42);
      expect(
          attrs['messaging.rabbitmq.destination.routing_key'], 'order.created');
    });

    test('records exception and sets error status on throw', () async {
      await expectLater(
        tracedAmqpProcess<void>(
          destinationName: 'orders.in',
          process: () async => throw StateError('reject'),
        ),
        throwsA(isA<StateError>()),
      );
      expect(exporter.spans.single.status, SpanStatusCode.Error);
    });
  });

  group('suppression', () {
    test('runWithoutAmqpInstrumentationAsync skips publish + process spans',
        () async {
      await runWithoutAmqpInstrumentationAsync(() async {
        await tracedAmqpPublish<void>(
          destinationName: 'orders',
          invoke: () async {},
        );
        await tracedAmqpProcess<void>(
          destinationName: 'orders.in',
          process: () async {},
        );
      });
      expect(exporter.spans, isEmpty);
    });
  });
}
