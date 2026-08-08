// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.
//
// Traced publish + consume against a RabbitMQ broker on localhost.
// Run a broker first, e.g.: docker run -p 5672:5672 rabbitmq:3

import 'package:dart_amqp/dart_amqp.dart';
import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart'
    hide Client; // dart_amqp also exports a `Client`.
import 'package:otel_amqp/otel_amqp.dart';

Future<void> main() async {
  // 1. Bring up OTel before any AMQP traffic so spans have somewhere
  //    to go. Configure exporters/endpoint as usual for your app.
  await OTel.initialize(serviceName: 'otel_amqp-example');

  final client = Client(settings: ConnectionSettings(host: 'localhost'));
  final channel = await client.channel();

  // 2. Producer side: `publishTraced` is a drop-in for `publish` and
  //    emits a PRODUCER span named `orders publish` with
  //    messaging.system=rabbitmq and the routing key attribute.
  final exchange = await channel.exchange('orders', ExchangeType.TOPIC);
  await exchange.publishTraced('{"orderId": 1}', 'order.created');

  // 3. Consumer side: wrap each delivery in `tracedAmqpProcess` to get
  //    a CONSUMER span named `orders.in process` per message.
  final queue = await channel.queue('orders.in');
  await queue.bind(exchange, 'order.#');
  final consumer = await queue.consume();
  consumer.listen((AmqpMessage message) {
    tracedAmqpProcess<void>(
      destinationName: 'orders.in',
      routingKey: message.routingKey,
      deliveryTag: message.deliveryTag,
      bodySize: message.payload?.length,
      process: () async {
        // Handle the message; exceptions are recorded on the span and
        // rethrown.
      },
    );
  });

  // 4. To run a block without AMQP spans (e.g. health checks), use the
  //    suppression helpers:
  await runWithoutAmqpInstrumentationAsync(() async {
    await exchange.publishTraced('ping', 'health.check'); // no span
  });

  await client.close();
  await OTel.shutdown();
}
