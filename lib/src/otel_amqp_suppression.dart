// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'dart:async';

const _key = #dartastic_otel_amqp_suppress;

/// Whether AMQP instrumentation is suppressed in the current [Zone]
/// (i.e. the caller is inside [runWithoutAmqpInstrumentation] or
/// [runWithoutAmqpInstrumentationAsync]).
bool amqpInstrumentationSuppressed() => Zone.current[_key] == true;

/// Runs [body] with AMQP instrumentation suppressed: `tracedAmqp*`
/// calls inside it invoke their callbacks directly without spans.
T runWithoutAmqpInstrumentation<T>(T Function() body) =>
    runZoned(body, zoneValues: {_key: true});

/// Async variant of [runWithoutAmqpInstrumentation]; suppression
/// follows the zone across `await` boundaries within [body].
Future<T> runWithoutAmqpInstrumentationAsync<T>(
  Future<T> Function() body,
) =>
    runZoned(body, zoneValues: {_key: true});
