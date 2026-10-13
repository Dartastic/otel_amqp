# Changelog

## [0.2.2-wip]

## [0.2.1] - 2026-10-13

### Changed

- README: standard header naming the instrumented library, its supported platforms and project links.

## [0.2.0] - 2026-10-11

### Changed

- Requires `dartastic_opentelemetry_api >=1.0.0-rc.4 <1.0.0-rc.5`.
- **Breaking:** depends on `dartastic_opentelemetry_api` only, no
  longer on the SDK. The application chooses and initializes the
  OpenTelemetry SDK. Parameters and fields typed with SDK classes
  now use the API types (`APITracer`, `APISpan`, `APILogger`, ...);
  SDK instances are still accepted wherever an API type is expected.

## [0.1.0] - 2026-08-08

### Added

- Initial release.
