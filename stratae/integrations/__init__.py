"""
Integrations with third-party tools. Import each submodule directly.

* {py:mod}`stratae.integrations.asgi`: middleware that opens an envelope around each request,
  for any ASGI application.
* {py:mod}`stratae.integrations.fastapi` and {py:mod}`stratae.integrations.starlette`:
  routes that activate a scope per request.
* {py:mod}`stratae.integrations.opentelemetry`: bus middleware that traces emits and handlers,
  carrying the trace across brokers and the outbox.
* {py:mod}`stratae.integrations.rabbitmq`: async publish and consume adapters over `aiormq`.
"""
