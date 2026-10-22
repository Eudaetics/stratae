"""
Integrations with third-party tools. Import each submodule directly.

* {py:mod}`stratae.integrations.fastapi` and {py:mod}`stratae.integrations.starlette`:
  routes that activate a scope per request.
* {py:mod}`stratae.integrations.msgspec`: a faster
  {py:func}`serialize <stratae.serde.serialize>` for `msgspec.Struct`.
* {py:mod}`stratae.integrations.rabbitmq`: async publish and consume adapters over `aiormq`.
"""
