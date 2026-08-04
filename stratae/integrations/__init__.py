"""
Integration modules to bridge Stratae modules with third party tools.

Each integration lives in its own submodule. Import the submodule you need directly.

{py:mod}`stratae.integrations.fastapi` and {py:mod}`stratae.integrations.starlette`
both provide a `scoped_route` helper. It activates an
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` around every
request a route handles.

{py:mod}`stratae.integrations.msgspec`
registers a faster {py:func}`serialize <stratae.serde.serialize>` path for
`msgspec.Struct` payloads.

{py:mod}`stratae.integrations.rabbitmq` provides async publish and consume
adapters over `aiormq`. They implement the
{py:class}`Producer <stratae.events.protocols.Producer>` and
{py:class}`Consumer <stratae.events.protocols.Consumer>` protocols.
"""
