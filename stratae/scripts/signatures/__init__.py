"""
Generate `.pyi` stubs typing injected functions, without running any of the code read.

{py:func}`inject <stratae.depends.inject>` types the function it returns as
`Callable[..., Result]`. Python's typing can't remove named parameters from a signature. So
this command writes a `.pyi` beside each module, where each injected function is declared
without its injected parameters. A type checker reads the stub in place of the module, so it
declares everything the module does. Nothing changes at runtime, and the module's source is
never written.

```{code-block} text
python -m stratae.scripts.signatures app/payments.py app.orders
python -m stratae.scripts.signatures --check app/
```

Modules are named by path, folder or import name. They're read, never imported. A parameter is
injected when `inject` would inject it: its annotation is an `Annotated` whose metadata holds a
`Depends(...)`. Plain aliases are followed, and at most one `type` alias, as `inject` does.
Aliases imported from other modules are read from those modules' source. A module that can't
be found or read is warned about, and its names are taken as not injected.

The stub is the module's syntax tree, stubbed and unparsed, so it's laid out as `ast.unparse`
lays it out. A rerun writes only a stub that changed in meaning, so formatting it is safe.
Only a module with an injected function gets a stub. A stub generated for a module that no
longer injects anything is removed, but one written by hand is left alone. `--check` writes
and removes nothing, and fails when a stub is missing, out of date or left over, or a module
can't be read. Run it in CI to keep the stubs current.

````{example} An injected function, and its stub
<!--- skip: next -->
```{code-block} python
# app/payments.py
@inject
async def refund(stripe: StripeDep, order: int, *, reason: str = "late") -> bool: ...
```

<!--- skip: next -->
```{code-block} python
# app/payments.pyi
async def refund(order: int, *, reason: str='late') -> bool:
    ...
```
````
"""

from ._command import main
from ._modules import Unreadable
from ._stub import Stub, Unneeded, generate

__all__ = ["Stub", "Unneeded", "Unreadable", "generate", "main"]
