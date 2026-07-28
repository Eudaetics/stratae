# Project Walkthrough

The rest of this guide builds one small tool end to end: generating a report for an A/B test run on a product page. Along the way it picks up a piece of Stratae exactly when the tool needs it.

## Building a new tool

Say the business ran an A/B test on a product page. The raw data is a list of visits, each tagged with which variant they saw, what they did (`"purchased"`, `"saved"` for later, or `"left"`), and who they were if known. The script starts out with the initial requirement: generate an aggregate report from the visit data.

:::{dropdown} Sample data used throughout this guide
```{code-block} python
from dataclasses import dataclass

@dataclass
class Visit:
    variant: str
    visited_on: str
    email: str | None  # known if the visitor was logged in; None if anonymous
    action: str  # "purchased", "saved", or "left"

VISITS = [
    Visit("A", "2026-07-20", "alice@example.com", "purchased"),
    Visit("A", "2026-07-20", None, "left"),
    Visit("A", "2026-07-20", "dave@example.com", "saved"),
    Visit("A", "2026-07-21", "erin@example.com", "purchased"),
    Visit("A", "2026-07-21", None, "left"),
    Visit("B", "2026-07-20", "bob@example.com", "saved"),
    Visit("B", "2026-07-20", None, "left"),
    Visit("B", "2026-07-21", None, "left"),
    Visit("B", "2026-07-21", "carol@example.com", "purchased"),
]
```
:::

It starts by aggregating actions by variant:

````{example} Aggregating visits by variant
```{code-block} python
ReportRow = dict[str, str | int]

def build_aggregate_report(visits: list[Visit]) -> list[ReportRow]:
    counts: dict[str, dict[str, int]] = {}
    for visit in visits:
        bucket = counts.setdefault(
            visit.variant, {"purchased": 0, "saved": 0, "left": 0}
        )
        bucket[visit.action] += 1
    return [
        {"variant": variant, **bucket} for variant, bucket in counts.items()
    ]

def print_report(report: list[ReportRow]) -> None:
    for row in report:
        print(row)

print_report(build_aggregate_report(VISITS))
```
```{output}
{'variant': 'A', 'purchased': 2, 'saved': 1, 'left': 2}
{'variant': 'B', 'purchased': 1, 'saved': 1, 'left': 2}
```
````

No Stratae yet, just the logic.

## We need two different reports

Now there's a second kind of report: one aggregate, one detailed. Which builder runs is decided by what's requested. The easy way to write that is a plain argument:

```{code-block} python
def build_detailed_report(visits: list[Visit]) -> list[ReportRow]:
    return [asdict(visit) for visit in visits]

def generate_report(visits: list[Visit], report_type: str) -> list[ReportRow]:
    if report_type == "detailed":
        return build_detailed_report(visits)
    return build_aggregate_report(visits)
```

That works, but it starts to get more complicated when other things call `generate_report`, or as options grow. Every caller now has to know about `report_type` and pass it along, even ones that don't care about the kind of report. Instead, mark a parameter with `Annotated[T, Depends(provider)]` and decorate the function with `@inject`. Now the decision moves out of the signature entirely. The provider runs, and its result is passed in without any caller ever seeing that parameter:

````{example} Injecting which report builder runs
```{code-block} python
from dataclasses import asdict
from typing import Annotated, Callable
from stratae.depends import Depends, inject

def get_report_type() -> str:
    return "aggregate"  # or "detailed"

type ReportBuilder = Callable[[list[Visit]], list[dict]]

@inject
def get_report_builder(report_type: Annotated[str, Depends(get_report_type)]) -> ReportBuilder:
    return build_detailed_report if report_type == "detailed" else build_aggregate_report

@inject
def generate_report(visits: list[Visit], build_report: Annotated[ReportBuilder, Depends(get_report_builder)]):
    return build_report(visits)

print_report(generate_report(VISITS))
```
```{output}
{'visited_on': '2026-07-20', 'variant': 'A', 'purchased': 1, 'saved': 1, 'left': 1}
{'visited_on': '2026-07-21', 'variant': 'A', 'purchased': 1, 'saved': 0, 'left': 1}
{'visited_on': '2026-07-20', 'variant': 'B', 'purchased': 0, 'saved': 1, 'left': 1}
{'visited_on': '2026-07-21', 'variant': 'B', 'purchased': 1, 'saved': 0, 'left': 1}
```
````

`build_report` is injected. `visits` still isn't, that's still a plain argument here. Only which report-building function runs is decided by `stratae.depends`, based on `get_report_type`. Both builders return the same shape, a `list[dict]`, so `print_report` doesn't need to know or care which one ran.

## Visits come from a region-specific API

EU visits have to be served from an EU-resident API, and US visits from a US one. That's a data residency rule, and it has nothing to do with what report was requested. Which API `get_visits` calls should be injected the same way the report builder was:

````{example} Injecting which regional API serves the visits
```{code-block} python
def fetch_from_eu_api() -> list[Visit]:
    print("fetching from the EU API")
    return VISITS

def fetch_from_us_api() -> list[Visit]:
    print("fetching from the US API")
    return VISITS

def get_region() -> str:
    return "EU"

type ApiFetcher = Callable[[], list[Visit]]

@inject
def get_api(region: Annotated[str, Depends(get_region)]) -> ApiFetcher:
    return fetch_from_eu_api if region == "EU" else fetch_from_us_api

@inject
def get_visits(api: Annotated[ApiFetcher, Depends(get_api)]) -> list[Visit]:
    return api()

type Visits = Annotated[list[Visit], Depends(get_visits)]

@inject
def generate_report(visits: Visits, build_report: Annotated[ReportBuilder, Depends(get_report_builder)]):
    return build_report(visits)

print_report(generate_report())
```
```{output}
fetching from the EU API
{'visited_on': '2026-07-20', 'variant': 'A', 'purchased': 1, 'saved': 1, 'left': 1}
{'visited_on': '2026-07-21', 'variant': 'A', 'purchased': 1, 'saved': 0, 'left': 1}
{'visited_on': '2026-07-20', 'variant': 'B', 'purchased': 0, 'saved': 1, 'left': 1}
{'visited_on': '2026-07-21', 'variant': 'B', 'purchased': 1, 'saved': 0, 'left': 1}
```
````

`generate_report` calls itself with no arguments now. `visits` resolves through `get_visits`, which resolves through `get_api`, which resolves through `get_region`, three providers deep, and none of them know about each other beyond the one they each depend on directly.

## Guarding the sensitive report

Anyone can *ask* for the detailed report, but not everyone should get it. `stratae.checks`' `require` guards `build_detailed_report` itself, so it refuses to run no matter how it gets called, whether that's through the picker or directly:

````{example} Guarding the sensitive report builder
```{code-block} python
from dataclasses import dataclass
from stratae.checks import require

@dataclass
class Caller:
    name: str
    role: str  # "staff" or "growth"

def get_caller() -> Caller:
    return Caller("bob", "staff")

type CallerDep = Annotated[Caller, Depends(get_caller)]

@inject
def caller_can_view_visits(caller: CallerDep) -> None:
    if caller.role != "growth":
        raise PermissionError(f"{caller.name} cannot view individual visits")

@require(caller_can_view_visits)
def build_detailed_report(visits: list[Visit]) -> list[dict]:
    return [asdict(visit) for visit in visits]

print_report(generate_report())

try:
    build_detailed_report(get_visits())
except PermissionError as e:
    print(f"blocked: {e}")
```
```{output}
fetching from the EU API
{'visited_on': '2026-07-20', 'variant': 'A', 'purchased': 1, 'saved': 1, 'left': 1}
{'visited_on': '2026-07-21', 'variant': 'A', 'purchased': 1, 'saved': 0, 'left': 1}
{'visited_on': '2026-07-20', 'variant': 'B', 'purchased': 0, 'saved': 1, 'left': 1}
{'visited_on': '2026-07-21', 'variant': 'B', 'purchased': 1, 'saved': 0, 'left': 1}
fetching from the EU API
blocked: bob cannot view individual visits
```
````

`get_report_type` is still `"aggregate"`, so `generate_report` picks `build_aggregate_report` and nothing here is affected by the new guard. `get_report_builder` and `generate_report` didn't need to change either; neither of them depends on `get_caller` at all. The guard is a property of `build_detailed_report` itself, layered on independently of which report was requested.

## Making the guard observable

A guard that only blocks and says nothing is a missed signal. `stratae.events` decouples *that something happened* from *what happens next*. Here that's an access log entry on success and an alert on a blocked attempt, neither of which `build_detailed_report` needs to know about:

````{example} Logging access and alerting on a blocked attempt
```{code-block} python
from stratae.events import DirectBus, Event, PubSub

class ReportViewed:
    def __init__(self, caller: str) -> None:
        self.caller = caller

class AccessDenied:
    def __init__(self, caller: str) -> None:
        self.caller = caller

report_viewed = Event(PubSub, ReportViewed)
access_denied = Event(PubSub, AccessDenied)

bus = DirectBus()
notify_viewed = bus.bind(report_viewed, factory=ReportViewed)
notify_denied = bus.bind(access_denied, factory=AccessDenied)

@bus.handle(report_viewed)
def log_access(e: ReportViewed) -> None:
    print(f"access log: {e.caller} viewed the detailed visit report")

@bus.handle(access_denied)
def alert_denied(e: AccessDenied) -> None:
    print(f"blocked: {e.caller} tried to view individual visits")

def get_caller() -> Caller:
    return Caller("dana", "growth")

type CallerDep = Annotated[Caller, Depends(get_caller)]

@inject
def caller_can_view_visits(caller: CallerDep) -> None:
    if caller.role != "growth":
        notify_denied(caller=caller.name)
        raise PermissionError(f"{caller.name} cannot view individual visits")

@require(caller_can_view_visits)
@inject
def build_detailed_report(visits: list[Visit], caller: CallerDep) -> list[dict]:
    notify_viewed(caller=caller.name)
    return [asdict(visit) for visit in visits]

def get_report_type() -> str:
    return "detailed"

@inject
def get_report_builder(report_type: Annotated[str, Depends(get_report_type)]) -> ReportBuilder:
    return build_detailed_report if report_type == "detailed" else build_aggregate_report

@inject
def generate_report(visits: Visits, build_report: Annotated[ReportBuilder, Depends(get_report_builder)]):
    return build_report(visits)

print_report(generate_report())
```
```{output}
fetching from the EU API
access log: dana viewed the detailed visit report
{'variant': 'A', 'visited_on': '2026-07-20', 'email': 'alice@example.com', 'action': 'purchased'}
{'variant': 'A', 'visited_on': '2026-07-20', 'email': None, 'action': 'left'}
{'variant': 'A', 'visited_on': '2026-07-20', 'email': 'dave@example.com', 'action': 'saved'}
{'variant': 'A', 'visited_on': '2026-07-21', 'email': 'erin@example.com', 'action': 'purchased'}
{'variant': 'A', 'visited_on': '2026-07-21', 'email': None, 'action': 'left'}
{'variant': 'B', 'visited_on': '2026-07-20', 'email': 'bob@example.com', 'action': 'saved'}
{'variant': 'B', 'visited_on': '2026-07-20', 'email': None, 'action': 'left'}
{'variant': 'B', 'visited_on': '2026-07-21', 'email': None, 'action': 'left'}
{'variant': 'B', 'visited_on': '2026-07-21', 'email': 'carol@example.com', 'action': 'purchased'}
```
````

`get_report_type` now requests `"detailed"`, and `dana` is on growth, so `generate_report` resolves to `build_detailed_report` and the guard lets it through. The access log line prints before the report itself, since the event dispatches synchronously as soon as `notify_viewed` is called, before the function returns. `get_visits`, `get_api`, and `get_region` didn't need to change here, so they aren't redefined.

## Testing both branches

`override` replaces a provider's value for the duration of a `with` block, target and all. Swap `get_caller` to a `"staff"` `Caller`, keep the request at `"detailed"`, and the guard refuses exactly as it would for a real staff member, with no real caller or auth system involved:

````{example} Overriding the caller to test both branches
```{code-block} python
from stratae.depends import override

with override(get_caller, Caller("bob", "staff")):
    try:
        print_report(generate_report())
    except PermissionError:
        pass

print_report(generate_report())
```
```{output}
fetching from the EU API
blocked: bob tried to view individual visits
fetching from the EU API
access log: dana viewed the detailed visit report
{'variant': 'A', 'visited_on': '2026-07-20', 'email': 'alice@example.com', 'action': 'purchased'}
{'variant': 'A', 'visited_on': '2026-07-20', 'email': None, 'action': 'left'}
{'variant': 'A', 'visited_on': '2026-07-20', 'email': 'dave@example.com', 'action': 'saved'}
{'variant': 'A', 'visited_on': '2026-07-21', 'email': 'erin@example.com', 'action': 'purchased'}
{'variant': 'A', 'visited_on': '2026-07-21', 'email': None, 'action': 'left'}
{'variant': 'B', 'visited_on': '2026-07-20', 'email': 'bob@example.com', 'action': 'saved'}
{'variant': 'B', 'visited_on': '2026-07-20', 'email': None, 'action': 'left'}
{'variant': 'B', 'visited_on': '2026-07-21', 'email': None, 'action': 'left'}
{'variant': 'B', 'visited_on': '2026-07-21', 'email': 'carol@example.com', 'action': 'purchased'}
```
````

Inside the `with` block, `bob` still requests the detailed report and still gets refused and alerted on. Outside it, `get_caller` is back to `dana`, unchanged, and the same request succeeds. Neither `generate_report` nor `build_detailed_report` had to change to test either path.

## Scoping the data source to the whole run

Three separate calls just hit the EU API three times for what should be one report run. `stratae.lifecycle` scopes caching and cleanup to a unit of work, here the whole run rather than each call. Wrap `get_visits` in `resource` and register it with `.cache(scope)`, and the code after `yield` becomes cleanup, run once the scope exits:

````{example} Caching the visits source for the whole run
```{code-block} python
from stratae.lifecycle import Lifecycle, Scope, resource

lifecycle = Lifecycle([Scope("run", isolation="shared")])

@lifecycle.cache("run")
@resource
def get_visits():
    print("connecting to the visits store")
    try:
        yield get_api()()
    finally:
        print("closing the visits store")

type Visits = Annotated[list[Visit], Depends(get_visits)]

@inject
def generate_report(visits: Visits, build_report: Annotated[ReportBuilder, Depends(get_report_builder)]):
    return build_report(visits)

with lifecycle.start("run"):
    print_report(generate_report())
```
```{output}
connecting to the visits store
fetching from the EU API
access log: dana viewed the detailed visit report
{'variant': 'A', 'visited_on': '2026-07-20', 'email': 'alice@example.com', 'action': 'purchased'}
{'variant': 'A', 'visited_on': '2026-07-20', 'email': None, 'action': 'left'}
{'variant': 'A', 'visited_on': '2026-07-20', 'email': 'dave@example.com', 'action': 'saved'}
{'variant': 'A', 'visited_on': '2026-07-21', 'email': 'erin@example.com', 'action': 'purchased'}
{'variant': 'A', 'visited_on': '2026-07-21', 'email': None, 'action': 'left'}
{'variant': 'B', 'visited_on': '2026-07-20', 'email': 'bob@example.com', 'action': 'saved'}
{'variant': 'B', 'visited_on': '2026-07-20', 'email': None, 'action': 'left'}
{'variant': 'B', 'visited_on': '2026-07-21', 'email': None, 'action': 'left'}
{'variant': 'B', 'visited_on': '2026-07-21', 'email': 'carol@example.com', 'action': 'purchased'}
closing the visits store
```
````

The API gets called once now, no matter how many times something inside the run asks for the visits. The connection closes automatically when the scope exits, whether that's after one report or many. `build_detailed_report`, `get_report_builder`, `get_api`, and `get_region` didn't change; `generate_report` is only rebuilt here because it has to re-bind to the newly decorated `get_visits`, the same way any injected function does when its provider changes.

From here, the [Dependency Injection](dependency-injection), [Checks](checks.md), [Events](events.md), and [Lifecycle](lifecycle.md) guides go deeper into each piece; the [API reference](../api-reference) has the full signature-level detail.
