# Developer guide

How the simulation is put together and how to extend it. The README covers
what it demonstrates and how to run it. Section 2 is the reference for how a
service component is structured.

## 1. Repository layout

```
src/kai_sim_cowork/     shared library (config, topics, envelope, broker, consumer) and provision
  services/             one package per service component, plus the registry that lists them
tests/                  pytest suite; mirrors the package module for module
  services/             one test file per service component
scripts/dev.sh, dev.ps1 the only place that knows how to build, lint, test, scan
scripts/audit_gate.py   the scan gate shared by both scripts
docker/Dockerfile       service image (all entry points)
docker-compose.yml      local stack: broker (optional), provision, publisher, five consumers
.env.example            every setting, with the two broker modes
.github/workflows/      ci.yml (gates) and tag.yml (release on tag)
pyproject.toml          dependencies, entry points, ruff/pytest/coverage config
uv.lock                 pinned dependency set; commit it
```

Python 3.12, managed with [uv](https://docs.astral.sh/uv/). `uv sync` creates
`.venv` from `uv.lock`; `uv run <command>` runs inside it.

## 2. How a service component is structured

This is the part to copy when building a real service. Every service is its
own package under `src/kai_sim_cowork/services/` and owns everything about
itself; nothing about a service lives anywhere else.

```
services/
  base.py                 shapes: Service, Handler, dispatch(), QueueSpec, AccessSpec
  events.py               the event and command types: the contract services share
  access.py               principal -> client username and role profile
  __init__.py             the registry: one line per service in _MANIFEST

  orchestrator/           kai-sim-orchestrator
    __init__.py           declaration: NAME, subscriptions(), PUBLISHES, SERVICE
    handlers.py           business logic: one pure function per inbound event type
    main.py               entry point: run(SERVICE)
  agent_runtime/          kai-sim-agent-runtime      same three files
  tool_execution/         kai-sim-tool-execution     same three files
  streaming_gateway/      kai-sim-streaming-gateway  same three files (a sink: publishes nothing)
  audit_pipeline/         kai-sim-audit-pipeline     same three files (a sink: publishes nothing)
  api_gateway/            kai-sim-publisher
    __init__.py           declaration: NAME, PUBLISHES (no queue, so no subscriptions)
    tasks.py              business logic: builds the root task envelope
    main.py               entry point: the publish loop
```

What each file is for, using the orchestrator as the example:

| File | Answers | Depends on | Changes when |
|---|---|---|---|
| `orchestrator/__init__.py` | What is this service on the broker? Its name, the topics its queue subscribes to, the event types it may publish. | `base`, `events`, `topics` | It should receive or emit a different event. |
| `orchestrator/handlers.py` | What does it do with each message? `on_task_created`, `on_agent_completed`, `on_agent_failed`, and the `HANDLERS` table that routes by `eventType`. | `base`, `events`, `envelope` | The business logic changes. |
| `orchestrator/main.py` | How does it run? Three lines: `run(SERVICE)`. | `consumer` | Almost never. |

The rules that keep the components independent:

- **A service imports nothing from another service.** What they share is
  `events.py` (the contract) and the library modules. They meet only on the
  broker.
- **Handlers are pure.** One inbound `Envelope` in, a list of envelopes out.
  No connecting, publishing, acknowledging or logging; `consumer.run` does
  that identically for every service. This is why each handler has a plain
  unit test in `tests/services/test_<service>.py` with no broker.
- **Only `main.py` touches the broker**, through `consumer.run`.
- **The declaration is the single source for the broker objects.** The
  registry derives the queue, its dead message queue, the client username,
  the ACL profile and its publish exceptions from `SERVICE`, so a service
  cannot do on the broker anything its declaration does not state.
- **Outgoing envelopes are derived, not built**: `inbound.derive(...)`
  carries the correlation, trace and tenant context forward. Only the API
  gateway, which starts a flow, uses `Envelope.new()`.

In a real system each service package would be its own repository or
deployable, `events.py` and `envelope.py` a shared contract library, and
`consumer.py` plus `broker.py` the shared consumer SDK the brief describes
(section 13). They sit in one package here so the simulation runs from one
image.

## 3. Module map

| Module | Responsibility | Depends on |
|---|---|---|
| `config.py` | Environment settings with defaults and validation: broker, per-service credentials, Cowork topic space, publisher cadence, SEMP. Passwords masked for logs. | stdlib |
| `topics.py` | `TopicSpace`: builds event and command topics, subscriptions and queue names for one environment/region, parses inbound topics. `validate_level` is the one gate for identifiers that become topic levels. | stdlib |
| `envelope.py` | `Envelope`: the message contract. `new()` roots a correlation, `derive()` makes a reaction with causation set, `from_json()` validates at the trust boundary. | topics |
| `services/__init__.py` | The registry `SERVICES`, built from the manifest of service packages. `queue_specs()` lists every queue and `access_specs()` every client username, ACL profile and client profile for provisioning. | the service packages |
| `services/base.py`, `events.py`, `access.py` | The shapes a service is built from, the shared event types, and the username and role-profile rules. | envelope, topics |
| `services/<service>/` | One service component: declaration, handlers, entry point (section 2). | base, events, envelope, topics; `main.py` also consumer |
| `broker.py` | Wrapper over the Solace Python API: persistent, DMQ-eligible publish with ack; durable-queue receive with ack, or a failed settlement when the handler raises. `Inbound`/`Outbound` are the SDK-free shapes. | solace-pubsubplus (imported lazily) |
| `consumer.py` | The consumer runtime every service shares. `run(SERVICE)` connects as the service's username, binds its queue, validates, calls its handlers, publishes. Also the generic entry point `kai-sim-consumer --service X`. | broker, envelope, services, topics |
| `publisher.py` | Launcher only: hands over to `services/api_gateway/main.py`. Exists so the build scripts have a top-level file for the `kai-sim-publisher` binary. | services.api_gateway |
| `provision.py` | Entry point `kai-sim-provision`: SEMP v2 client that creates the access control in `access_specs()`, then every queue in `queue_specs()` with its owner, subscriptions, TTL, redelivery limit and dead message queue. | config, services, httpx |

Dependency direction is strictly downwards: a service's declaration and
handlers never import the broker (only its `main.py` does, through
`consumer`), so every handler is a pure function and the whole flow is
testable in `tests/test_services.py` with a ten-line fake router.

## 4. How one task moves through the code

```
services/api_gateway/main.main
  └─ loop: tasks.build_task → Envelope.new(TASK_CREATED, ...)           root: corr = id
           Broker.publish(Outbound(space.topic_for(eventType), json, properties))

services/X/main.main → consumer.run(SERVICE)
  ├─ SERVICE.subscriptions(space)            what X's queue subscribes to (X/__init__.py)
  └─ Broker.subscribe(space.queue_name(prefix, X), subscriptions, handler)
        └─ consumer.make_handler → handle(Inbound)
              ├─ space.parse(topic)                    contract violation → warn, drop
              ├─ Envelope.from_json(payload)           contract violation → warn, drop
              ├─ envelope.eventType == parts.event_type  else drop
              ├─ SERVICE.react(envelope, parts, me)     X/handlers.py: HANDLERS[eventType] → [derive(...)]
              └─ Broker.publish(...) for each reaction  corr kept, cause = inbound id
```

Two invariants to preserve:

- **Validate before use.** Nothing from a message is used before
  `space.parse` and `Envelope.from_json` have accepted it. Keep the
  contract-violation tests in `tests/test_consumer.py` green.
- **Validate before splice.** Any string that becomes a topic level or a
  queue name goes through `topics.validate_level`.

Threading: the SDK delivers messages on its own thread and calls the handler
there. `Broker.wait` only parks the main thread. Handlers must be quick and
must not raise for a bad message (log and return; the wrapper acks). A
handler that does raise gets the message again: the wrapper settles it
`FAILED`, and the queue's redelivery limit sends it to the DMQ if it keeps
failing.

## 5. Local development loop

```sh
./scripts/dev.sh all        # build vet test cov; logs in scripts/logs/<task>.log
./scripts/dev.sh test       # just the tests
./scripts/dev.sh up         # the compose stack; down to stop
SOLACE_INTEGRATION=1 uv run pytest tests/test_broker.py   # round trip against the running broker
```

The integration test is the one place that still uses the broker's `default`
client username: it creates its own throwaway queue, which the provisioned
service identities are not allowed to do.

On PowerShell use `.\scripts\dev.ps1` with the same task names.

## 6. Extending

**Add a consuming service.** Copy an existing package, for example
`services/tool_execution/`, to `services/<name>/` and change its three files:
the declaration (`NAME`, `subscriptions()`, `PUBLISHES`, the receipt text),
the handlers, and the import in `main.py`. Then:

1. add `<name>.SERVICE` to `_MANIFEST` in `services/__init__.py` (one line);
2. add its console script `kai-sim-<name>` to `pyproject.toml`;
3. add a `<name>:` service to `docker-compose.yml` using the `x-consumer`
   anchor with `entrypoint: ["kai-sim-<name>"]` and its own
   `SOLACE_PASSWORD: ${SOLACE_PASSWORD_<NAME>:?...}`, and add that variable
   to the `provision` service and to `.env.example`;
4. add `tests/services/test_<name>.py` (copy a sibling) and its
   subscriptions to the registry test in `tests/test_services.py`.

Its queue, dead message queue, client username, ACL profile and client
profile are provisioned from `queue_specs()` and `access_specs()`
automatically. Keep the name to 25 characters or fewer: `cowork-<name>` is
also the ACL profile name, which the broker limits to 32. The README tables
in sections 1 and 3 list each service.

**Add a handler to a service.** In `services/<name>/handlers.py` write
`on_<event>(inbound, me) -> list[Envelope]` and add it to `HANDLERS`. Add
the matching subscription in the package's `__init__.py` and any new event
type it emits to `PUBLISHES`. Each service's tests check that every
subscription has a handler and that handlers emit only declared types.

**Add an event or command.** Add its `eventType` constant to
`services/events.py` (`cowork.{domain}.{entity}.{name}` for events,
`cowork.{capability}.command.{name}` for commands); the topic follows from
`TopicSpace.topic_for`. Publish it from a handler with
`inbound.derive(...)`, add it to that service's `PUBLISHES` (the broker
refuses the publish otherwise, and the flow tests in
`tests/test_services.py` fail first), and subscribe to it in the receiving
service's `subscriptions()` with `space.event_subscription(...)` /
`space.command_subscription(...)`. Add a row to the README topic table and
to the access-control table.

**Access control.** `services.access_specs()` derives one `AccessSpec` per
principal (the gateway plus each service in the manifest): client username and ACL profile
`cowork-<principal>`, a client profile chosen by role from
`CLIENT_PROFILES`, and publish rights for exactly the topics of its
`PUBLISHES`. `provision.provision_access` applies them before any queue is
created, because a queue names its owner. Three rules hold it together:

- A service's username is derived, never configured: `consumer.run` and
  `services/api_gateway/main.py` call
  `broker_config(principal=..., username=client_username(...))`.
  Only the password is a setting.
- Consumers never add subscriptions (`Broker.subscribe` only binds unless
  `SOLACE_CREATE_QUEUES=true`), so no ACL profile needs a subscribe exception.
- The provisioner only adds. Removing an entry from `PUBLISHES` or a
  subscription does not remove it from an existing broker; delete it in
  PubSub+ Manager or recreate the broker volume.

To give a role a new permission, change `provision.client_profile_settings`;
to restrict where a service may connect from, add client-connect exceptions
next to the publish exceptions in `SempClient.ensure_acl_profile`.

**Change the topic convention.** `TopicSpace` in `topics.py` is the single
source for building and parsing; `tests/test_topics.py` pins the shape.

**Add an envelope field.** Extend `Envelope`, `to_dict()` and `from_json()`
together, and the wire-format test in `tests/test_envelope.py`.

**Queue properties.** `provision.queue_body` (create) and `policy_body`
(update) set them over SEMP v2; `tests/test_provision.py` has a fake SEMP
that stores whatever is sent. Put a new property in `policy_body` if an
existing queue should pick it up on the next run.

**Dead message queues.** Three pieces work together; change them together:

- `TopicSpace.dmq_name` names the DMQ (`<queue>.dmq`) and `queue_specs()`
  puts it on each `QueueSpec`. `provision.py` creates the DMQ (`dmq_body`)
  before the service queue and sets `deadMsgQueue`, `maxRedeliveryCount`
  and `redeliveryEnabled` on the service queue.
- `Broker.publish` marks every message DMQ-eligible. Without the flag the
  broker discards instead of moving.
- `Broker.subscribe` acks when the handler returns and settles `FAILED`
  when it raises (`broker.deliver` decides). A failed message is
  redelivered until `maxRedeliveryCount`, then moved to the DMQ.

Contract violations are still acked and dropped by `consumer.make_handler`.
To dead-letter them instead, have the handler signal them and settle those
messages `REJECTED` (add the outcome to
`with_required_message_outcome_support`), which moves a message to the DMQ
without redelivery. A consumer for the DMQs (alerting, replay) would be a
new entry point, not a `SERVICES` entry: a DMQ has no subscriptions.

## 7. CI and release

`.github/workflows/ci.yml` runs `scripts/dev.sh all scan` (and the
PowerShell twin on Windows) on a manual dispatch and on PRs that touch CI
config. `tag.yml` builds and releases on a version tag: binaries per
`BUILD_TARGETS`, an image only if a root `Dockerfile` exists (ours is under
`docker/`, so no image is published). Dependabot keeps the SHA-pinned
actions current.
