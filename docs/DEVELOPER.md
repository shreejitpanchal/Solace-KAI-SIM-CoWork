# Developer guide

How the simulation is put together and how to extend it. The README covers
what it demonstrates and how to run it.

## 1. Repository layout

```
src/kai_sim_cowork/     the package (three entry points: publisher, consumer, provision)
tests/                  pytest suite; mirrors the package module for module
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

## 2. Module map

| Module | Responsibility | Depends on |
|---|---|---|
| `config.py` | Environment settings with defaults and validation: broker, Cowork topic space, publisher cadence, SEMP. Passwords masked for logs. | stdlib |
| `topics.py` | `TopicSpace`: builds event and command topics, subscriptions and queue names for one environment/region, parses inbound topics. `validate_level` is the one gate for identifiers that become topic levels. | stdlib |
| `envelope.py` | `Envelope`: the message contract. `new()` roots a correlation, `derive()` makes a reaction with causation set, `from_json()` validates at the trust boundary. | topics |
| `services.py` | The registry `SERVICES`: each service's subscriptions and reaction. `queue_specs()` lists every queue for provisioning. | envelope, topics |
| `broker.py` | Wrapper over the Solace Python API: persistent publish with ack, durable-queue receive with ack. `Inbound`/`Outbound` are the SDK-free shapes. | solace-pubsubplus (imported lazily) |
| `publisher.py` | Entry point `kai-sim-publisher`: the simulated API gateway, one `task.created` per interval. | broker, envelope, services, topics |
| `consumer.py` | Entry point `kai-sim-consumer --service X`: binds X's queue, validates, reacts, publishes. | broker, envelope, services, topics |
| `provision.py` | Entry point `kai-sim-provision`: SEMP v2 client that creates every queue in `queue_specs()` with its subscriptions and the TTL. | config, services, httpx |

Dependency direction is strictly downwards: `services.py` never imports the
broker, so every reaction is a pure function and the whole flow is testable
in `tests/test_services.py` with a ten-line fake router.

## 3. How one task moves through the code

```
publisher.main
  └─ loop: publisher.build_task → Envelope.new(TASK_CREATED, ...)       root: corr = id
           Broker.publish(Outbound(space.topic_for(eventType), json, properties))

consumer.main (--service X)
  ├─ SERVICES[X].subscriptions(space)        what X's queue subscribes to
  └─ Broker.subscribe(space.queue_name(prefix, X), subscriptions, handler)
        └─ consumer.make_handler → handle(Inbound)
              ├─ space.parse(topic)                    contract violation → warn, drop
              ├─ Envelope.from_json(payload)           contract violation → warn, drop
              ├─ envelope.eventType == parts.event_type  else drop
              ├─ SERVICES[X].react(envelope, parts, me) → [Envelope.derive(...)]
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
must not raise for a bad message (log and return; the wrapper acks).

## 4. Local development loop

```sh
./scripts/dev.sh all        # build vet test cov; logs in scripts/logs/<task>.log
./scripts/dev.sh test       # just the tests
./scripts/dev.sh up         # the compose stack; down to stop
SOLACE_INTEGRATION=1 uv run pytest tests/test_broker.py   # round trip against the running broker
```

On PowerShell use `.\scripts\dev.ps1` with the same task names.

## 5. Extending

**Add a consuming service.** Add one `Service` entry to `SERVICES` in
`services.py` (name, receipt text, subscriptions, reaction) and one
`<name>:` service to `docker-compose.yml` using the `x-consumer` anchor with
`command: ["--service", "<name>"]`. Its queue is provisioned from
`queue_specs()` automatically. Add its subscriptions to the registry test
and, if it reacts, a reaction test in `tests/test_services.py`; the README
tables in sections 1 and 3 list each service.

**Add an event or command.** Add its `eventType` constant to `services.py`
(`cowork.{domain}.{entity}.{name}` for events, `cowork.{capability}.command.{name}`
for commands); the topic follows from `TopicSpace.topic_for`. Publish it
from a reaction with `inbound.derive(...)` and subscribe to it with
`space.event_subscription(...)` / `space.command_subscription(...)`. Add a
row to the README topic table.

**Change the topic convention.** `TopicSpace` in `topics.py` is the single
source for building and parsing; `tests/test_topics.py` pins the shape.

**Add an envelope field.** Extend `Envelope`, `to_dict()` and `from_json()`
together, and the wire-format test in `tests/test_envelope.py`.

**Queue properties.** `provision.queue_body` (create) and `ttl_body`
(update) set them over SEMP v2; `tests/test_provision.py` has a fake SEMP
that stores whatever is sent. A DMQ per queue would be added there
(`deadMsgQueue`, `maxRedeliveryCount`) together with the DMQ-eligible flag
on published messages.

**Redeliver instead of dropping on handler errors.** In
`broker.Broker.subscribe` the adapter acks in `finally`. To make unexpected
exceptions redeliver, settle the message with a failed outcome instead, and
set a max-redelivery count and a DMQ on the queue so a poison message ends
there rather than looping.

## 6. CI and release

`.github/workflows/ci.yml` runs `scripts/dev.sh all scan` (and the
PowerShell twin on Windows) on a manual dispatch and on PRs that touch CI
config. `tag.yml` builds and releases on a version tag: binaries per
`BUILD_TARGETS`, an image only if a root `Dockerfile` exists (ours is under
`docker/`, so no image is published). Dependabot keeps the SHA-pinned
actions current.
