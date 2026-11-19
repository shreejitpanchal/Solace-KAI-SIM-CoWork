# Solace-KAI-SIM-CoWork

Simulation of the Cowork event backbone on Solace PubSub+. It exists to show
one thing: a service publishes to a **topic**, and each consuming service
receives through its own durable **queue** whose **topic subscriptions** do
the routing. Keep every change in service of that demonstration.

Read `README.md` for what it demonstrates and `docs/DEVELOPER.md` for how a
service component is structured (section 2), the module map, the message
trace and the "add a service / handler / event" walkthroughs.

## Commands

All gates go through the mirrored task runners; CI calls the same names.

```sh
./scripts/dev.sh all          # build vet test cov   (.\scripts\dev.ps1 all on PowerShell)
./scripts/dev.sh test         # pytest only
./scripts/dev.sh scan         # vuln + image + trivy (report-only)
./scripts/dev.sh up | down    # the compose stack
```

Do not run these yourself; ask the user to run the matching task and read
`scripts/logs/<task>.log`. `uv lock` after any `pyproject.toml` change; the
Dockerfile builds with `--locked`.

## Architecture rules

- **Dependency direction is downwards**: `config` → `topics` → `envelope`
  → `services` (declarations and handlers) → `broker` → `consumer` → entry
  points (each service's `main.py`, `provision`). A service's `__init__.py`
  and `handlers.py` never import `broker.py` or `consumer.py`; only its
  `main.py` does.
- **One package per service, same three files.** `services/<name>/` holds
  `__init__.py` (the declaration: `NAME`, `subscriptions()`, `PUBLISHES`,
  `SERVICE`), `handlers.py` (one pure function per inbound event type,
  `(Envelope, Producer) -> list[Envelope]`, routed by a `HANDLERS` table)
  and `main.py` (`run(SERVICE)`). A service imports nothing from another
  service; they share only `services/events.py`, `services/base.py` and the
  library modules. Keep the packages parallel: they are the customer-facing
  reference for how to structure a service, so a change to the pattern goes
  into all of them and into `docs/DEVELOPER.md` section 2.
- **The registry is the source of truth.** `services/__init__.py` lists the
  service packages in `_MANIFEST`; `SERVICES`, `queue_specs()` and
  `access_specs()` are derived from their declarations. Add a service as a
  new package plus one manifest line, one console script and one compose
  service. Never hard-code a queue name or subscription elsewhere.
- **Least privilege, derived from the registry.** Every principal (the
  gateway and each service) connects as its own client username
  `cowork-<principal>` with its own ACL profile and a role-based client
  profile, all from `services.access_specs()`. Never use the broker's
  `default` username or profiles for a service, never make a username
  configurable, and never grant a subscribe exception: consumers only bind
  the queue they own. A handler may emit only the event types in its
  service's `PUBLISHES`; add the type there when adding the handler.
- **Topics follow the brief**: events
  `cowork/{env}/{region}/{domain}/{entity}/{event}/v1`, commands
  `cowork/{env}/{region}/{capability}/command/{command}/v1`. Build them only
  through `topics.TopicSpace`; the envelope's `eventType`
  (`cowork.{domain}.{entity}.{name}`) plus `schemaVersion` fixes the topic.
- **Validate before splice.** Any string that becomes a topic level or part
  of a queue name goes through `topics.validate_level`. CLI arguments and
  environment values are validated before any broker call.
- **Validate before use.** Consumers accept nothing from a message until
  `TopicSpace.parse` and `Envelope.from_json` have passed, and the envelope's
  `eventType` must match its topic. Violations are logged and dropped
  (acked), never raised; a handler must not block the queue.
- **Reactions derive, never construct.** Use `inbound.derive(...)` so
  `correlationId`, `traceparent`, tenant, principal and workspace carry over
  and `causationId` is set. `Envelope.new()` is for root messages only
  (the publisher).
- **Nothing sensitive in messages or topics.** No credentials, tokens or
  large content; payloads stay small (64 KiB limit) and large content goes
  behind `payloadRef`. Passwords are masked in every log line
  (`BrokerConfig.masked`, `SempConfig.masked`, and SEMP request bodies in
  `provision._loggable`). Service passwords come only from
  `SOLACE_PASSWORD_<SERVICE>`; compose hands each container just its own.
- **Guaranteed messaging throughout.** Persistent publish with ack, durable
  exclusive queues, ack after handling. A handler that raises is settled
  `FAILED` (redelivered, then dead-lettered); every publish is DMQ-eligible.
  Queue properties (owner, TTL, access type, redelivery limit, DMQ),
  subscriptions and all access control are set only by `provision.py` over
  SEMP v2; each queue's DMQ is `<queue>.dmq` from
  `TopicSpace.dmq_name`.

## Conventions

- Python 3.12, uv, ruff (line length 100, rules E F I B UP), pytest with
  branch coverage. Match the existing comment density and docstring style:
  every module opens with "where it sits in the flow".
- Tests mirror modules one to one (`tests/test_<module>.py`, and
  `tests/services/test_<service>.py` for each service package: declaration,
  handlers, entry point). Cover the lines you touch in the same change,
  including every rejection path and every log line a user would look for.
  `tests/test_services.py` runs the whole flow through a fake router; keep
  it green when changing a handler.
- Errors are actionable: say what failed, the offending value (masked if
  secret) and the fix or setting to change.
- Keep docs in sync in the same change: the README's topic table (section 2),
  queue table and routing matrix (section 3), the settings table (section 7)
  `docs/TOPIC-TAXONOMY.md` (topic catalogue, queue mapping, routing matrix)
  and `docs/DEVELOPER.md`. `.env.example` documents every variable.
- Commit messages explain the why; end with
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

## Out of scope by design

Identity integration (OAuth, mTLS; services use basic auth), a dedicated
message VPN, TLS, transactional outbox, consuming or replaying from the
DMQs, DR and per-tenant ACLs from the brief are not simulated. Do not add them unless
asked; mention them as follow-ups instead.
