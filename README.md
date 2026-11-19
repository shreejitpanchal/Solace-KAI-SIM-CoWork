# Solace-KAI-SIM-CoWork

A runnable simulation of the **Cowork event backbone on Solace PubSub+**. It
shows one capability end to end: a service publishes an event to a **topic**,
and every consuming service receives it through its own durable **queue**
whose **topic subscriptions** decide what lands in it. The broker does the
routing; no service ever addresses another.

The simulation follows the *Cowork-Solace Architecture* brief (external
consultant edition): its topic convention (section 4), message envelope
(section 5), one-queue-per-consumer rule (section 7) and the example task
flow (section 13). Everything else in the brief (identity integration, TLS,
outbox, DMQ, DR) is deliberately out of scope here.

---

## 1. What runs

| Piece | Plays | Does |
|---|---|---|
| `publisher` | API gateway / task service | Publishes one `task.created` event per interval. Knows only the topic. |
| `orchestrator` | Orchestrator | Owns the task: starts an agent, closes the task on the outcome. |
| `agent-runtime` | Agent runtime | Runs the agent turn: asks tooling for a tool, reports the result. |
| `tool-execution` | Tool execution service | Runs the tool: accepted, progressed, completed or failed. |
| `streaming-gateway` | Notification / streaming gateway | Receives task lifecycle and progress events to push to the user. |
| `audit-pipeline` | Audit and analytics pipeline | Receives a copy of everything. |
| `provision` | Infrastructure as code | Creates the five queues and their subscriptions over SEMP v2, with a 24 h message TTL. |

Each consumer is the same program, `kai-sim-consumer --service <name>`; the
registry in `src/kai_sim_cowork/services.py` says what each one subscribes
to and how it reacts. A reaction is a pure function from an inbound envelope
to the envelopes to publish next, so the whole flow is unit-tested without a
broker.

```
 publisher ──task/created──►  ┌──────────────────── Solace PubSub+ ────────────────────┐
                              │  topic  cowork/dev/apse1/orchestration/task/created/v1 │
                              │    │ matches subscriptions on …                        │
                              │    ├─► q…orchestrator        ├─► q…streaming-gateway   │
                              │    └─► q…audit-pipeline                                │
                              └─────────────────────────────────────────────────────────┘
                                       │
              orchestrator ──agent/command/execute──► q…agent-runtime
                          ──task/state-changed──────► q…streaming-gateway, q…audit-pipeline
              agent-runtime ──agent/agent/started───► q…audit-pipeline
                            ──tooling/command/execute► q…tool-execution
              tool-execution ──tool/accepted|progressed|completed──► q…agent-runtime (completed), q…streaming-gateway (progressed), q…audit-pipeline
              agent-runtime ──agent/agent/completed─► q…orchestrator
              orchestrator ──task/completed─────────► q…streaming-gateway, q…audit-pipeline
```

Every fifth task asks for the `flaky` tool, so the failure branch
(`tool.failed` → `agent.failed` → `task.failed`) runs too.

---

## 2. Topics

The convention from the brief, section 4:

```
events     cowork/{environment}/{region}/{domain}/{entity}/{event}/{version}
commands   cowork/{environment}/{region}/{capability}/command/{command}/{version}
```

`environment` and `region` come from `COWORK_ENVIRONMENT` / `COWORK_REGION`
(default `dev` / `apse1`), so a lower environment never shares topics with
production. The brief's dotted command names (`agent.execute`) map to
capability + command (`agent/command/execute`).

| Envelope `eventType` | Topic | Published by |
|---|---|---|
| `cowork.orchestration.task.created` | `cowork/dev/apse1/orchestration/task/created/v1` | publisher |
| `cowork.orchestration.task.state-changed` | `cowork/dev/apse1/orchestration/task/state-changed/v1` | orchestrator |
| `cowork.orchestration.task.completed` | `cowork/dev/apse1/orchestration/task/completed/v1` | orchestrator |
| `cowork.orchestration.task.failed` | `cowork/dev/apse1/orchestration/task/failed/v1` | orchestrator |
| `cowork.agent.command.execute` | `cowork/dev/apse1/agent/command/execute/v1` | orchestrator |
| `cowork.agent.agent.started` | `cowork/dev/apse1/agent/agent/started/v1` | agent-runtime |
| `cowork.agent.agent.completed` | `cowork/dev/apse1/agent/agent/completed/v1` | agent-runtime |
| `cowork.agent.agent.failed` | `cowork/dev/apse1/agent/agent/failed/v1` | agent-runtime |
| `cowork.tooling.command.execute` | `cowork/dev/apse1/tooling/command/execute/v1` | agent-runtime |
| `cowork.tooling.tool.accepted` | `cowork/dev/apse1/tooling/tool/accepted/v1` | tool-execution |
| `cowork.tooling.tool.progressed` | `cowork/dev/apse1/tooling/tool/progressed/v1` | tool-execution |
| `cowork.tooling.tool.completed` | `cowork/dev/apse1/tooling/tool/completed/v1` | tool-execution |
| `cowork.tooling.tool.failed` | `cowork/dev/apse1/tooling/tool/failed/v1` | tool-execution |

The topic is routing metadata only. Identifiers, tenant, correlation and the
small domain payload travel in the envelope (section 4 below).

---

## 3. Queues and their topic subscriptions

One durable exclusive queue per consuming service, named
`<prefix>.<environment>.<region>.<service>` (`SOLACE_QUEUE_PREFIX`, default
`q.cowork`). This is the complete list the `provision` service creates on
the broker:

| Queue | Topic subscriptions | Why |
|---|---|---|
| `q.cowork.dev.apse1.orchestrator` | `cowork/dev/apse1/orchestration/task/created/v1`<br>`cowork/dev/apse1/agent/agent/completed/v1`<br>`cowork/dev/apse1/agent/agent/failed/v1` | New tasks, and the agent outcomes that close them. |
| `q.cowork.dev.apse1.agent-runtime` | `cowork/dev/apse1/agent/command/execute/v1`<br>`cowork/dev/apse1/tooling/tool/completed/v1`<br>`cowork/dev/apse1/tooling/tool/failed/v1` | Its own command, and the tool outcomes it waits for. |
| `q.cowork.dev.apse1.tool-execution` | `cowork/dev/apse1/tooling/command/execute/v1` | Its own command only. |
| `q.cowork.dev.apse1.streaming-gateway` | `cowork/dev/apse1/orchestration/task/*/v1`<br>`cowork/dev/apse1/tooling/tool/progressed/v1` | Every task lifecycle event (`*` = any event) plus progress. |
| `q.cowork.dev.apse1.audit-pipeline` | `cowork/dev/apse1/>` | Everything in this environment and region (`>` = all remaining levels). The one justified broad subscription. |

Every queue is created with `respectTtlEnabled` on and `maxTtl` =
`QUEUE_MAX_TTL_SECONDS` (default 86400), so a message nobody consumes within
24 hours is expired by the broker.

### How a queue's topic subscription drives delivery

Nothing here sends a message *to* a queue. Publishers publish to a topic and
consumers bind to a queue; the subscription attached to the queue is the
link, and the broker evaluates it for every published message:

1. **Publish.** A service publishes a persistent message to a topic and
   waits for the broker's acknowledgement. It knows nothing about queues.
2. **Match.** The broker compares the topic with the subscriptions on every
   queue. Levels are matched one by one: a literal level must be equal, `*`
   matches exactly one level of any value, `>` at the end matches one or
   more remaining levels.
3. **Spool.** Each queue whose subscription matches gets its own copy of the
   message, whether or not its consumer is connected. A queue with no match
   gets nothing.
4. **Deliver and ack.** The consumer bound to the queue receives the copy,
   handles it, and acks it. The ack removes that copy from that queue only.
   A consumer that disconnects before acking gets the message again on rebind.

Worked out for the topics above (`q…` = `q.cowork.dev.apse1.`):

| Published topic | q…orchestrator | q…agent-runtime | q…tool-execution | q…streaming-gateway | q…audit-pipeline |
|---|---|---|---|---|---|
| `…/orchestration/task/created/v1` | yes | no | no | yes (`task/*`) | yes |
| `…/agent/command/execute/v1` | no | yes | no | no | yes |
| `…/orchestration/task/state-changed/v1` | no | no | no | yes | yes |
| `…/tooling/command/execute/v1` | no | no | yes | no | yes |
| `…/tooling/tool/progressed/v1` | no | no | no | yes | yes |
| `…/tooling/tool/completed/v1` | no | yes | no | no | yes |
| `…/agent/agent/completed/v1` | yes | no | no | no | yes |
| `…/orchestration/task/completed/v1` | no | no | no | yes | yes |

So the number of services that receive an event is exactly the number of
queues whose subscription matches its topic. Adding a consumer is adding a
queue; no publisher changes. Stop any consumer and its queue's *Messages
Queued* count in PubSub+ Manager rises while the others keep flowing; start
it again and it catches up.

---

## 4. The envelope

Every message is a JSON envelope (brief, section 5) with
`eventId`, `eventType`, `schemaVersion`, `occurredAt`, `producer`
(`service`, `version`, `instance`), `tenantId`, `principalId`,
`workspaceId`, `correlationId`, `causationId`, `traceparent`,
`idempotencyKey`, `classification` and a small `payload`. `eventType` plus
`schemaVersion` fix the topic.

A reaction keeps the inbound message's `correlationId`, `traceparent`,
tenant, principal and workspace, and sets `causationId` to the inbound
`eventId`. One task is therefore one correlation chain across all five
queues, and each hop points at the message that caused it:

```
task.created (corr=A, id=A) → agent/command/execute (corr=A, cause=A, id=B)
                            → tooling/command/execute (corr=A, cause=B, id=C)
                            → tool.completed (corr=A, cause=C, id=D) → … → task.completed (corr=A)
```

Consumers validate at the trust boundary: a message whose topic is not a
Cowork topic, whose body is not a valid envelope, or whose `eventType`
disagrees with its topic is logged as a contract violation and dropped
(acknowledged), so it can never block the queue. Envelopes above 64 KiB are
rejected; large content belongs behind `payloadRef`. `eventType`,
`correlationId` and `tenantId` are also set as message user properties, so
they are usable in broker-side selectors.

---

## 5. Run it with Docker

Prerequisites: Docker with the compose plugin, uv, and the PubSub+ Enterprise
image loaded locally (`docker image ls solace-pubsub-enterprise`).

```sh
uv lock                       # writes uv.lock; commit it
cp .env.example .env          # local broker by default; see the file for an external one
docker compose up -d --build  # broker, provision, publisher, five consumers
docker compose ps             # wait until solace shows (healthy): 1-2 minutes
```

Then watch one task travel through the queues:

```sh
docker compose logs -f publisher orchestrator agent-runtime tool-execution streaming-gateway audit-pipeline
```

Expected, every 5 seconds, in this order (the `corr=` prefix is the same on
every line of one task):

- publisher: `published task task-0001 (search) corr=… -> cowork/dev/apse1/orchestration/task/created/v1`
- orchestrator: `received cowork.orchestration.task.created … 2 reaction(s)`, then `published cowork.agent.command.execute -> …/agent/command/execute/v1`
- agent-runtime: `received cowork.agent.command.execute …`, then publishes `agent.started` and `tooling/command/execute`
- tool-execution: `received cowork.tooling.command.execute …`, publishes `accepted`, `progressed`, `completed`
- agent-runtime: `received cowork.tooling.tool.completed …`, publishes `agent.completed`
- orchestrator: `received cowork.agent.agent.completed …`, publishes `task.completed`
- streaming-gateway: one line per task lifecycle event and per progress event, `pushed the update to the user's session`
- audit-pipeline: one line per message in the flow, `stored an immutable audit record`

Every fifth task ends in `task.failed` instead. PubSub+ Manager
(http://localhost:8080, admin / `SOLACE_ADMIN_PASSWORD`) → Queues shows the
five queues, their subscriptions and the TTL settings.

Things to try:

```sh
docker compose stop streaming-gateway     # its queue keeps filling (Messages Queued in Manager)
docker compose start streaming-gateway    # it drains the backlog; nothing was lost
PUBLISH_COUNT=3 docker compose run --rm publisher   # a burst of three tasks alongside the running publisher
docker compose down                       # stop everything; add -v to drop the broker's data
```

To use a broker that is already running, set `COMPOSE_PROFILES=` (empty),
`SOLACE_HOST`, `SOLACE_VPN`, `SOLACE_USERNAME`, `SOLACE_PASSWORD` and
`SEMP_URL` / `SEMP_USERNAME` / `SEMP_PASSWORD` in `.env`.

---

## 6. Run it on the host

```sh
docker compose up -d solace                         # broker only
uv run kai-sim-provision                            # once: the five queues with a 24 h TTL (SEMP on :8080)
uv run kai-sim-consumer --service orchestrator      # one terminal per service
uv run kai-sim-consumer --service agent-runtime
uv run kai-sim-consumer --service tool-execution
uv run kai-sim-consumer --service streaming-gateway
uv run kai-sim-consumer --service audit-pipeline
uv run kai-sim-publisher --count 3 --interval 1     # three tasks, then exit
```

---

## 7. Settings

| Variable | Default | Meaning |
|---|---|---|
| `SOLACE_HOST` | `tcp://localhost:55555` | Broker SMF address (`tcp://solace:55555` inside compose) |
| `SOLACE_VPN` | `default` | Message VPN |
| `SOLACE_USERNAME` / `SOLACE_PASSWORD` | `default` / `default` | Client username and password |
| `SOLACE_QUEUE_PREFIX` | `q.cowork` | Prefix of the service queues |
| `SOLACE_CREATE_QUEUES` | `true` | Let a consumer create a missing queue on bind (no TTL); `false` requires provisioned queues |
| `COWORK_ENVIRONMENT` | `dev` | Second topic level and part of every queue name |
| `COWORK_REGION` | `apse1` | Third topic level and part of every queue name |
| `COWORK_TENANT_ID` | `acme` | `tenantId` the publisher stamps on every task |
| `PUBLISH_INTERVAL_SECONDS` | `5` | Seconds between tasks (decimal allowed) |
| `PUBLISH_COUNT` | `0` | Tasks to publish before exiting; `0` runs until stopped |
| `QUEUE_MAX_TTL_SECONDS` | `86400` | Message expiry `kai-sim-provision` sets on every queue |
| `SEMP_URL` | `http://localhost:8080` | Broker management (SEMP v2) URL (`http://solace:8080` inside compose) |
| `SEMP_USERNAME` / `SEMP_PASSWORD` | `admin` / `admin` | SEMP credentials (compose defaults the password to `SOLACE_ADMIN_PASSWORD`) |
| `PROVISION_TIMEOUT_SECONDS` | `180` | How long `kai-sim-provision` waits for SEMP while the broker boots |
| `LOG_LEVEL` | `INFO` | `DEBUG` logs every broker call and SEMP request |

---

## 8. Develop

`scripts/dev.sh` and `scripts/dev.ps1` are the only place that knows how to
build, lint, test and scan; CI calls the same task names.

```sh
./scripts/dev.sh all          # build vet test cov    (dev.ps1 on PowerShell)
./scripts/dev.sh scan         # vuln + image + trivy
```

See `docs/DEVELOPER.md` for the module map and how to add a service or an
event.
