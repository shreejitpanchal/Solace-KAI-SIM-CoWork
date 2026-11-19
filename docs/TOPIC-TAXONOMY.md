# Solace topic taxonomy and queue mapping

The design of the Cowork topic space on Solace PubSub+ as this simulation
implements it: how a topic is built, which topics exist, and which consumer
queue attracts which topics. Examples use the default deployment,
`COWORK_ENVIRONMENT=dev` and `COWORK_REGION=apse1`.

The code is the source of truth: the topic grammar lives in
`src/kai_sim_cowork/topics.py`, the event types in
`src/kai_sim_cowork/services/events.py` and each service's declaration in
`src/kai_sim_cowork/services/<service>/__init__.py`. Change those and this document in the same
change.

---

## 1. Design principles

- **The topic is routing metadata.** Every level is something a consumer may
  want to filter on. Anything else (identifiers, tenant, correlation, the
  domain payload) travels in the message envelope.
- **Publishers address topics, never queues.** A service publishes to a topic
  and knows nothing about who receives it. Consumers bind to their own queue;
  the topic subscriptions on that queue are the only link between the two.
- **One durable queue per consuming service.** Each queue receives its own
  copy of every matching message, so consumers are isolated from each other.
- **Fixed depth, fixed order, most general level first.** Every topic has
  seven levels, ordered from the widest scope (root, environment, region) to
  the narrowest (event, version), so wildcard subscriptions stay simple.
- **Narrow subscriptions by default.** A queue subscribes to exactly what its
  service handles. The audit pipeline is the one justified broad subscription.
- **Nothing sensitive or unbounded in a topic.** No credentials, tokens, free
  text or per-entity identifiers such as a task id.

---

## 2. Topic structure

Two shapes, both seven levels deep:

```
events     cowork/{environment}/{region}/{domain}/{entity}/{event}/{version}
commands   cowork/{environment}/{region}/{capability}/command/{command}/{version}
```

| # | Level | Events | Commands | Values in this simulation |
|---|---|---|---|---|
| 1 | root | `cowork` | `cowork` | Fixed. Separates Cowork from any other application on the message VPN. |
| 2 | environment | `{environment}` | `{environment}` | `COWORK_ENVIRONMENT`, default `dev`. Lower environments never share topics with production. |
| 3 | region | `{region}` | `{region}` | `COWORK_REGION`, default `apse1`. |
| 4 | domain / capability | `{domain}` that owns the event | `{capability}` that owns the command | `orchestration`, `agent`, `tooling` |
| 5 | entity | `{entity}` the event is about | the literal `command` | `task`, `agent`, `tool`, `command` |
| 6 | event / command | `{event}`, a past-tense fact | `{command}`, an imperative verb | `created`, `state-changed`, `completed`, `failed`, `started`, `accepted`, `progressed`, `execute` |
| 7 | version | `{version}` | `{version}` | `v1` |

**Events and commands.** An event states something that happened and may have
any number of consumers. A command asks one capability to do something and has
exactly one owning consumer. A command is recognised by the literal `command`
in level 5, so both shapes share one parser and one depth.

**Level rules.** Every level is 1 to 64 characters of letters, digits, `.`,
`_` or `-`. The check is `topics.validate_level`, and every value that becomes
a topic level or part of a queue name passes through it, which also keeps `/`,
`*` and `>` out of a level. The existing levels are lower-case with hyphens
between words (`state-changed`); keep to that.

**Link to the envelope.** The envelope's `eventType` is
`cowork.{domain}.{entity}.{name}`. Together with the schema version it fixes
the topic: `cowork.orchestration.task.created` publishes to
`cowork/dev/apse1/orchestration/task/created/v1`. A consumer drops any message
whose `eventType` does not match the topic it arrived on.

---

## 3. Topic catalogue

All topics start with `cowork/dev/apse1/`, shown as `…/`.

### Orchestration domain

| Topic | Kind | Published by | Meaning |
|---|---|---|---|
| `…/orchestration/task/created/v1` | event | publisher (API gateway) | A user task was accepted. |
| `…/orchestration/task/state-changed/v1` | event | orchestrator | The task moved to a new state (`running`). |
| `…/orchestration/task/completed/v1` | event | orchestrator | The task finished; the payload references the result. |
| `…/orchestration/task/failed/v1` | event | orchestrator | The task failed; the payload carries the reason. |

### Agent capability

| Topic | Kind | Published by | Meaning |
|---|---|---|---|
| `…/agent/command/execute/v1` | command | orchestrator | Run an agent turn for a task. |
| `…/agent/agent/started/v1` | event | agent-runtime | The agent turn started. |
| `…/agent/agent/completed/v1` | event | agent-runtime | The agent turn finished. |
| `…/agent/agent/failed/v1` | event | agent-runtime | The agent turn failed. |

### Tooling capability

| Topic | Kind | Published by | Meaning |
|---|---|---|---|
| `…/tooling/command/execute/v1` | command | agent-runtime | Run a tool for a task. |
| `…/tooling/tool/accepted/v1` | event | tool-execution | The tool request was accepted. |
| `…/tooling/tool/progressed/v1` | event | tool-execution | Progress update (percent). |
| `…/tooling/tool/completed/v1` | event | tool-execution | The tool finished; the payload references the result. |
| `…/tooling/tool/failed/v1` | event | tool-execution | The tool returned an error. |

---

## 4. Subscriptions and wildcards

A subscription is a topic pattern attached to a queue. The broker compares
every published topic with it level by level:

| Pattern element | Matches | Example |
|---|---|---|
| literal level | that exact level | `…/tooling/tool/progressed/v1` matches one topic |
| `*` | exactly one level, any value | `…/orchestration/task/*/v1` matches every task event |
| `>` (last level only) | one or more remaining levels | `cowork/dev/apse1/>` matches everything in the environment and region |

Subscriptions are built only through `topics.TopicSpace`:

| Builder | Produces | Used for |
|---|---|---|
| `event_subscription(domain, entity, event)` | `…/{domain}/{entity}/{event}/v1`, with `*` allowed for entity and event | Events a service reacts to or forwards |
| `command_subscription(capability, command)` | `…/{capability}/command/{command}/v1` | The commands a capability owns |
| `all_subscription()` | `cowork/{environment}/{region}/>` | Audit pipeline only |

---

## 5. Consumer queues

Queue name: `{prefix}.{environment}.{region}.{service}`, with the prefix from
`SOLACE_QUEUE_PREFIX` (default `q.cowork`). Example:
`q.cowork.dev.apse1.orchestrator`.

Every queue is created by the `provision` service over SEMP v2 with the same
properties:

| Property | Value | Effect |
|---|---|---|
| Durable | yes | Messages are spooled while the consumer is down. |
| `accessType` | `exclusive` | One active consumer per queue; delivery order is preserved. |
| `owner` | `cowork-{service}` | The service's own client username; the only identity that can bind. |
| `permission` | `no-access` | Every other client username is refused. |
| `respectTtlEnabled` | `true` | The queue honours message expiry. |
| `maxTtl` | `QUEUE_MAX_TTL_SECONDS`, default 86400 | A message not consumed within 24 hours is expired and moved to the DMQ. |
| `maxRedeliveryCount` | `QUEUE_MAX_REDELIVERY_COUNT`, default 3 | A message the consumer has failed that many times is moved to the DMQ. |
| `deadMsgQueue` | `{queue}.dmq` | The queue's own dead message queue. |

### Dead message queues

Each consumer queue has one dead message queue (DMQ), so a dead message is
attributable to the service that could not handle it:

| Consumer queue | Dead message queue |
|---|---|
| `q.cowork.dev.apse1.orchestrator` | `q.cowork.dev.apse1.orchestrator.dmq` |
| `q.cowork.dev.apse1.agent-runtime` | `q.cowork.dev.apse1.agent-runtime.dmq` |
| `q.cowork.dev.apse1.tool-execution` | `q.cowork.dev.apse1.tool-execution.dmq` |
| `q.cowork.dev.apse1.streaming-gateway` | `q.cowork.dev.apse1.streaming-gateway.dmq` |
| `q.cowork.dev.apse1.audit-pipeline` | `q.cowork.dev.apse1.audit-pipeline.dmq` |

A DMQ is outside the topic taxonomy: it carries no topic subscriptions, so
no published topic routes to it, including through the audit queue's `>`.
Only the broker moves messages in, from the one queue that names it, when a
message expires or exceeds the redelivery limit. A dead message keeps its
original topic and envelope. DMQs are durable, exclusive, closed to every
client username and do not expire their contents. Messages are published DMQ-eligible; the broker discards an
ineligible message instead of moving it.

---

## 6. Consumer queue to topic mapping

`q…` stands for `q.cowork.dev.apse1.` and `…/` for `cowork/dev/apse1/`.

### 6.1 Subscriptions on each queue

| Queue | Consumer | Topic subscriptions | Topics attracted | Why |
|---|---|---|---|---|
| `q…orchestrator` | orchestrator | `…/orchestration/task/created/v1`<br>`…/agent/agent/completed/v1`<br>`…/agent/agent/failed/v1` | 3 | New tasks, and the agent outcomes that close them. |
| `q…agent-runtime` | agent-runtime | `…/agent/command/execute/v1`<br>`…/tooling/tool/completed/v1`<br>`…/tooling/tool/failed/v1` | 3 | Its own command, and the tool outcomes it waits for. |
| `q…tool-execution` | tool-execution | `…/tooling/command/execute/v1` | 1 | Its own command only. |
| `q…streaming-gateway` | streaming-gateway | `…/orchestration/task/*/v1`<br>`…/tooling/tool/progressed/v1` | 5 | Every task lifecycle event, plus tool progress, to push to the user's session. |
| `q…audit-pipeline` | audit-pipeline | `cowork/dev/apse1/>` | 13 | An immutable record of everything in the environment and region. |

### 6.2 Routing matrix: every topic against every queue

A mark means the queue receives a copy of each message published to that
topic.

| Published topic | Publisher | q…orchestrator | q…agent-runtime | q…tool-execution | q…streaming-gateway | q…audit-pipeline | Copies |
|---|---|:-:|:-:|:-:|:-:|:-:|:-:|
| `…/orchestration/task/created/v1` | publisher | ✔ | | | ✔ | ✔ | 3 |
| `…/orchestration/task/state-changed/v1` | orchestrator | | | | ✔ | ✔ | 2 |
| `…/orchestration/task/completed/v1` | orchestrator | | | | ✔ | ✔ | 2 |
| `…/orchestration/task/failed/v1` | orchestrator | | | | ✔ | ✔ | 2 |
| `…/agent/command/execute/v1` | orchestrator | | ✔ | | | ✔ | 2 |
| `…/agent/agent/started/v1` | agent-runtime | | | | | ✔ | 1 |
| `…/agent/agent/completed/v1` | agent-runtime | ✔ | | | | ✔ | 2 |
| `…/agent/agent/failed/v1` | agent-runtime | ✔ | | | | ✔ | 2 |
| `…/tooling/command/execute/v1` | agent-runtime | | | ✔ | | ✔ | 2 |
| `…/tooling/tool/accepted/v1` | tool-execution | | | | | ✔ | 1 |
| `…/tooling/tool/progressed/v1` | tool-execution | | | | ✔ | ✔ | 2 |
| `…/tooling/tool/completed/v1` | tool-execution | | ✔ | | | ✔ | 2 |
| `…/tooling/tool/failed/v1` | tool-execution | | ✔ | | | ✔ | 2 |

Reading the matrix:

- Each command topic has exactly one consumer besides audit: the capability
  that owns it.
- `agent/agent/started` and `tooling/tool/accepted` have no functional
  consumer today. They are published as facts and only audit records them; a
  new consumer can subscribe later without any publisher changing.
- The streaming gateway picks up `task/created`, `state-changed`, `completed`
  and `failed` through one `*` subscription, so a new task event reaches it
  with no change to the queue.

### 6.3 One task through the queues

Successful path, in publish order:

| Step | Publisher | Topic | Delivered to (besides audit) |
|---|---|---|---|
| 1 | publisher | `…/orchestration/task/created/v1` | orchestrator, streaming-gateway |
| 2 | orchestrator | `…/agent/command/execute/v1` | agent-runtime |
| 3 | orchestrator | `…/orchestration/task/state-changed/v1` | streaming-gateway |
| 4 | agent-runtime | `…/agent/agent/started/v1` | none |
| 5 | agent-runtime | `…/tooling/command/execute/v1` | tool-execution |
| 6 | tool-execution | `…/tooling/tool/accepted/v1` | none |
| 7 | tool-execution | `…/tooling/tool/progressed/v1` | streaming-gateway |
| 8 | tool-execution | `…/tooling/tool/completed/v1` | agent-runtime |
| 9 | agent-runtime | `…/agent/agent/completed/v1` | orchestrator |
| 10 | orchestrator | `…/orchestration/task/completed/v1` | streaming-gateway |

On the failure path (every fifth task uses the `flaky` tool) steps 8 to 10
become `tool/failed`, `agent/failed` and `task/failed`, routed to the same
queues as their `completed` counterparts.

---

## 7. Access control: who may use which topic

The taxonomy is enforced on the broker, not only followed by convention.
Each service connects as its own client username with its own ACL profile,
both named `cowork-{service}`. Every ACL profile disallows publish and
subscribe by default.

### 7.1 Publish rights

A service may publish only the topics it owns. These are publish exceptions
on its ACL profile, as exact topics with no wildcards:

| Client username / ACL profile | May publish (`…/` = `cowork/dev/apse1/`) |
|---|---|
| `cowork-api-gateway` | `…/orchestration/task/created/v1` |
| `cowork-orchestrator` | `…/agent/command/execute/v1`<br>`…/orchestration/task/state-changed/v1`<br>`…/orchestration/task/completed/v1`<br>`…/orchestration/task/failed/v1` |
| `cowork-agent-runtime` | `…/agent/agent/started/v1`<br>`…/tooling/command/execute/v1`<br>`…/agent/agent/completed/v1`<br>`…/agent/agent/failed/v1` |
| `cowork-tool-execution` | `…/tooling/tool/accepted/v1`<br>`…/tooling/tool/progressed/v1`<br>`…/tooling/tool/completed/v1`<br>`…/tooling/tool/failed/v1` |
| `cowork-streaming-gateway` | nothing |
| `cowork-audit-pipeline` | nothing |

Each of the 13 topics has exactly one publisher, so an event on a topic is
attributable to one identity.

### 7.2 Receive rights

No ACL profile has a subscribe exception. A service cannot subscribe to a
topic directly; it receives only what its queue attracts (section 6), and it
can bind only the queue it owns. Subscriptions are set by the provisioner
over the management API, never by a service. The audit pipeline's
`cowork/dev/apse1/>` is therefore a property of one queue that one identity
can read, not a right any client could exercise.

### 7.3 Client profiles

What kind of client a service is, shared by role:

| Client profile | Used by | Guaranteed send | Guaranteed receive | Create queues | Transactions, bridging |
|---|---|:-:|:-:|:-:|:-:|
| `cowork-publish` | api-gateway | yes | no | no | no |
| `cowork-publish-consume` | orchestrator, agent-runtime, tool-execution | yes | yes | no | no |
| `cowork-consume` | streaming-gateway, audit-pipeline | no | yes | no | no |

Each client username is limited to five connections.

---

## 8. Evolving the taxonomy

- **New event on an existing entity.** Add the `eventType` constant in
  `services/events.py`; the topic follows from it. Queues with a `*` on the event
  level receive it automatically, and so does audit. Add an explicit
  subscription for any other service that needs it, and add the event to the
  publishing service's `PUBLISHES` so its ACL profile gains the exception.
- **New consumer.** Add one service package under `services/` with its
  subscriptions, one line in the registry's manifest and one compose service with its own password variable.
  `queue_specs()` and `access_specs()` derive the queue, its DMQ, the client
  username and the ACL profile, and the provisioner creates them. No
  publisher changes.
- **New domain or capability.** Choose a level-4 name that passes
  `validate_level`, then add its events and commands as above.
- **Breaking schema change.** Publish under a new version level (`v2`)
  alongside `v1`. Subscriptions are pinned to `v1`, so existing consumers are
  unaffected until they opt in. The audit queue receives both versions.
- **New environment or region.** Set `COWORK_ENVIRONMENT` / `COWORK_REGION`.
  Topics and queue names both change, so the deployments do not overlap.

Full walkthroughs are in `docs/DEVELOPER.md`, section 6.

---

## 9. Not covered by this design

Tenant-level topic isolation, a dedicated message VPN per environment,
OAuth or mutual-TLS authentication (the services use basic authentication),
TLS and DR replication are in the Cowork-Solace architecture brief but are
not simulated here. The broker's built-in `default` client username is left
enabled; disable it on any shared broker. Nothing consumes
the dead message queues; alerting on them and replaying from them are
follow-ups. The tenant travels in the envelope (`tenantId`), not in the topic.
