"""API gateway: accepts a user's task and announces it. The one pure publisher.

Where it sits in the flow
-------------------------
    [api-gateway] --orchestration/task/created--> Solace --> every queue that subscribes

The component, file by file
---------------------------
    __init__.py   this declaration: what the service is called and what it is
                  allowed to publish. It has no queue, so no subscriptions.
    tasks.py      the business logic: builds the root envelope of a new task
    main.py       the process entry point, `kai-sim-publisher`

A publisher has no handlers.py because nothing arrives: it starts a flow
instead of reacting to one, so its envelopes are made with Envelope.new()
(a new correlation and trace), where every other service uses derive().

On the broker this declaration becomes
    queue            none
    client username  cowork-api-gateway
    ACL profile      cowork-api-gateway: may publish task.created, nothing else
    client profile   cowork-publish (may not bind to any queue)
"""

from __future__ import annotations

from kai_sim_cowork.services.events import TASK_CREATED

NAME = "api-gateway"

# Everything this service may publish. The broker refuses any other topic.
PUBLISHES = (TASK_CREATED,)
