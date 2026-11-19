"""Process entry point of the agent runtime.

    kai-sim-agent-runtime [--instance ID]

Where it sits in the flow
-------------------------
    [main] --> consumer.run(SERVICE): connect as cowork-agent-runtime, bind
               q.cowork.{env}.{region}.agent-runtime, hand each message to handlers.py

The only file in the component that depends on the broker.
"""

from __future__ import annotations

from kai_sim_cowork.consumer import run
from kai_sim_cowork.services.agent_runtime import SERVICE


def main(argv: list[str] | None = None) -> int:
    """Run the agent runtime until Ctrl-C."""
    return run(SERVICE, argv)


if __name__ == "__main__":
    raise SystemExit(main())
