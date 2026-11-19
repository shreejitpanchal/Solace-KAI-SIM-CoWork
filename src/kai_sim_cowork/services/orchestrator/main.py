"""Process entry point of the orchestrator.

    kai-sim-orchestrator [--instance ID]

Where it sits in the flow
-------------------------
    [main] --> consumer.run(SERVICE): connect as cowork-orchestrator, bind
               q.cowork.{env}.{region}.orchestrator, hand each message to handlers.py

This file is all a service needs in order to run: the shared consumer loop
does the connecting, validating, publishing and acknowledging. It is the
only file in the component that depends on the broker.
"""

from __future__ import annotations

from kai_sim_cowork.consumer import run
from kai_sim_cowork.services.orchestrator import SERVICE


def main(argv: list[str] | None = None) -> int:
    """Run the orchestrator until Ctrl-C."""
    return run(SERVICE, argv)


if __name__ == "__main__":
    raise SystemExit(main())
