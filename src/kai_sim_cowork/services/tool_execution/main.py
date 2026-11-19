"""Process entry point of the tool execution service.

    kai-sim-tool-execution [--instance ID]

Where it sits in the flow
-------------------------
    [main] --> consumer.run(SERVICE): connect as cowork-tool-execution, bind
               q.cowork.{env}.{region}.tool-execution, hand each message to handlers.py

The only file in the component that depends on the broker.
"""

from __future__ import annotations

from kai_sim_cowork.consumer import run
from kai_sim_cowork.services.tool_execution import SERVICE


def main(argv: list[str] | None = None) -> int:
    """Run the tool execution service until Ctrl-C."""
    return run(SERVICE, argv)


if __name__ == "__main__":
    raise SystemExit(main())
