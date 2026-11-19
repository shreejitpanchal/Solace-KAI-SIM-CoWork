"""Process entry point of the streaming gateway.

    kai-sim-streaming-gateway [--instance ID]

Where it sits in the flow
-------------------------
    [main] --> consumer.run(SERVICE): connect as cowork-streaming-gateway, bind
               q.cowork.{env}.{region}.streaming-gateway, hand each message to handlers.py

The only file in the component that depends on the broker.
"""

from __future__ import annotations

from kai_sim_cowork.consumer import run
from kai_sim_cowork.services.streaming_gateway import SERVICE


def main(argv: list[str] | None = None) -> int:
    """Run the streaming gateway until Ctrl-C."""
    return run(SERVICE, argv)


if __name__ == "__main__":
    raise SystemExit(main())
