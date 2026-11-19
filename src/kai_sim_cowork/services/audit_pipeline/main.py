"""Process entry point of the audit pipeline.

    kai-sim-audit-pipeline [--instance ID]

Where it sits in the flow
-------------------------
    [main] --> consumer.run(SERVICE): connect as cowork-audit-pipeline, bind
               q.cowork.{env}.{region}.audit-pipeline, hand each message to handlers.py

The only file in the component that depends on the broker.
"""

from __future__ import annotations

from kai_sim_cowork.consumer import run
from kai_sim_cowork.services.audit_pipeline import SERVICE


def main(argv: list[str] | None = None) -> int:
    """Run the audit pipeline until Ctrl-C."""
    return run(SERVICE, argv)


if __name__ == "__main__":
    raise SystemExit(main())
