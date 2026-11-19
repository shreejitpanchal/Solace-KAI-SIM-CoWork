"""Launcher for the API gateway component, kept at the package root for the binary build.

    kai-sim-publisher [--count N] [--interval S]

Where it sits in the flow
-------------------------
    [publisher] --> services/api_gateway/main.py --task.created--> Solace --> queues ...

The publisher's code lives with the other service components, in
services/api_gateway/ (declaration, tasks.py, main.py). This module only
gives the build scripts (scripts/dev.sh, dev.ps1) a top-level file to turn
into the `kai-sim-publisher` binary, next to consumer.py and provision.py.
"""

from __future__ import annotations

from kai_sim_cowork.services.api_gateway.main import main

__all__ = ["main"]

if __name__ == "__main__":
    raise SystemExit(main())
