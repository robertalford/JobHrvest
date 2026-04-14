#!/usr/bin/env python3
"""CLI entrypoint for one evolutionary-search cycle."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_DIR)

from app.ml.evo.cycle import run_cycle  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one evo auto-improve cycle")
    parser.add_argument("--model-id", required=True)
    parser.add_argument("--n-candidates", type=int, default=int(os.environ.get("AUTO_IMPROVE_CANDIDATES_N", "3")))
    parser.add_argument("--islands", type=int, default=int(os.environ.get("EVO_ISLANDS", "1")))
    args = parser.parse_args()

    result = asyncio.run(
        run_cycle(
            model_id=args.model_id,
            working_dir=os.path.dirname(PROJECT_DIR),
            n_candidates=args.n_candidates,
            islands_n=args.islands,
        )
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
