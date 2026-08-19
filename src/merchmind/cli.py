from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from merchmind.config import default_data_dir
from merchmind.pipeline import load_kpis, run_pipeline


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="merchmind",
        description="Fashion retail market intelligence pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="Generate data and build Bronze/Silver/Gold outputs")
    run.add_argument("--data-dir", type=Path, default=default_data_dir())
    run.add_argument("--transactions", type=int, default=50_000)
    run.add_argument("--customers", type=int, default=2_500)
    run.add_argument("--products", type=int, default=500)
    run.add_argument("--seed", type=int, default=42)

    summary = subparsers.add_parser("summary", help="Print the latest executive KPIs")
    summary.add_argument("--data-dir", type=Path, default=default_data_dir())
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "run":
        result = run_pipeline(
            data_dir=args.data_dir,
            transactions=args.transactions,
            customers=args.customers,
            products=args.products,
            seed=args.seed,
        )
        print(json.dumps(asdict(result), indent=2))
        return
    if args.command == "summary":
        print(json.dumps(load_kpis(args.data_dir), indent=2))


if __name__ == "__main__":
    main()
