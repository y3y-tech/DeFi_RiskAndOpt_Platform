"""
Crypto Whale Detection & Risk Analytics Platform
Entry point.

Commands:
    monitor     Run the real-time whale detection system only
    dashboard   Run only the analytics dashboard (reads from DB)
    all         Run both (default)

Usage:
    python main.py [monitor|dashboard|all]
    python main.py --help
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import threading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_monitor() -> None:
    from whale_detector.orchestrator import Platform
    platform = Platform()
    asyncio.run(platform.run())


def run_dashboard() -> None:
    from whale_detector.dashboard.app import run_dashboard as _run
    _run()


def run_all() -> None:
    """Run the monitor in a background thread and dashboard in the main thread."""
    monitor_thread = threading.Thread(target=run_monitor, daemon=True, name="monitor")
    monitor_thread.start()
    run_dashboard()


def main() -> None:
    parser = argparse.ArgumentParser(description="Crypto Whale Detection & Risk Analytics Platform")
    parser.add_argument(
        "command",
        nargs="?",
        default="all",
        choices=["monitor", "dashboard", "all"],
        help="Component to run (default: all)",
    )
    args = parser.parse_args()

    logger.info("Starting platform in '%s' mode", args.command)

    if args.command == "monitor":
        run_monitor()
    elif args.command == "dashboard":
        run_dashboard()
    else:
        run_all()


if __name__ == "__main__":
    main()
