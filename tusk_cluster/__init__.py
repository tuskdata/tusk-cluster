"""Tusk Cluster Plugin - Distributed queries with DataFusion

This plugin provides distributed query execution using DataFusion
and Arrow Flight for data transfer between workers.

Install:
    pip install tusk-cluster

Usage:
    # Start scheduler
    tusk cluster-scheduler

    # Start workers
    tusk cluster-worker --scheduler localhost:8814

    # Or all-in-one dev mode
    tusk cluster-dev --workers 3
"""

from pathlib import Path
from tusk.plugins.base import TuskPlugin

__version__ = "0.1.0"


class ClusterPlugin(TuskPlugin):
    """Cluster mode plugin for distributed queries"""

    @property
    def name(self) -> str:
        return "tusk-cluster"

    @property
    def version(self) -> str:
        return __version__

    @property
    def description(self) -> str:
        return "Distributed queries with DataFusion and Arrow Flight"

    # ─────────────────────────────────────────────────────────────
    # Tab configuration
    # ─────────────────────────────────────────────────────────────

    @property
    def tab_id(self) -> str:
        return "cluster"

    @property
    def tab_label(self) -> str:
        return "Cluster"

    @property
    def tab_icon(self) -> str:
        return "server"  # Lucide icon name

    @property
    def tab_url(self) -> str:
        return "/cluster"

    # ─────────────────────────────────────────────────────────────
    # Compatibility
    # ─────────────────────────────────────────────────────────────

    @property
    def min_tusk_version(self) -> str:
        return "0.1.0"

    # ─────────────────────────────────────────────────────────────
    # Storage & Config
    # ─────────────────────────────────────────────────────────────

    @property
    def requires_storage(self) -> bool:
        return True  # For job history

    @property
    def requires_config(self) -> bool:
        return True  # For scheduler address, etc.

    # ─────────────────────────────────────────────────────────────
    # Routes
    # ─────────────────────────────────────────────────────────────

    def get_route_handlers(self) -> list:
        from tusk_cluster.routes import ClusterPageController, ClusterAPIController
        return [ClusterPageController, ClusterAPIController]

    def get_templates_path(self) -> Path | None:
        return Path(__file__).parent / "templates"

    def get_static_path(self) -> Path | None:
        return Path(__file__).parent / "static"

    # ─────────────────────────────────────────────────────────────
    # CLI Commands
    # ─────────────────────────────────────────────────────────────

    def get_cli_commands(self) -> dict[str, callable]:
        return {
            "cluster-scheduler": self._cli_scheduler,
            "cluster-worker": self._cli_worker,
            "cluster-dev": self._cli_dev,
        }

    def _cli_scheduler(self, args: list[str]) -> int:
        """Start the cluster scheduler"""
        from tusk_cluster.scheduler import Scheduler

        host = "0.0.0.0"
        port = 8814

        # Parse arguments
        i = 0
        while i < len(args):
            if args[i] == "--host" and i + 1 < len(args):
                host = args[i + 1]
                i += 2
            elif args[i] in ("--port", "-p") and i + 1 < len(args):
                port = int(args[i + 1])
                i += 2
            else:
                i += 1

        print(f"Starting Tusk Cluster Scheduler on {host}:{port}")

        scheduler = Scheduler(host=host, port=port)
        try:
            scheduler.serve()
        except KeyboardInterrupt:
            print("\nShutting down scheduler...")
            scheduler.shutdown()

        return 0

    def _cli_worker(self, args: list[str]) -> int:
        """Start a cluster worker"""
        from tusk_cluster.worker import Worker

        scheduler_host = "localhost"
        scheduler_port = 8814
        host = "0.0.0.0"
        port = 8815
        tusk_url = "http://localhost:8080"

        # Parse arguments
        i = 0
        while i < len(args):
            if args[i] == "--scheduler" and i + 1 < len(args):
                addr = args[i + 1]
                if ":" in addr:
                    scheduler_host, scheduler_port = addr.split(":")
                    scheduler_port = int(scheduler_port)
                else:
                    scheduler_host = addr
                i += 2
            elif args[i] == "--host" and i + 1 < len(args):
                host = args[i + 1]
                i += 2
            elif args[i] in ("--port", "-p") and i + 1 < len(args):
                port = int(args[i + 1])
                i += 2
            elif args[i] == "--tusk-url" and i + 1 < len(args):
                tusk_url = args[i + 1]
                i += 2
            else:
                i += 1

        print(f"Starting Tusk Cluster Worker on {host}:{port}")
        print(f"Connecting to scheduler at {scheduler_host}:{scheduler_port}")
        print(f"Loading catalog from {tusk_url}")

        worker = Worker(
            scheduler_host=scheduler_host,
            scheduler_port=scheduler_port,
            host=host,
            port=port,
            tusk_url=tusk_url
        )
        try:
            worker.serve()
        except KeyboardInterrupt:
            print("\nShutting down worker...")
            worker.shutdown()

        return 0

    def _cli_dev(self, args: list[str]) -> int:
        """Start local cluster with scheduler + workers for development"""
        import subprocess
        import sys
        import time
        import signal
        import os

        num_workers = 3
        tusk_url = "http://localhost:8080"

        # Parse arguments
        i = 0
        while i < len(args):
            if args[i] in ("--workers", "-w") and i + 1 < len(args):
                num_workers = int(args[i + 1])
                i += 2
            elif args[i] == "--tusk-url" and i + 1 < len(args):
                tusk_url = args[i + 1]
                i += 2
            else:
                i += 1

        print("Starting Tusk Cluster (dev mode)")
        print(f"  Scheduler: localhost:8814")
        print(f"  Workers: {num_workers}")
        print(f"  Tusk URL: {tusk_url}")

        python = sys.executable
        processes = []

        # Start scheduler as subprocess
        p_scheduler = subprocess.Popen(
            [python, "-m", "tusk.cli", "cluster-scheduler"],
            start_new_session=False,
        )
        processes.append(p_scheduler)

        # Wait for scheduler to start
        time.sleep(3)

        if p_scheduler.poll() is not None:
            print("ERROR: Scheduler failed to start")
            return 1

        # Start workers as subprocesses
        for i in range(num_workers):
            worker_port = 8815 + i
            p = subprocess.Popen(
                [
                    python, "-m", "tusk.cli", "cluster-worker",
                    "--scheduler", "localhost:8814",
                    "--port", str(worker_port),
                    "--tusk-url", tusk_url,
                ],
                start_new_session=False,
            )
            processes.append(p)
            print(f"  Worker {i+1}: localhost:{worker_port}")

        print("\nCluster started. Press Ctrl+C to stop.")

        try:
            # Wait for any process to exit (or Ctrl+C)
            while True:
                for p in processes:
                    ret = p.poll()
                    if ret is not None:
                        print(f"Process {p.pid} exited with code {ret}")
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down cluster...")
            for p in processes:
                p.terminate()
            for p in processes:
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()
            print("Cluster stopped.")

        return 0

    # ─────────────────────────────────────────────────────────────
    # Datasets (for Data module integration)
    # ─────────────────────────────────────────────────────────────

    def get_datasets(self) -> list[dict]:
        return [
            {
                "name": "job_history",
                "description": "Cluster job execution history",
                "table": "jobs",
            }
        ]

    # ─────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────

    async def on_startup(self) -> None:
        """Initialize cluster plugin storage"""
        from tusk.plugins.storage import init_plugin_db

        schema = """
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            sql TEXT NOT NULL,
            status TEXT NOT NULL,
            progress REAL DEFAULT 0,
            stages_total INTEGER DEFAULT 1,
            stages_completed INTEGER DEFAULT 0,
            created_at TEXT,
            started_at TEXT,
            completed_at TEXT,
            worker_id TEXT,
            rows_processed INTEGER DEFAULT 0,
            error TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
        CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at);
        """

        init_plugin_db(self.name, schema)

    async def on_shutdown(self) -> None:
        """Cleanup on shutdown"""
        pass
