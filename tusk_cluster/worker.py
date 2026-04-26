"""Worker for distributed query execution"""

import asyncio
import os
import re
import socket
import time
import threading
from pathlib import Path
from uuid import uuid4

import psutil
import pyarrow as pa
import pyarrow.flight as flight
from datafusion import SessionContext

from tusk.core.logging import get_logger

log = get_logger("worker")

_CLUSTER_TOKEN = os.environ.get("TUSK_CLUSTER_TOKEN", "")


def _prefix_token(data: str) -> str:
    """Prefix cluster token to data if configured."""
    if _CLUSTER_TOKEN:
        return f"TOKEN:{_CLUSTER_TOKEN}:{data}"
    return data


class FlightWorkerServer(flight.FlightServerBase):
    """Arrow Flight server for worker"""

    def __init__(self, worker: "Worker", location: str):
        super().__init__(location)
        self.worker = worker

    def do_get(self, context, ticket):
        """Handle query execution request"""
        sql = ticket.ticket.decode()
        log.info("Executing query", sql=sql[:100])

        try:
            table = self.worker.execute_query(sql)
            return flight.RecordBatchStream(table)
        except Exception as e:
            log.error("Query execution failed", error=str(e))
            raise

    def do_action(self, context, action):
        """Handle control actions"""
        action_type = action.type

        if action_type == "ping":
            yield flight.Result(b"pong")

        elif action_type == "status":
            cpu = psutil.cpu_percent()
            memory = psutil.Process().memory_info().rss / 1024 / 1024
            memory_percent = psutil.virtual_memory().percent
            status = f"{cpu}:{memory}:{memory_percent}"
            yield flight.Result(status.encode())

        else:
            yield flight.Result(b"unknown_action")


class Worker:
    """Distributed query worker using DataFusion"""

    def __init__(
        self,
        scheduler_host: str = "localhost",
        scheduler_port: int = 8814,
        host: str = "0.0.0.0",
        port: int = 8815,
        data_path: str | None = None,
        tusk_url: str | None = None,
    ):
        self.id = f"worker_{str(uuid4())[:6]}"
        self.scheduler_host = scheduler_host
        self.scheduler_port = scheduler_port
        self.host = host
        self.port = port
        self.data_path = Path(data_path).expanduser() if data_path else Path.home() / "data"
        self.tusk_url = tusk_url or "http://localhost:8080"

        self.ctx = SessionContext()
        self._server: FlightWorkerServer | None = None
        self._running = False
        self._heartbeat_task: asyncio.Task | None = None
        self._registered_tables: set[str] = set()

    def _get_local_ip(self) -> str:
        """Get local IP address for registration"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    def _register_with_scheduler(self) -> bool:
        """Register with the scheduler"""
        try:
            location = flight.Location.for_grpc_tcp(self.scheduler_host, self.scheduler_port)
            client = flight.FlightClient(location)

            local_ip = self._get_local_ip()
            if self.host == "0.0.0.0":
                register_host = local_ip
            else:
                register_host = self.host

            action = flight.Action(
                "register",
                _prefix_token(f"{self.id}:{register_host}:{self.port}").encode()
            )
            list(client.do_action(action))

            log.info("Registered with scheduler",
                     scheduler=f"{self.scheduler_host}:{self.scheduler_port}",
                     worker_id=self.id)
            return True

        except Exception as e:
            log.error("Failed to register with scheduler", error=str(e))
            return False

    def _unregister_from_scheduler(self) -> None:
        """Unregister from the scheduler"""
        try:
            location = flight.Location.for_grpc_tcp(self.scheduler_host, self.scheduler_port)
            client = flight.FlightClient(location)

            action = flight.Action("unregister", _prefix_token(self.id).encode())
            list(client.do_action(action))

            log.info("Unregistered from scheduler", worker_id=self.id)

        except Exception as e:
            log.warning("Failed to unregister from scheduler", error=str(e))

    def _load_catalog_from_tusk(self) -> int:
        """Load table catalog from Tusk and register in DataFusion.

        Returns number of tables registered.
        """
        import httpx

        try:
            url = f"{self.tusk_url}/api/data/catalog"
            response = httpx.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()
            tables = data.get("tables", [])

            registered = 0
            for table in tables:
                name = table.get("name")
                path = table.get("path")
                fmt = table.get("format")

                if not name or not path:
                    continue

                # Skip if already registered
                if name in self._registered_tables:
                    continue

                try:
                    p = Path(path).expanduser()
                    if not p.exists():
                        log.warning("Table file not found", table=name, path=path)
                        continue

                    if fmt == "parquet":
                        self.ctx.register_parquet(name, str(p))
                    elif fmt == "csv":
                        self.ctx.register_csv(name, str(p))
                    elif fmt == "json":
                        self.ctx.register_json(name, str(p))
                    else:
                        log.warning("Unsupported format", table=name, format=fmt)
                        continue

                    self._registered_tables.add(name)
                    registered += 1
                    log.info("Table registered", table=name, path=path, format=fmt)

                except Exception as e:
                    log.error("Failed to register table", table=name, error=str(e))

            return registered

        except httpx.ConnectError:
            log.warning("Could not connect to Tusk", url=self.tusk_url)
            return 0
        except Exception as e:
            log.error("Failed to load catalog", error=str(e))
            return 0

    def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats to scheduler (runs in background thread)"""
        while self._running:
            time.sleep(5)
            if not self._running:
                break
            try:
                cpu = psutil.cpu_percent()
                memory = psutil.Process().memory_info().rss / 1024 / 1024
                memory_percent = psutil.virtual_memory().percent

                location = flight.Location.for_grpc_tcp(self.scheduler_host, self.scheduler_port)
                client = flight.FlightClient(location)

                action = flight.Action(
                    "heartbeat",
                    _prefix_token(f"{self.id}:{cpu}:{memory}:{memory_percent}").encode()
                )
                list(client.do_action(action))

                log.debug("Heartbeat sent", cpu=cpu, memory_mb=memory)

            except Exception as e:
                log.warning("Heartbeat failed", error=str(e))

    def _register_file_sources(self, sql: str) -> str:
        """Auto-register files mentioned as read_*() calls and rewrite SQL to use table names."""
        formats = {
            "read_parquet": "parquet",
            "read_csv": "csv",
            "read_json": "json",
        }
        rewritten = sql
        for func_name, fmt in formats.items():
            pattern = rf"{func_name}\s*\(\s*['\"]([^'\"]+)['\"]\s*\)"
            for match in re.finditer(pattern, rewritten, re.IGNORECASE):
                full_match = match.group(0)
                path = match.group(1)
                p = Path(path).expanduser()
                if not p.exists():
                    continue
                table_name = re.sub(r'[^a-zA-Z0-9_]', '_', p.stem).lower()
                if table_name in self._registered_tables:
                    rewritten = rewritten.replace(full_match, table_name)
                    continue
                try:
                    register_fn = getattr(self.ctx, f"register_{fmt}")
                    register_fn(table_name, str(p))
                    self._registered_tables.add(table_name)
                    rewritten = rewritten.replace(full_match, table_name)
                    log.debug("Registered file", path=str(p), table=table_name, format=fmt)
                except Exception as e:
                    log.warning("Failed to register file", path=str(p), format=fmt, error=str(e))
        return rewritten

    def execute_query(self, sql: str) -> pa.Table:
        """Execute a query using DataFusion"""
        start = time.time()

        sql = self._register_file_sources(sql)

        try:
            df = self.ctx.sql(sql)
            table = df.to_arrow_table()

            elapsed = time.time() - start
            log.info("Query executed",
                     rows=table.num_rows,
                     cols=table.num_columns,
                     time_ms=round(elapsed * 1000, 2))

            return table

        except Exception as e:
            log.error("Query execution failed", error=str(e))
            raise

    def serve(self) -> None:
        """Start the worker server (blocking)"""
        if not self._register_with_scheduler():
            log.warning("Could not register with scheduler, starting anyway...")

        # Load table catalog from Tusk
        tables_loaded = self._load_catalog_from_tusk()
        if tables_loaded > 0:
            log.info("Catalog loaded", tables=tables_loaded)
            print(f"Loaded {tables_loaded} tables from Tusk catalog")

        location = f"grpc://{self.host}:{self.port}"
        self._server = FlightWorkerServer(self, location)
        self._running = True

        log.info("Worker starting", worker_id=self.id, host=self.host, port=self.port)
        print(f"Worker {self.id} listening on {self.host}:{self.port}")

        # Start heartbeat in background thread
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

        try:
            self._server.serve()
        finally:
            self._running = False
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
            self._unregister_from_scheduler()

    def shutdown(self) -> None:
        """Shutdown the worker"""
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._server:
            self._server.shutdown()
        self._unregister_from_scheduler()
        log.info("Worker shutdown", worker_id=self.id)
