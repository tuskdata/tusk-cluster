"""Cluster plugin API routes"""

import asyncio
import os
import secrets
import sqlite3
import subprocess
import threading
from datetime import datetime, timezone
from litestar import Controller, Request, get, post, delete
from litestar.exceptions import NotAuthorizedException
from litestar.params import Body
from litestar.response import Template

from tusk.core.logging import get_logger
from tusk.studio.htmx import is_htmx
from tusk.plugins.storage import get_plugin_db_path

log = get_logger("cluster_api")

# Thread lock for the in-memory _cluster_state dict — workers can post
# heartbeats concurrently with user requests reading status.
_state_lock = threading.Lock()


def _cluster_secret() -> str | None:
    """Shared secret required for worker-to-scheduler calls.

    When `TUSK_CLUSTER_SECRET` is unset, worker endpoints accept anyone
    (single-node/dev default). Set it in production to lock down registration.
    """
    return os.environ.get("TUSK_CLUSTER_SECRET") or None


def _flight_location(host: str, port: int) -> str:
    """Build Arrow Flight location, honoring TUSK_CLUSTER_TLS env var.

    Set `TUSK_CLUSTER_TLS=1` to dial workers/scheduler over grpc+tls.
    """
    scheme = "grpc+tls" if os.environ.get("TUSK_CLUSTER_TLS", "").lower() in ("1", "true", "yes") else "grpc"
    return f"{scheme}://{host}:{port}"


def _check_user_auth(connection: Request, _: object) -> None:
    """Guard for user-facing cluster endpoints. Mirrors the core admin guard:
    multi-user requires admin session; single-user requires loopback."""
    try:
        from tusk.core.config import get_config
    except ImportError:
        return

    config = get_config()

    if config.auth_mode != "multi":
        client = getattr(connection, "client", None)
        host = getattr(client, "host", None) if client else None
        if host and (host in {"127.0.0.1", "::1", "localhost"} or host.startswith("127.")):
            return
        raise NotAuthorizedException(
            "Cluster admin endpoints require multi-user auth for non-loopback access"
        )

    try:
        from tusk.core.auth import get_session, get_user_by_id
    except ImportError:
        return
    session_id = connection.cookies.get("tusk_session")
    if not session_id:
        raise NotAuthorizedException("Authentication required")
    session = get_session(session_id)
    if not session:
        raise NotAuthorizedException("Invalid or expired session")
    user = get_user_by_id(session.user_id)
    if not user or not user.is_active or not user.is_admin:
        raise NotAuthorizedException("Admin access required")


def _check_worker_auth(connection: Request, _: object) -> None:
    """Guard for worker-facing endpoints (register, heartbeat, unregister).

    Requires `X-Cluster-Secret` header when TUSK_CLUSTER_SECRET is set.
    """
    expected = _cluster_secret()
    if not expected:
        return
    presented = connection.headers.get("x-cluster-secret") or connection.headers.get("X-Cluster-Secret")
    if not presented or not secrets.compare_digest(presented, expected):
        raise NotAuthorizedException("Invalid cluster secret")


def _get_jobs_db():
    """Get connection to cluster plugin's SQLite database"""
    db_path = get_plugin_db_path("tusk-cluster")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _persist_job(job: dict) -> None:
    """Save or update a job in SQLite"""
    conn = _get_jobs_db()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO jobs
            (id, sql, status, progress, stages_total, stages_completed,
             created_at, started_at, completed_at, worker_id, rows_processed, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job.get("id") or job.get("job_id"),
            job.get("sql", ""),
            job.get("status", "pending"),
            job.get("progress", 0),
            job.get("stages_total", 1),
            job.get("stages_completed", 0),
            job.get("created_at"),
            job.get("started_at"),
            job.get("completed_at"),
            job.get("worker_id"),
            job.get("rows_processed", 0),
            job.get("error"),
        ))
        conn.commit()
    finally:
        conn.close()


def _get_persisted_jobs(limit: int = 100) -> list[dict]:
    """Get job history from SQLite"""
    conn = _get_jobs_db()
    try:
        cursor = conn.execute(
            "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)
        )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()

# In-memory cluster state (shared between requests)
_cluster_state = {
    "scheduler": None,
    "workers": {},
    "jobs": {},
}

# Cluster connection config
_cluster_config = {
    "scheduler_host": "localhost",
    "scheduler_port": 8814,
    "connected": False,
}

# Local cluster process (for single-node mode)
_local_cluster = {
    "process": None,
    "running": False,
}


def _user_or_worker_auth(connection: Request, op: object) -> None:
    """Accept either a valid user session (admin) or a valid worker secret.

    Worker endpoints pass through this too, since a presented valid secret
    satisfies the check. If `TUSK_CLUSTER_SECRET` is unset, only user auth is
    enforced (dev default).
    """
    expected = _cluster_secret()
    if expected:
        presented = (connection.headers.get("x-cluster-secret")
                     or connection.headers.get("X-Cluster-Secret"))
        if presented and secrets.compare_digest(presented, expected):
            return
    _check_user_auth(connection, op)


class ClusterAPIController(Controller):
    """API for Cluster management"""

    path = "/api/cluster"
    guards = [_user_or_worker_auth]

    @get("/status")
    async def get_status(self) -> dict:
        """Get overall cluster status"""
        with _state_lock:
            workers = list(_cluster_state["workers"].values())
            jobs = list(_cluster_state["jobs"].values())

        online_workers = sum(1 for w in workers if w.get("status") != "offline")
        active_jobs = sum(1 for j in jobs if j.get("status") == "running")
        completed_jobs = sum(1 for j in jobs if j.get("status") == "completed")

        return {
            "scheduler_online": _cluster_state["scheduler"] is not None,
            "scheduler_address": _cluster_state["scheduler"]["address"] if _cluster_state["scheduler"] else None,
            "workers_online": online_workers,
            "workers_total": len(workers),
            "active_jobs": active_jobs,
            "completed_jobs": completed_jobs,
            "total_bytes_processed": sum(w.get("bytes_processed", 0) for w in workers),
        }

    @get("/workers")
    async def list_workers(self) -> dict:
        """List all workers with metrics"""
        workers = []
        with _state_lock:
            snapshot = list(_cluster_state["workers"].items())
        for worker_id, worker in snapshot:
            workers.append({
                "id": worker_id,
                "address": worker.get("address", "unknown"),
                "port": worker.get("port", 0),
                "status": worker.get("status", "offline"),
                "cpu_percent": worker.get("cpu_percent", 0),
                "memory_mb": worker.get("memory_mb", 0),
                "memory_percent": worker.get("memory_percent", 0),
                "last_heartbeat": worker.get("last_heartbeat"),
                "jobs_completed": worker.get("jobs_completed", 0),
                "bytes_processed": worker.get("bytes_processed", 0),
            })
        return {"workers": workers}

    @post("/workers/register")
    async def register_worker(self, data: dict = Body()) -> dict:
        """Register a worker (called by workers).

        Requires `X-Cluster-Secret` header if TUSK_CLUSTER_SECRET is set;
        also validates the required fields.
        """
        worker_id = data.get("id")
        address = data.get("address")
        port = data.get("port")
        if not worker_id or not isinstance(worker_id, str):
            return {"error": "Worker ID required (string)"}
        if not address or not isinstance(address, str):
            return {"error": "address required"}
        try:
            port = int(port)
        except (TypeError, ValueError):
            return {"error": "port must be an integer"}
        if port < 1 or port > 65535:
            return {"error": "port out of range"}

        with _state_lock:
            _cluster_state["workers"][worker_id] = {
                "id": worker_id,
                "address": address,
                "port": port,
                "status": "idle",
                "cpu_percent": 0,
                "memory_mb": 0,
                "memory_percent": 0,
                "last_heartbeat": datetime.now(timezone.utc).isoformat(),
                "jobs_completed": 0,
                "bytes_processed": 0,
            }

        log.info("Worker registered via API", worker_id=worker_id)
        return {"registered": True, "worker_id": worker_id}

    @post("/workers/{worker_id:str}/heartbeat")
    async def worker_heartbeat(self, worker_id: str, data: dict = Body()) -> dict:
        """Update worker metrics (called by workers)"""
        with _state_lock:
            if worker_id not in _cluster_state["workers"]:
                return {"error": "Worker not found"}
            worker = _cluster_state["workers"][worker_id]
            worker["cpu_percent"] = data.get("cpu", 0)
            worker["memory_mb"] = data.get("memory", 0)
            worker["memory_percent"] = data.get("memory_percent", 0)
            worker["last_heartbeat"] = datetime.now(timezone.utc).isoformat()
            worker["status"] = data.get("status", "idle")

        return {"ok": True}

    @post("/workers/{worker_id:str}/unregister")
    async def unregister_worker(self, worker_id: str) -> dict:
        """Unregister a worker"""
        with _state_lock:
            removed = _cluster_state["workers"].pop(worker_id, None)
        if removed is not None:
            log.info("Worker unregistered via API", worker_id=worker_id)
            return {"unregistered": True}
        return {"error": "Worker not found"}

    @get("/jobs")
    async def list_jobs(self) -> dict:
        """List all jobs from the scheduler"""
        # Try to get from scheduler
        if _cluster_config["connected"] or _cluster_state["scheduler"]:
            try:
                import pyarrow.flight as flight
                import json

                host = _cluster_config["scheduler_host"]
                port = _cluster_config["scheduler_port"]

                location = _flight_location(host, port)
                client = flight.connect(location)

                try:
                    action = flight.Action("list_jobs", b"")
                    results = list(client.do_action(action))

                    if results:
                        data = json.loads(results[0].body.to_pybytes().decode())
                        jobs = data.get("jobs", [])
                        # Persist jobs to SQLite for offline access
                        for j in jobs:
                            _persist_job(j)
                        jobs.sort(key=lambda j: j.get("created_at") or "", reverse=True)
                        return {"jobs": jobs}
                finally:
                    client.close()

            except Exception as e:
                log.warning("Failed to list jobs from scheduler", error=str(e))

        # Fallback: return persisted job history
        try:
            persisted = _get_persisted_jobs()
            if persisted:
                return {"jobs": persisted}
        except Exception:
            pass
        return {"jobs": []}

    @get("/jobs/{job_id:str}")
    async def get_job(self, job_id: str) -> dict:
        """Get job details from scheduler"""
        if _cluster_config["connected"] or _cluster_state["scheduler"]:
            try:
                import pyarrow.flight as flight
                import json

                host = _cluster_config["scheduler_host"]
                port = _cluster_config["scheduler_port"]

                location = _flight_location(host, port)
                client = flight.connect(location)

                try:
                    action = flight.Action("get_job", job_id.encode())
                    results = list(client.do_action(action))

                    if results:
                        return json.loads(results[0].body.to_pybytes().decode())
                finally:
                    client.close()

            except Exception as e:
                log.warning("Failed to get job from scheduler", error=str(e))

        return {"error": "Job not found"}

    @get("/jobs/{job_id:str}/result")
    async def get_job_result(self, request: Request, job_id: str) -> dict | Template:
        """Get result data for a completed job (JSON or HTMX partial)"""
        if not (_cluster_config["connected"] or _cluster_state["scheduler"]):
            error = "Not connected to scheduler"
            if is_htmx(request):
                return Template("partials/cluster/job-result.html", context={"error": error})
            return {"error": error}

        try:
            import pyarrow.flight as flight
            import json

            host = _cluster_config["scheduler_host"]
            port = _cluster_config["scheduler_port"]
            location = _flight_location(host, port)
            client = flight.connect(location)

            try:
                action = flight.Action("get_job_result", job_id.encode())
                results = list(client.do_action(action))
                if results:
                    data = json.loads(results[0].body.to_pybytes().decode())
                    if is_htmx(request):
                        return Template("partials/cluster/job-result.html", context=data)
                    return data
                error = "No result available"
                if is_htmx(request):
                    return Template("partials/cluster/job-result.html", context={"error": error})
                return {"error": error}
            finally:
                client.close()

        except Exception as e:
            log.warning("Failed to get job result", error=str(e))
            error = f"Failed to get result: {str(e)}"
            if is_htmx(request):
                return Template("partials/cluster/job-result.html", context={"error": error})
            return {"error": error}

    @post("/jobs")
    async def submit_job(self, data: dict = Body()) -> dict:
        """Submit a new job to the scheduler"""
        sql = data.get("sql")
        if not sql:
            return {"error": "SQL query required"}

        # Check if connected to scheduler
        if not _cluster_config["connected"] and not _cluster_state["scheduler"]:
            return {"error": "Not connected to scheduler. Start local cluster or connect to remote scheduler."}

        try:
            import pyarrow.flight as flight
            import json

            host = _cluster_config["scheduler_host"]
            port = _cluster_config["scheduler_port"]

            location = _flight_location(host, port)
            client = flight.connect(location)

            try:
                action = flight.Action("submit_job", sql.encode())
                results = list(client.do_action(action))

                if results:
                    result_data = json.loads(results[0].body.to_pybytes().decode())
                    job_id = result_data.get("job_id")

                    if job_id:
                        log.info("Job submitted to scheduler", job_id=job_id)
                        _persist_job({"id": job_id, "sql": sql, "status": "pending", "created_at": datetime.now().isoformat()})
                        return {"job_id": job_id, "status": "pending"}

                return {"error": "Failed to submit job to scheduler"}

            finally:
                client.close()

        except Exception as e:
            log.error("Failed to submit job", error=str(e))
            return {"error": f"Failed to submit job: {str(e)}"}

    @post("/jobs/{job_id:str}/cancel")
    async def cancel_job(self, job_id: str) -> dict:
        """Cancel a job"""
        job = _cluster_state["jobs"].get(job_id)
        if not job:
            return {"error": "Job not found"}

        if job["status"] in ("pending", "running"):
            job["status"] = "cancelled"
            job["completed_at"] = datetime.now().isoformat()
            job["error"] = "Cancelled by user"
            log.info("Job cancelled", job_id=job_id)
            return {"cancelled": True}

        return {"error": "Job cannot be cancelled (already completed)"}

    @post("/jobs/{job_id:str}/retry")
    async def retry_job(self, job_id: str) -> dict:
        """Retry a failed job by resubmitting its SQL"""
        # Get original job
        if _cluster_config["connected"] or _cluster_state["scheduler"]:
            try:
                import pyarrow.flight as flight
                import json

                host = _cluster_config["scheduler_host"]
                port = _cluster_config["scheduler_port"]
                location = _flight_location(host, port)
                client = flight.connect(location)

                try:
                    action = flight.Action("get_job", job_id.encode())
                    results = list(client.do_action(action))
                    if results:
                        original = json.loads(results[0].body.to_pybytes().decode())
                        sql = original.get("sql")
                        if sql:
                            # Submit as new job
                            submit_action = flight.Action("submit_job", sql.encode())
                            submit_results = list(client.do_action(submit_action))
                            if submit_results:
                                new_data = json.loads(submit_results[0].body.to_pybytes().decode())
                                new_id = new_data.get("job_id")
                                if new_id:
                                    _persist_job({"id": new_id, "sql": sql, "status": "pending", "created_at": datetime.now().isoformat()})
                                    return {"job_id": new_id, "retried_from": job_id, "status": "pending"}
                finally:
                    client.close()
            except Exception as e:
                log.error("Failed to retry job", error=str(e))
                return {"error": f"Retry failed: {str(e)}"}

        # Fallback: try from persisted data
        try:
            conn = _get_jobs_db()
            try:
                cursor = conn.execute("SELECT sql FROM jobs WHERE id = ?", (job_id,))
                row = cursor.fetchone()
                if row and row["sql"]:
                    return {"error": "Cannot retry: scheduler not connected. Original SQL preserved."}
            finally:
                conn.close()
        except Exception:
            pass

        return {"error": "Job not found or no SQL to retry"}

    @post("/scheduler/register")
    async def register_scheduler(self, data: dict = Body()) -> dict:
        """Register scheduler (called by scheduler process)"""
        _cluster_state["scheduler"] = {
            "address": data.get("address", "localhost"),
            "port": data.get("port", 8814),
            "started_at": datetime.now().isoformat(),
        }
        log.info("Scheduler registered via API")
        return {"registered": True}

    @post("/scheduler/unregister")
    async def unregister_scheduler(self) -> dict:
        """Unregister scheduler"""
        _cluster_state["scheduler"] = None
        log.info("Scheduler unregistered via API")
        return {"unregistered": True}

    @get("/config")
    async def get_config(self) -> dict:
        """Get cluster connection config"""
        return {
            "scheduler_host": _cluster_config["scheduler_host"],
            "scheduler_port": _cluster_config["scheduler_port"],
            "connected": _cluster_config["connected"],
        }

    @post("/connect")
    async def connect_scheduler(self, data: dict = Body()) -> dict:
        """Connect to a remote scheduler"""
        host = data.get("host", "localhost")
        port = int(data.get("port", 8814))

        _cluster_config["scheduler_host"] = host
        _cluster_config["scheduler_port"] = port

        try:
            import pyarrow.flight as flight

            location = _flight_location(host, port)
            client = flight.connect(location)

            try:
                list(client.do_action(flight.Action("list_workers", b"")))
                _cluster_config["connected"] = True

                _cluster_state["scheduler"] = {
                    "address": f"{host}:{port}",
                    "port": port,
                    "started_at": datetime.now().isoformat(),
                }

                log.info("Connected to scheduler", host=host, port=port)
                return {"connected": True, "address": f"{host}:{port}"}
            except Exception as e:
                _cluster_config["connected"] = False
                log.warning("Scheduler not responding", host=host, port=port, error=str(e))
                return {"connected": False, "error": f"Scheduler at {host}:{port} not responding"}
            finally:
                client.close()

        except Exception as e:
            _cluster_config["connected"] = False
            log.error("Failed to connect to scheduler", host=host, port=port, error=str(e))
            return {"connected": False, "error": str(e)}

    @post("/disconnect")
    async def disconnect_scheduler(self) -> dict:
        """Disconnect from scheduler"""
        _cluster_config["connected"] = False
        _cluster_state["scheduler"] = None
        _cluster_state["workers"] = {}
        log.info("Disconnected from scheduler")
        return {"disconnected": True}

    @get("/catalog")
    async def get_catalog(self) -> dict:
        """Get table catalog from Tusk Data (datasets enabled for cluster)"""
        try:
            from tusk.core.workspace import get_cluster_catalog
            tables = get_cluster_catalog()
            return {"tables": tables}
        except Exception as e:
            log.error("Failed to load catalog", error=str(e))
            return {"tables": [], "error": str(e)}

    @post("/refresh-workers")
    async def refresh_workers_from_scheduler(self) -> dict:
        """Fetch workers from connected scheduler"""
        if not _cluster_config["connected"]:
            return {"error": "Not connected to scheduler"}

        host = _cluster_config["scheduler_host"]
        port = _cluster_config["scheduler_port"]

        try:
            import pyarrow.flight as flight
            import json

            location = _flight_location(host, port)
            client = flight.connect(location)

            try:
                action = flight.Action("list_workers", b"")
                results = list(client.do_action(action))

                if results:
                    workers_data = json.loads(results[0].body.to_pybytes().decode())

                    _cluster_state["workers"] = {}
                    for w in workers_data.get("workers", []):
                        _cluster_state["workers"][w["id"]] = w

                    return {"refreshed": True, "workers": len(_cluster_state["workers"])}

                return {"refreshed": True, "workers": 0}

            finally:
                client.close()

        except Exception as e:
            log.error("Failed to refresh workers", error=str(e))
            return {"error": str(e)}

    @get("/local/status")
    async def get_local_status(self) -> dict:
        """Get local cluster status"""
        return {
            "running": _local_cluster["running"],
            "pid": _local_cluster["process"].pid if _local_cluster["process"] else None,
        }

    @post("/local/start")
    async def start_local_cluster(self, data: dict = Body()) -> dict:
        """Start a local single-node cluster"""
        if _local_cluster["running"]:
            return {"error": "Local cluster already running"}

        num_workers = data.get("workers", 1)
        tusk_port = data.get("tusk_port", 8080)

        try:
            import sys
            from pathlib import Path

            # Log subprocess output to file for debugging
            log_dir = Path.home() / ".tusk" / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / "cluster-dev.log"
            log_fh = open(log_file, "w")

            process = subprocess.Popen(
                [
                    sys.executable, "-m", "tusk.cli", "cluster-dev",
                    "--workers", str(num_workers),
                    "--tusk-url", f"http://localhost:{tusk_port}",
                ],
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

            _local_cluster["process"] = process
            _local_cluster["log_file"] = log_fh
            _local_cluster["running"] = True

            _cluster_config["scheduler_host"] = "localhost"
            _cluster_config["scheduler_port"] = 8814

            # Wait for scheduler to be ready with retry
            connected = False
            for attempt in range(12):
                await asyncio.sleep(1.5)

                # Check if process died
                retcode = process.poll()
                if retcode is not None:
                    log_fh.close()
                    # Read log for error details
                    try:
                        err_output = log_file.read_text()[-500:]
                    except Exception:
                        err_output = ""
                    _local_cluster["running"] = False
                    _local_cluster["process"] = None
                    log.error("Cluster process died", return_code=retcode, output=err_output)
                    return {"error": f"Cluster process exited with code {retcode}. Check ~/.tusk/logs/cluster-dev.log"}

                try:
                    import pyarrow.flight as flight
                    client = flight.connect(_flight_location("localhost", 8814))
                    list(client.do_action(flight.Action("list_workers", b"")))
                    client.close()
                    connected = True
                    break
                except Exception:
                    log.debug("Waiting for scheduler...", attempt=attempt + 1)

            if connected:
                _cluster_state["scheduler"] = {
                    "address": "localhost:8814",
                    "port": 8814,
                    "started_at": datetime.now().isoformat(),
                }
                _cluster_config["connected"] = True
                log.info("Local cluster started", pid=process.pid, workers=num_workers)
                return {"started": True, "pid": process.pid, "workers": num_workers}
            else:
                log.warning("Scheduler did not respond after 12 attempts")
                _cluster_state["scheduler"] = {
                    "address": "localhost:8814",
                    "port": 8814,
                    "started_at": datetime.now().isoformat(),
                }
                _cluster_config["connected"] = False
                return {"started": True, "pid": process.pid, "workers": num_workers, "warning": "Scheduler may still be starting"}

        except Exception as e:
            log.error("Failed to start local cluster", error=str(e))
            return {"error": str(e)}

    @post("/local/stop")
    async def stop_local_cluster(self) -> dict:
        """Stop the local cluster"""
        if not _local_cluster["running"] or not _local_cluster["process"]:
            return {"error": "No local cluster running"}

        try:
            import os
            import signal

            process = _local_cluster["process"]
            pid = process.pid

            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
            except ProcessLookupError:
                pass

            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(pid), signal.SIGKILL)

            _local_cluster["process"] = None
            _local_cluster["running"] = False
            if _local_cluster.get("log_file"):
                try:
                    _local_cluster["log_file"].close()
                except Exception:
                    pass
                _local_cluster["log_file"] = None

            _cluster_state["scheduler"] = None
            _cluster_state["workers"] = {}
            _cluster_config["connected"] = False

            log.info("Local cluster stopped", pid=pid)
            return {"stopped": True}

        except Exception as e:
            log.error("Failed to stop local cluster", error=str(e))
            return {"error": str(e)}
