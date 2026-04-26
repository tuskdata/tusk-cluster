"""Scheduler for distributed query execution"""

import os
import threading
import time
from datetime import datetime, timedelta
from uuid import uuid4

import pyarrow as pa
import pyarrow.flight as flight
import msgspec

from tusk.core.logging import get_logger
from tusk_cluster.models import Job, WorkerInfo, ClusterStatus

log = get_logger("scheduler")

_CLUSTER_TOKEN = os.environ.get("TUSK_CLUSTER_TOKEN", "")


def _validate_token(data: str) -> tuple[bool, str]:
    """Validate cluster token prefix and return (valid, stripped_data).

    Protocol: if TUSK_CLUSTER_TOKEN is set, data must start with
    "TOKEN:<secret>:" prefix. Returns the data with the prefix stripped.
    If no token is configured, all data passes through unchanged.
    """
    if not _CLUSTER_TOKEN:
        return True, data

    prefix = f"TOKEN:{_CLUSTER_TOKEN}:"
    if data.startswith(prefix):
        return True, data[len(prefix):]
    return False, data


class FlightSchedulerServer(flight.FlightServerBase):
    """Arrow Flight server for scheduler"""

    def __init__(self, scheduler: "Scheduler", location: str):
        super().__init__(location)
        self.scheduler = scheduler

    def do_action(self, context, action):
        """Handle worker registration and heartbeats"""
        action_type = action.type
        raw_data = action.body.to_pybytes().decode()

        # Validate token for worker actions
        if action_type in ("register", "heartbeat", "unregister"):
            valid, raw_data = _validate_token(raw_data)
            if not valid:
                log.warning("Invalid cluster token", action=action_type)
                yield flight.Result(b"auth_error")
                return

        if action_type == "register":
            data = raw_data
            parts = data.split(":")
            worker_id = parts[0]
            host = parts[1] if len(parts) > 1 else "localhost"
            port = int(parts[2]) if len(parts) > 2 else 8815

            self.scheduler.register_worker(worker_id, host, port)
            log.info("Worker registered", worker_id=worker_id, host=host, port=port)
            yield flight.Result(b"registered")

        elif action_type == "heartbeat":
            parts = raw_data.split(":")
            worker_id = parts[0]
            cpu = float(parts[1]) if len(parts) > 1 else 0.0
            memory = float(parts[2]) if len(parts) > 2 else 0.0
            memory_percent = float(parts[3]) if len(parts) > 3 else 0.0

            self.scheduler.update_worker_status(worker_id, cpu, memory, memory_percent)
            yield flight.Result(b"ok")

        elif action_type == "unregister":
            worker_id = raw_data
            self.scheduler.unregister_worker(worker_id)
            log.info("Worker unregistered", worker_id=worker_id)
            yield flight.Result(b"unregistered")

        elif action_type == "list_workers":
            import json
            workers_data = {
                "workers": [
                    {
                        "id": w.id,
                        "address": w.address,
                        "port": w.port,
                        "status": w.status,
                        "cpu_percent": w.cpu_percent,
                        "memory_mb": w.memory_mb,
                        "memory_percent": w.memory_percent,
                        "last_heartbeat": w.last_heartbeat.isoformat() if w.last_heartbeat else None,
                        "jobs_completed": w.jobs_completed,
                        "bytes_processed": w.bytes_processed,
                        "active_jobs": w.active_jobs,
                    }
                    for w in self.scheduler.workers.values()
                ]
            }
            yield flight.Result(json.dumps(workers_data).encode())

        elif action_type == "submit_job":
            import json
            sql = action.body.to_pybytes().decode()
            job_id = self.scheduler.submit_job(sql)
            yield flight.Result(json.dumps({"job_id": job_id}).encode())

        elif action_type == "list_jobs":
            import json
            jobs_data = {
                "jobs": [
                    {
                        "id": j.id,
                        "sql": j.sql[:100] if j.sql else "",
                        "status": j.status,
                        "progress": j.progress,
                        "created_at": j.created_at.isoformat() if j.created_at else None,
                        "started_at": j.started_at.isoformat() if j.started_at else None,
                        "completed_at": j.completed_at.isoformat() if j.completed_at else None,
                        "worker_id": j.worker_id,
                        "rows_processed": j.rows_processed,
                        "error": j.error,
                        "retry_count": j.retry_count,
                        "max_retries": j.max_retries,
                        "next_retry_at": j.next_retry_at.isoformat() if j.next_retry_at else None,
                    }
                    for j in self.scheduler.jobs.values()
                ]
            }
            yield flight.Result(json.dumps(jobs_data).encode())

        elif action_type == "get_job":
            import json
            job_id = action.body.to_pybytes().decode()
            job = self.scheduler.get_job(job_id)
            if job:
                job_data = {
                    "id": job.id,
                    "sql": job.sql,
                    "status": job.status,
                    "progress": job.progress,
                    "created_at": job.created_at.isoformat() if job.created_at else None,
                    "started_at": job.started_at.isoformat() if job.started_at else None,
                    "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                    "worker_id": job.worker_id,
                    "rows_processed": job.rows_processed,
                    "error": job.error,
                    "retry_count": job.retry_count,
                    "max_retries": job.max_retries,
                    "next_retry_at": job.next_retry_at.isoformat() if job.next_retry_at else None,
                }
                yield flight.Result(json.dumps(job_data).encode())
            else:
                yield flight.Result(json.dumps({"error": "Job not found"}).encode())

        elif action_type == "get_job_result":
            import json
            job_id = action.body.to_pybytes().decode()
            job = self.scheduler.get_job(job_id)
            if job and job.result is not None:
                # Convert Arrow Table to JSON-serializable format
                table = job.result
                columns = [{"name": field.name, "type": str(field.type)} for field in table.schema]
                rows = table.to_pydict()
                # Convert dict-of-lists to list-of-lists
                row_count = table.num_rows
                row_list = []
                col_names = [c["name"] for c in columns]
                for i in range(min(row_count, 1000)):  # Limit to 1000 rows
                    row_list.append([rows[col][i] for col in col_names])
                result = {
                    "columns": columns,
                    "rows": row_list,
                    "row_count": row_count,
                    "truncated": row_count > 1000,
                }
                yield flight.Result(json.dumps(result, default=str).encode())
            else:
                yield flight.Result(json.dumps({"error": "No result available"}).encode())

        else:
            log.warning("Unknown action", action_type=action_type)
            yield flight.Result(b"unknown_action")

    def list_flights(self, context, criteria):
        """List available jobs"""
        for job_id, job in self.scheduler.jobs.items():
            descriptor = flight.FlightDescriptor.for_path(job_id)
            info_data = msgspec.json.encode({
                "id": job.id,
                "sql": job.sql,
                "status": job.status,
                "progress": job.progress,
            })
            yield flight.FlightInfo(
                schema=pa.schema([]),
                descriptor=descriptor,
                endpoints=[],
                total_records=-1,
                total_bytes=-1,
            )


class Scheduler:
    """Distributed query scheduler"""

    def __init__(self, host: str = "0.0.0.0", port: int = 8814):
        self.host = host
        self.port = port
        self.workers: dict[str, WorkerInfo] = {}
        self.jobs: dict[str, Job] = {}
        self.start_time = time.time()
        self._server: FlightSchedulerServer | None = None
        self._running = False

    def get_status(self) -> ClusterStatus:
        """Get overall cluster status"""
        online_workers = sum(1 for w in self.workers.values() if w.status != "offline")
        active_jobs = sum(1 for j in self.jobs.values() if j.status == "running")
        completed_jobs = sum(1 for j in self.jobs.values() if j.status == "completed")
        total_bytes = sum(w.bytes_processed for w in self.workers.values())

        return ClusterStatus(
            scheduler_address=self.host,
            scheduler_port=self.port,
            workers_online=online_workers,
            workers_total=len(self.workers),
            active_jobs=active_jobs,
            completed_jobs=completed_jobs,
            total_bytes_processed=total_bytes,
            uptime_seconds=time.time() - self.start_time,
        )

    def register_worker(self, worker_id: str, address: str, port: int) -> None:
        """Register a new worker"""
        self.workers[worker_id] = WorkerInfo(
            id=worker_id,
            address=address,
            port=port,
            status="idle",
        )
        log.info("Worker registered", worker_id=worker_id, address=address, port=port)

    def unregister_worker(self, worker_id: str) -> None:
        """Unregister a worker"""
        if worker_id in self.workers:
            del self.workers[worker_id]
            log.info("Worker unregistered", worker_id=worker_id)

    def update_worker_status(
        self, worker_id: str, cpu: float, memory: float, memory_percent: float = 0.0
    ) -> None:
        """Update worker metrics from heartbeat"""
        if worker_id in self.workers:
            worker = self.workers[worker_id]
            self.workers[worker_id] = WorkerInfo(
                id=worker.id,
                address=worker.address,
                port=worker.port,
                status=worker.status,
                cpu_percent=cpu,
                memory_mb=memory,
                memory_percent=memory_percent,
                last_heartbeat=datetime.now(),
                jobs_completed=worker.jobs_completed,
                bytes_processed=worker.bytes_processed,
            )

    def submit_job(self, sql: str) -> str:
        """Submit a new job and return job ID"""
        job_id = str(uuid4())[:8]
        job = Job(id=job_id, sql=sql)
        self.jobs[job_id] = job
        log.info("Job submitted", job_id=job_id, sql=sql[:100])

        t = threading.Thread(target=self._process_job, args=(job_id,), daemon=True)
        t.start()
        return job_id

    def get_job(self, job_id: str) -> Job | None:
        """Get job by ID"""
        return self.jobs.get(job_id)

    def list_jobs(self) -> list[Job]:
        """List all jobs"""
        return list(self.jobs.values())

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job"""
        job = self.jobs.get(job_id)
        if job and job.status in ("pending", "running"):
            self.jobs[job_id] = Job(
                id=job.id,
                sql=job.sql,
                status="cancelled",
                created_at=job.created_at,
                started_at=job.started_at,
                completed_at=datetime.now(),
                progress=job.progress,
                stages_total=job.stages_total,
                stages_completed=job.stages_completed,
                error="Cancelled by user",
            )
            log.info("Job cancelled", job_id=job_id)
            return True
        return False

    def _get_available_worker(self) -> WorkerInfo | None:
        """Get the best available worker using composite scoring.

        Score = active_jobs * 10 + cpu_percent * 0.5 + memory_percent * 0.5
        Lower score = better candidate.
        Workers with >90% CPU or memory are excluded.
        """
        alive = [
            w for w in self.workers.values()
            if w.status != "offline"
            and (datetime.now() - w.last_heartbeat).total_seconds() < 30
            and w.cpu_percent <= 90
            and w.memory_percent <= 90
        ]
        if not alive:
            return None
        return min(alive, key=lambda w: w.active_jobs * 10 + w.cpu_percent * 0.5 + w.memory_percent * 0.5)

    def _increment_worker_jobs(self, worker_id: str) -> None:
        """Increment active_jobs counter for a worker"""
        if worker_id in self.workers:
            w = self.workers[worker_id]
            self.workers[worker_id] = WorkerInfo(
                id=w.id, address=w.address, port=w.port, status="busy",
                cpu_percent=w.cpu_percent, memory_mb=w.memory_mb,
                memory_percent=w.memory_percent, last_heartbeat=w.last_heartbeat,
                jobs_completed=w.jobs_completed, bytes_processed=w.bytes_processed,
                active_jobs=w.active_jobs + 1,
            )

    def _decrement_worker_jobs(self, worker_id: str, result_bytes: int = 0, completed: bool = False) -> None:
        """Decrement active_jobs counter for a worker"""
        if worker_id in self.workers:
            w = self.workers[worker_id]
            new_active = max(0, w.active_jobs - 1)
            self.workers[worker_id] = WorkerInfo(
                id=w.id, address=w.address, port=w.port,
                status="idle" if new_active == 0 else "busy",
                cpu_percent=w.cpu_percent, memory_mb=w.memory_mb,
                memory_percent=w.memory_percent, last_heartbeat=w.last_heartbeat,
                jobs_completed=w.jobs_completed + (1 if completed else 0),
                bytes_processed=w.bytes_processed + result_bytes,
                active_jobs=new_active,
            )

    def _requeue_job(self, job_id: str) -> None:
        """Re-queue a failed job with exponential backoff"""
        job = self.jobs.get(job_id)
        if not job or job.retry_count >= job.max_retries:
            return

        delay = 2 ** job.retry_count * 2  # 2s, 4s, 8s
        next_retry = datetime.now() + timedelta(seconds=delay)

        self.jobs[job_id] = Job(
            id=job.id, sql=job.sql, status="pending",
            created_at=job.created_at,
            retry_count=job.retry_count + 1,
            max_retries=job.max_retries,
            next_retry_at=next_retry,
            error=job.error,
        )
        log.info("Job re-queued for retry", job_id=job_id,
                 retry=job.retry_count + 1, max=job.max_retries, delay_s=delay)

        # Schedule re-execution after delay
        def _delayed_run():
            time.sleep(delay)
            if self.jobs.get(job_id) and self.jobs[job_id].status == "pending":
                t = threading.Thread(target=self._process_job, args=(job_id,), daemon=True)
                t.start()
        threading.Thread(target=_delayed_run, daemon=True).start()

    def _process_job(self, job_id: str) -> None:
        """Process a job by sending to a worker"""
        job = self.jobs.get(job_id)
        if not job:
            return

        self.jobs[job_id] = Job(
            id=job.id, sql=job.sql, status="running",
            created_at=job.created_at, started_at=datetime.now(),
            stages_total=1, retry_count=job.retry_count,
            max_retries=job.max_retries,
        )

        worker = None
        try:
            # Wait briefly for a worker to become available
            for _ in range(10):
                worker = self._get_available_worker()
                if worker:
                    break
                time.sleep(0.5)

            if not worker:
                raise Exception("No workers available")

            self._increment_worker_jobs(worker.id)

            self.jobs[job_id] = Job(
                id=job_id, sql=job.sql, status="running",
                created_at=job.created_at, started_at=datetime.now(),
                stages_total=1, worker_id=worker.id,
                retry_count=job.retry_count, max_retries=job.max_retries,
            )

            log.info("Executing job on worker", job_id=job_id, worker_id=worker.id)

            result = self._execute_on_worker(worker, job.sql)

            self.jobs[job_id] = Job(
                id=job_id, sql=job.sql, status="completed",
                created_at=job.created_at,
                started_at=self.jobs[job_id].started_at,
                completed_at=datetime.now(),
                progress=1.0, stages_total=1, stages_completed=1,
                rows_processed=result.num_rows if result else 0,
                worker_id=worker.id, result=result,
                retry_count=job.retry_count, max_retries=job.max_retries,
            )

            self._decrement_worker_jobs(worker.id, result_bytes=(result.nbytes if result else 0), completed=True)
            log.info("Job completed", job_id=job_id, rows=result.num_rows if result else 0)

        except Exception as e:
            log.error("Job failed", job_id=job_id, error=str(e))
            current = self.jobs.get(job_id)
            retry_count = current.retry_count if current else 0
            max_retries = current.max_retries if current else 3

            self.jobs[job_id] = Job(
                id=job_id, sql=job.sql, status="failed",
                created_at=job.created_at,
                started_at=self.jobs[job_id].started_at if job_id in self.jobs else None,
                completed_at=datetime.now(), error=str(e),
                retry_count=retry_count, max_retries=max_retries,
            )

            if worker:
                self._decrement_worker_jobs(worker.id)

            # Auto-retry if under limit
            if retry_count < max_retries:
                self._requeue_job(job_id)

    def _execute_on_worker(self, worker: WorkerInfo, sql: str) -> pa.Table | None:
        """Send query to worker and get results.

        We attach a Flight call timeout so a wedged worker can't block the
        scheduler thread indefinitely. The monitor loop will mark the
        worker offline after 30s of missed heartbeats and re-queue the job
        on a healthy worker; without this timeout the original do_get()
        would still be hanging when the retry runs.
        """
        try:
            location = flight.Location.for_grpc_tcp(worker.address, worker.port)
            client = flight.FlightClient(location)

            ticket = flight.Ticket(sql.encode())
            timeout_s = float(os.environ.get("TUSK_CLUSTER_CALL_TIMEOUT", "300"))
            options = flight.FlightCallOptions(timeout=timeout_s)
            reader = client.do_get(ticket, options=options)

            table = reader.read_all()
            return table

        except Exception as e:
            log.error("Worker execution failed", worker_id=worker.id, error=str(e))
            raise

    def _monitor_workers(self) -> None:
        """Monitor worker heartbeats and re-queue jobs from dead workers."""
        while self._running:
            time.sleep(10)
            if not self._running:
                break
            now = datetime.now()
            for worker_id, w in list(self.workers.items()):
                if (now - w.last_heartbeat).total_seconds() > 30 and w.status != "offline":
                    log.warning("Worker stale, marking offline", worker_id=worker_id,
                                last_heartbeat=w.last_heartbeat.isoformat())
                    self.workers[worker_id] = WorkerInfo(
                        id=w.id, address=w.address, port=w.port, status="offline",
                        cpu_percent=w.cpu_percent, memory_mb=w.memory_mb,
                        memory_percent=w.memory_percent, last_heartbeat=w.last_heartbeat,
                        jobs_completed=w.jobs_completed, bytes_processed=w.bytes_processed,
                        active_jobs=0,
                    )
                    # Re-queue running jobs assigned to this dead worker
                    for job_id, job in list(self.jobs.items()):
                        if job.worker_id == worker_id and job.status == "running":
                            log.info("Re-queuing job from dead worker", job_id=job_id, worker_id=worker_id)
                            self._requeue_job(job_id)

    def serve(self) -> None:
        """Start the scheduler server (blocking)"""
        location = f"grpc://{self.host}:{self.port}"
        self._server = FlightSchedulerServer(self, location)
        self._running = True

        # Start worker monitor thread
        self._monitor_thread = threading.Thread(target=self._monitor_workers, daemon=True)
        self._monitor_thread.start()

        log.info("Scheduler starting", host=self.host, port=self.port)
        print(f"Scheduler listening on {self.host}:{self.port}")

        self._server.serve()

    def shutdown(self) -> None:
        """Shutdown the scheduler"""
        self._running = False
        if self._server:
            self._server.shutdown()
            log.info("Scheduler shutdown")
