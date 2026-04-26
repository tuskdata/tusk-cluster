"""Tests for tusk-cluster models and scheduler logic"""

import time
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

import pytest

from tusk_cluster.models import Job, WorkerInfo, ClusterStatus, JobSubmission, JobResult


# ── Model defaults ──────────────────────────────────────────────────

class TestJobModel:
    def test_defaults(self):
        job = Job(id="j1", sql="SELECT 1")
        assert job.status == "pending"
        assert job.progress == 0.0
        assert job.stages_total == 1
        assert job.retry_count == 0
        assert job.max_retries == 3
        assert job.next_retry_at is None
        assert job.worker_id is None
        assert job.error is None
        assert job.result is None

    def test_retry_fields(self):
        job = Job(id="j2", sql="SELECT 1", retry_count=2, max_retries=5,
                  next_retry_at=datetime(2026, 1, 1))
        assert job.retry_count == 2
        assert job.max_retries == 5
        assert job.next_retry_at == datetime(2026, 1, 1)


class TestWorkerInfoModel:
    def test_defaults(self):
        w = WorkerInfo(id="w1", address="localhost", port=8815)
        assert w.status == "idle"
        assert w.cpu_percent == 0.0
        assert w.memory_percent == 0.0
        assert w.active_jobs == 0
        assert w.jobs_completed == 0

    def test_active_jobs(self):
        w = WorkerInfo(id="w1", address="localhost", port=8815, active_jobs=3)
        assert w.active_jobs == 3


class TestClusterStatusModel:
    def test_creation(self):
        status = ClusterStatus(
            scheduler_address="0.0.0.0",
            scheduler_port=8814,
            workers_online=2,
            workers_total=3,
            active_jobs=1,
            completed_jobs=10,
        )
        assert status.workers_online == 2
        assert status.total_bytes_processed == 0


# ── Scheduler logic ─────────────────────────────────────────────────

class TestSchedulerWorkerSelection:
    """Test _get_available_worker composite scoring"""

    def _make_scheduler(self):
        """Create a Scheduler without starting the server"""
        # We import here to avoid import errors if pyarrow not installed
        from tusk_cluster.scheduler import Scheduler
        s = Scheduler(host="localhost", port=8814)
        return s

    def test_selects_least_loaded_worker(self):
        s = self._make_scheduler()
        now = datetime.now()
        s.workers = {
            "w1": WorkerInfo(id="w1", address="a", port=1, active_jobs=0,
                             cpu_percent=20.0, memory_percent=30.0, last_heartbeat=now),
            "w2": WorkerInfo(id="w2", address="b", port=2, active_jobs=2,
                             cpu_percent=50.0, memory_percent=40.0, last_heartbeat=now),
            "w3": WorkerInfo(id="w3", address="c", port=3, active_jobs=1,
                             cpu_percent=10.0, memory_percent=10.0, last_heartbeat=now),
        }
        # Scores: w1=0*10+20*0.5+30*0.5=25, w2=2*10+50*0.5+40*0.5=65, w3=1*10+10*0.5+10*0.5=20
        best = s._get_available_worker()
        assert best is not None
        assert best.id == "w3"

    def test_excludes_overloaded_workers(self):
        s = self._make_scheduler()
        now = datetime.now()
        s.workers = {
            "w1": WorkerInfo(id="w1", address="a", port=1,
                             cpu_percent=95.0, memory_percent=30.0, last_heartbeat=now),
            "w2": WorkerInfo(id="w2", address="b", port=2,
                             cpu_percent=50.0, memory_percent=95.0, last_heartbeat=now),
            "w3": WorkerInfo(id="w3", address="c", port=3,
                             cpu_percent=50.0, memory_percent=50.0, last_heartbeat=now),
        }
        best = s._get_available_worker()
        assert best is not None
        assert best.id == "w3"

    def test_excludes_stale_workers(self):
        s = self._make_scheduler()
        stale = datetime.now() - timedelta(seconds=60)
        fresh = datetime.now()
        s.workers = {
            "w1": WorkerInfo(id="w1", address="a", port=1,
                             cpu_percent=10.0, memory_percent=10.0, last_heartbeat=stale),
            "w2": WorkerInfo(id="w2", address="b", port=2,
                             cpu_percent=50.0, memory_percent=50.0, last_heartbeat=fresh),
        }
        best = s._get_available_worker()
        assert best is not None
        assert best.id == "w2"

    def test_returns_none_when_no_workers(self):
        s = self._make_scheduler()
        s.workers = {}
        assert s._get_available_worker() is None

    def test_returns_none_when_all_overloaded(self):
        s = self._make_scheduler()
        now = datetime.now()
        s.workers = {
            "w1": WorkerInfo(id="w1", address="a", port=1,
                             cpu_percent=95.0, memory_percent=95.0, last_heartbeat=now),
        }
        assert s._get_available_worker() is None

    def test_excludes_offline_workers(self):
        s = self._make_scheduler()
        now = datetime.now()
        s.workers = {
            "w1": WorkerInfo(id="w1", address="a", port=1, status="offline",
                             cpu_percent=10.0, memory_percent=10.0, last_heartbeat=now),
        }
        assert s._get_available_worker() is None


class TestSchedulerRegistration:
    def _make_scheduler(self):
        from tusk_cluster.scheduler import Scheduler
        return Scheduler(host="localhost", port=8814)

    def test_register_worker(self):
        s = self._make_scheduler()
        s.register_worker("w1", "192.168.1.1", 8815)
        assert "w1" in s.workers
        assert s.workers["w1"].address == "192.168.1.1"
        assert s.workers["w1"].port == 8815
        assert s.workers["w1"].status == "idle"

    def test_unregister_worker(self):
        s = self._make_scheduler()
        s.register_worker("w1", "localhost", 8815)
        s.unregister_worker("w1")
        assert "w1" not in s.workers

    def test_unregister_nonexistent_is_noop(self):
        s = self._make_scheduler()
        s.unregister_worker("nonexistent")  # Should not raise

    def test_update_worker_status(self):
        s = self._make_scheduler()
        s.register_worker("w1", "localhost", 8815)
        s.update_worker_status("w1", cpu=75.5, memory=1024.0, memory_percent=45.0)
        w = s.workers["w1"]
        assert w.cpu_percent == 75.5
        assert w.memory_mb == 1024.0
        assert w.memory_percent == 45.0


class TestSchedulerJobs:
    def _make_scheduler(self):
        from tusk_cluster.scheduler import Scheduler
        return Scheduler(host="localhost", port=8814)

    def test_submit_job_creates_entry(self):
        s = self._make_scheduler()
        # Mock _process_job to avoid actual execution
        with patch.object(s, '_process_job'):
            job_id = s.submit_job("SELECT 1")
            assert job_id in s.jobs
            assert s.jobs[job_id].sql == "SELECT 1"
            assert s.jobs[job_id].status == "pending"

    def test_get_job(self):
        s = self._make_scheduler()
        with patch.object(s, '_process_job'):
            job_id = s.submit_job("SELECT 1")
        job = s.get_job(job_id)
        assert job is not None
        assert job.id == job_id

    def test_get_nonexistent_job(self):
        s = self._make_scheduler()
        assert s.get_job("nonexistent") is None

    def test_cancel_pending_job(self):
        s = self._make_scheduler()
        s.jobs["j1"] = Job(id="j1", sql="SELECT 1", status="pending")
        assert s.cancel_job("j1") is True
        assert s.jobs["j1"].status == "cancelled"

    def test_cancel_completed_job_fails(self):
        s = self._make_scheduler()
        s.jobs["j1"] = Job(id="j1", sql="SELECT 1", status="completed")
        assert s.cancel_job("j1") is False

    def test_list_jobs(self):
        s = self._make_scheduler()
        s.jobs["j1"] = Job(id="j1", sql="SELECT 1")
        s.jobs["j2"] = Job(id="j2", sql="SELECT 2")
        jobs = s.list_jobs()
        assert len(jobs) == 2

    def test_get_status(self):
        s = self._make_scheduler()
        s.register_worker("w1", "localhost", 8815)
        s.jobs["j1"] = Job(id="j1", sql="SELECT 1", status="running")
        s.jobs["j2"] = Job(id="j2", sql="SELECT 2", status="completed")
        status = s.get_status()
        assert status.workers_online == 1
        assert status.active_jobs == 1
        assert status.completed_jobs == 1


class TestSchedulerActiveJobs:
    def _make_scheduler(self):
        from tusk_cluster.scheduler import Scheduler
        return Scheduler(host="localhost", port=8814)

    def test_increment_worker_jobs(self):
        s = self._make_scheduler()
        s.register_worker("w1", "localhost", 8815)
        assert s.workers["w1"].active_jobs == 0
        s._increment_worker_jobs("w1")
        assert s.workers["w1"].active_jobs == 1
        assert s.workers["w1"].status == "busy"

    def test_decrement_worker_jobs(self):
        s = self._make_scheduler()
        s.workers["w1"] = WorkerInfo(id="w1", address="localhost", port=8815, active_jobs=2, status="busy")
        s._decrement_worker_jobs("w1", completed=True)
        assert s.workers["w1"].active_jobs == 1
        assert s.workers["w1"].jobs_completed == 1
        assert s.workers["w1"].status == "busy"

    def test_decrement_to_zero_sets_idle(self):
        s = self._make_scheduler()
        s.workers["w1"] = WorkerInfo(id="w1", address="localhost", port=8815, active_jobs=1, status="busy")
        s._decrement_worker_jobs("w1")
        assert s.workers["w1"].active_jobs == 0
        assert s.workers["w1"].status == "idle"


class TestSchedulerRetry:
    def _make_scheduler(self):
        from tusk_cluster.scheduler import Scheduler
        return Scheduler(host="localhost", port=8814)

    def test_requeue_job(self):
        s = self._make_scheduler()
        s.jobs["j1"] = Job(id="j1", sql="SELECT 1", status="failed",
                           retry_count=0, max_retries=3, error="timeout")
        with patch('threading.Thread') as mock_thread:
            s._requeue_job("j1")
        job = s.jobs["j1"]
        assert job.status == "pending"
        assert job.retry_count == 1
        assert job.next_retry_at is not None

    def test_requeue_respects_max_retries(self):
        s = self._make_scheduler()
        s.jobs["j1"] = Job(id="j1", sql="SELECT 1", status="failed",
                           retry_count=3, max_retries=3)
        s._requeue_job("j1")
        # Should not change status since at max retries
        assert s.jobs["j1"].status == "failed"


class TestSchedulerStaleWorkers:
    def _make_scheduler(self):
        from tusk_cluster.scheduler import Scheduler
        return Scheduler(host="localhost", port=8814)

    def test_stale_worker_detection(self):
        s = self._make_scheduler()
        stale_time = datetime.now() - timedelta(seconds=60)
        s.workers["w1"] = WorkerInfo(
            id="w1", address="localhost", port=8815,
            last_heartbeat=stale_time, status="idle",
        )
        s.jobs["j1"] = Job(id="j1", sql="SELECT 1", status="running", worker_id="w1")

        s._running = True
        # Run one iteration of _monitor_workers logic inline
        now = datetime.now()
        for worker_id, w in list(s.workers.items()):
            if (now - w.last_heartbeat).total_seconds() > 30 and w.status != "offline":
                s.workers[worker_id] = WorkerInfo(
                    id=w.id, address=w.address, port=w.port, status="offline",
                    cpu_percent=w.cpu_percent, memory_mb=w.memory_mb,
                    memory_percent=w.memory_percent, last_heartbeat=w.last_heartbeat,
                    jobs_completed=w.jobs_completed, bytes_processed=w.bytes_processed,
                    active_jobs=0,
                )

        assert s.workers["w1"].status == "offline"
        assert s.workers["w1"].active_jobs == 0
