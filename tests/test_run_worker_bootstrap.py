"""run_worker kernel bootstrap tests — fakes, no Postgres needed."""
from __future__ import annotations

import sys
import types
from typing import ClassVar

import pytest

from keelflow.engine import worker as worker_mod
from keelflow.engine.kernel import KernelJobSpec
from keelflow.engine.registry import InMemoryRegistry


class _FakeQueue:
    calls: ClassVar[list[tuple]] = []

    def __init__(self, *args, **kwargs):
        type(self).calls.append((args, kwargs))


class _FakeWorker:
    def __init__(self, *args, **kwargs):
        self.state = {"started": 0, "stopped": 0, "waited": 0}

    async def start(self):
        self.state["started"] += 1

    async def wait_for_shutdown(self):
        self.state["waited"] += 1

    async def stop(self, **kwargs):
        self.state["stopped"] += 1


class _FakeChancyClient:
    created: ClassVar[list] = []
    workers: ClassVar[list] = []

    class Executor:
        Async = "async"

    def __init__(self, dsn, prefix=None):
        self.dsn = dsn
        self.prefix = prefix
        self.log = []
        type(self).created.append(self)

    async def __aenter__(self):
        self.log.append("enter")
        return self

    async def __aexit__(self, *exc):
        self.log.append("exit")

    async def migrate(self):
        self.log.append("migrate")

    async def declare(self, q, *, upsert=False):
        self.log.append(("declare", q, upsert))


def _install_fake_chancy(monkeypatch):
    mod = types.SimpleNamespace(
        Chancy=_FakeChancyClient,
        Queue=_FakeQueue,
        Worker=lambda client, **kw: _FakeChancyClient.workers.append(
            w := _FakeWorker(client, **kw)) or w,
    )
    monkeypatch.setitem(sys.modules, "chancy", mod)
    monkeypatch.setattr(worker_mod, "chancy", mod)
    _FakeChancyClient.created.clear()
    _FakeChancyClient.workers.clear()
    _FakeQueue.calls.clear()


def test_default_chancy_bootstrap(monkeypatch):
    """kernel=None preserves the Chancy client/queue/worker lifecycle."""
    _install_fake_chancy(monkeypatch)
    worker_mod.run_worker(InMemoryRegistry(), dsn="postgresql://x",
                          prefix="p_")

    client = _FakeChancyClient.created[0]
    assert client.prefix == "p_"
    assert client.log[0] == "enter"
    assert "migrate" in client.log
    assert client.log[-1] == "exit"
    declares = [e for e in client.log
                if isinstance(e, tuple) and e[0] == "declare"]
    assert declares and declares[0][2] is True
    assert _FakeQueue.calls  # default queue declared
    w = _FakeChancyClient.workers[0]
    assert w.state == {"started": 1, "waited": 1, "stopped": 1}


class _FakeKernel:
    """Minimal Kernel impl — caller-owned, must not be closed."""

    def __init__(self, dsn="sqlite://mem"):
        self.dsn = dsn
        self.log = []
        self.worker = _FakeWorker()

    def job(self, **kwargs):
        return KernelJobSpec(**kwargs)

    async def push_ex(self, cursor, spec):
        pass

    async def push_many_ex(self, cursor, specs):
        pass

    async def notify_pushed(self, cursor, queue="default"):
        pass

    def create_worker(self, **kwargs):
        return self.worker

    async def migrate(self):
        self.log.append("migrate")

    async def declare_queue(self, queue, *, upsert=False):
        self.log.append(("declare", queue))


def test_injected_kernel_bootstrap(monkeypatch):
    """An explicit Kernel is configured, migrated, started; never closed."""
    import os

    monkeypatch.delenv(worker_mod.DSN_ENV, raising=False)
    kernel = _FakeKernel()
    worker_mod.run_worker(InMemoryRegistry(), kernel=kernel)
    assert kernel.log == ["migrate"]
    assert kernel.worker.state == {"started": 1, "waited": 1, "stopped": 1}
    # dispatch configured with the kernel's own dsn
    assert os.environ[worker_mod.DSN_ENV] == kernel.dsn


def test_injected_kernel_rejects_chancy_queues(monkeypatch):
    """queues= is chancy-specific — refuse silently misconfiguring."""
    monkeypatch.delenv(worker_mod.DSN_ENV, raising=False)
    with pytest.raises(ValueError, match="queues"):
        worker_mod.run_worker(
            InMemoryRegistry(), kernel=_FakeKernel(), queues=[object()])
