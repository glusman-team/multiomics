"""Pipes helpers: launch Go workers (DAKP pattern) from dagster assets.

Python side of the contract with `workers/go/internal/pipesutil`:
- the worker reads its pipes context from env (set by PipesSubprocessClient)
- the worker reports materializations with BLAKE3 cache metadata
  (cache_key, cache_hit, rows)
"""

from __future__ import annotations

from dagster import AssetExecutionContext, PipesSubprocessClient


def subprocess_client() -> PipesSubprocessClient:
    """Fresh pipes subprocess client per invocation (dagster-recommended usage)."""
    return PipesSubprocessClient()


def subprocess_client_resource():
    """Resource form for definitions that prefer resources over direct calls."""
    return PipesSubprocessClient().resource()


def launch_worker(
    context: AssetExecutionContext,
    worker_bin: str,
    extras: dict | None = None,
):
    """Run a Go worker binary as a pipes subprocess, returning the pipes result.

    Caller returns `result.get_output()` from the asset body.
    """
    return subprocess_client().run(
        context=context,
        command=[worker_bin],
        extras=extras or {},
    )
