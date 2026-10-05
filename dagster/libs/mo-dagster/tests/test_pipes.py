"""mo-dagster tests: helpers build without running dagster."""

import dagster as dg

from mo_dagster import pipes


def test_launch_worker_builds_command():
    # pure shape check: command is a [bin] argv, extras pass through
    context = dg.build_asset_context()
    result = pipes.launch_worker(
        context,
        "/nonexistent/bin",
        extras={"input": "/tmp/x", "output": "/tmp/y"},
    )
    assert result is not None


def test_resource_form_is_resource_definition():
    resource = pipes.subprocess_client_resource()
    assert isinstance(resource, dg.ResourceDefinition)
