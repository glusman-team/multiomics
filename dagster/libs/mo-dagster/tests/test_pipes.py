"""mo-dagster tests: helpers build without running dagster."""

from unittest.mock import patch

import dagster as dg

from mo_dagster import pipes


def test_launch_worker_builds_command():
    # pure shape check: command is a [bin] argv, extras pass through.
    # run() needs a real asset invocation context (open_pipes_session reads
    # context.has_assets_def), so stub the client and assert what we forward.
    context = dg.build_asset_context()
    with patch.object(pipes, "subprocess_client") as factory:
        result = pipes.launch_worker(
            context,
            "/nonexistent/bin",
            extras={"input": "/tmp/x", "output": "/tmp/y"},
        )
    factory.return_value.run.assert_called_once_with(
        context=context,
        command=["/nonexistent/bin"],
        extras={"input": "/tmp/x", "output": "/tmp/y"},
    )
    assert result is factory.return_value.run.return_value


def test_resource_form_usable_as_resource():
    @dg.asset
    def uses_pipes_client(
        context: dg.AssetExecutionContext,
        pipes_client: dg.PipesSubprocessClient,
    ) -> dg.MaterializeResult:
        return dg.MaterializeResult()

    resource = pipes.subprocess_client_resource()
    defs = dg.Definitions(
        assets=[uses_pipes_client],
        resources={"pipes_client": resource},
    )
    defs.get_repository_def()  # raises if the resource wiring is invalid
