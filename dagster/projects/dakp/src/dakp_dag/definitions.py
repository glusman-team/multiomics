"""Code-location entrypoint. dg loads the `defs` symbol (see pyproject [tool.dg])."""

import dagster as dg

from dakp_dag import assets

defs = dg.Definitions(
    assets=assets.all_assets,
    jobs=[assets.dakp_build],
    resources=assets.resources,
)
