---
name: materialize-local
description: Run the dakp pipeline locally through dg dev and verify KGX output.
---

# Materialize locally

1. Build the worker once: `cd dagster/projects/dakp/workers/go && go get github.com/hupe1980/dagster-pipes-go github.com/zeebo/blake3 && go build -o bin/extractor ./extractor`.
2. Sync the project venv: `cd dagster/projects/dakp && uv sync`.
3. Launch: `make dev` (repo root) - opens http://localhost:3000 with every
   code location loaded (`dg dev` reads `dagster/dg.toml`).
4. In the UI: select the `dakp` location -> `dakp_build` job -> Launch run.
   Or materialize `kg_build` only to re-emit KGX quickly.
5. Verify output under `dagster/projects/dakp/published/dakp/` (or `$MO_PUBLISHED_ROOT`):
   `acquisition.manifest.json`, `extraction.ndjson`, `kg.nodes.ndjson`, `kg.edges.ndjson`.
6. Cache check: re-run `extraction` -> worker metadata should show `cache_hit: true`
   (BLAKE3 content key unchanged, worker version unchanged).
7. Headless alternative: `make up` (process-compose daemon + webserver on the
   same fixed ports).

Troubleshooting: `FileNotFoundError: extractor worker not built` means step 1.
Import errors mean step 2 (or libs not path-depped in pyproject).
