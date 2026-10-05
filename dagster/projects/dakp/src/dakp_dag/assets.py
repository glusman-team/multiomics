"""Asset graph mirroring DAKP's stages.

Phase 1 bodies are demo implementations: they exercise the full pattern
(python assets -> Go pipes worker -> KGX ndjson output) with toy data.
Phase 2 replaces the bodies with the real DAKP pipeline package without
changing this asset graph.

Stage mapping from DAKP's Airflow DAG (`dakp_build`):
  acquisition  <- DailyMed / Drugs@FDA / FAERS pulls (Phase 2: dakp package)
  extraction   <- native Go extraction worker via Dagster Pipes
  ner          <- disease NER (gliner) (Phase 2)
  assertions   <- Tablassert-style assertion tables (Phase 2)
  kg_build     <- KGX ndjson via mo-kg (KGX ndjson is the only sink)
"""

import json
import os
from pathlib import Path

import dagster as dg

from mo_dagster import pipes as mo_pipes

# Everything runs from env; nothing hardcoded outside these defaults.
REPO_ROOT = Path(__file__).resolve().parents[4]
PUBLISHED_ROOT = Path(os.environ.get("MO_PUBLISHED_ROOT", REPO_ROOT / "published"))
WORKER_BIN = Path(
    os.environ.get(
        "DAKP_WORKER_BIN",
        REPO_ROOT / "dagster/projects/dakp/workers/go/bin/extractor",
    )
)


def _published(name: str) -> Path:
    path = PUBLISHED_ROOT / "dakp"
    path.mkdir(parents=True, exist_ok=True)
    return path / name


@dg.asset(description="DAKP stage 1: acquire source records (toy manifest in phase 1)")
def acquisition(context: dg.AssetExecutionContext) -> dg.Output:
    manifest_path = _published("acquisition.manifest.json")
    records = [
        {"id": "demo-1", "source": "dailymed", "text": "Demo record one."},
        {"id": "demo-2", "source": "faers", "text": "Demo record two."},
    ]
    manifest_path.write_text(json.dumps(records, indent=2) + "\n")
    context.log.info(f"wrote {len(records)} records to {manifest_path}")
    return dg.Output(value=str(manifest_path), metadata={"records": len(records)})


@dg.asset(description="DAKP stage 2: Go extraction worker via Dagster Pipes")
def extraction(context: dg.AssetExecutionContext, acquisition: str) -> dg.MaterializeResult:
    if not WORKER_BIN.exists():
        raise FileNotFoundError(
            f"extractor worker not built: {WORKER_BIN} "
            "(build with `go build -o bin/extractor ./extractor` in workers/go)"
        )
    client = mo_pipes.subprocess_client()
    return client.run(
        context=context,
        command=[str(WORKER_BIN)],
        extras={"input": acquisition, "output": str(_published("extraction.ndjson"))},
    ).get_output()


@dg.asset(description="DAKP stage 3: disease NER (placeholder in phase 1)")
def ner(context: dg.AssetExecutionContext, extraction: dg.MaterializeResult) -> dg.MaterializeResult:
    context.log.info(f"extraction metadata: {extraction.metadata}")
    # Phase 2: gliner-based NER from the dakp package. Placeholder passthrough now.
    return dg.MaterializeResult(metadata={"stage": "ner", "status": "placeholder"})


@dg.asset(description="DAKP stage 4: Tablassert-style assertions (placeholder in phase 1)")
def assertions(context: dg.AssetExecutionContext, ner: dg.MaterializeResult) -> dg.MaterializeResult:
    context.log.info(f"ner metadata: {ner.metadata}")
    return dg.MaterializeResult(metadata={"stage": "assertions", "status": "placeholder"})


@dg.asset(description="DAKP stage 5: build the knowledge graph as KGX ndjson")
def kg_build(context: dg.AssetExecutionContext, assertions: dg.MaterializeResult) -> dg.MaterializeResult:
    from mo_kg import kgx

    nodes = [
        {"id": "DRUG:demo", "name": "Demo drug", "category": "biolink:Drug"},
        {"id": "DISEASE:demo", "name": "Demo disease", "category": "biolink:Disease"},
    ]
    edges = [
        {
            "subject": "DRUG:demo",
            "predicate": "biolink:treats",
            "object": "DISEASE:demo",
            "knowledge_level": "prediction",
        }
    ]
    nodes_path = _published("kg.nodes.ndjson")
    edges_path = _published("kg.edges.ndjson")
    kgx.write_kgx_nodes(nodes_path, nodes)
    kgx.write_kgx_edges(edges_path, edges)
    context.log.info(f"KGX written: {nodes_path}, {edges_path}")
    return dg.MaterializeResult(
        metadata={
            "nodes": len(nodes),
            "edges": len(edges),
            "nodes_path": str(nodes_path),
            "edges_path": str(edges_path),
        }
    )


all_assets = [
    acquisition,
    extraction,
    ner,
    assertions,
    kg_build,
]

dakp_build = dg.define_asset_job("dakp_build", selection=dg.AssetSelection.assets(*all_assets))

resources: dict[str, object] = {
    "pipes_client": mo_pipes.subprocess_client_resource(),
}
