# workers: non-python parts of this DAG, one directory per language

Rules:

- One directory per language: `go/` today, `scala/` when needed. Never mix
  languages inside one directory.
- Each language directory owns its own build system (`go.mod`, mill
  `build.mill`). There is intentionally NO repo-wide `go.work` - workers are
  scoped to the DAG that runs them.
- Workers speak the Dagster Pipes protocol. The python side launches them via
  `mo-dagster` (`PipesSubprocessClient`); see `internal/pipesutil`.
- Shared Go code is NOT extracted preemptively. Only when two DAGs need the
  identical module, promote it to `dagster/golib/<name>` and have both go.mods
  reference it.

## go/

First build on a fresh checkout (fetches the two deps, then everything is
cached by the nix toolchain):

```sh
cd workers/go
go get github.com/hupe1980/dagster-pipes-go github.com/zeebo/blake3
go build -o bin/extractor ./extractor
go test ./...
```

- `internal/pipesutil` - pipes session + logging + materialization reporting
  with BLAKE3 content-addressed cache metadata (`cache_key`, `cache_hit`, `rows`).
- `extractor/` - the DAKP-pattern extraction worker binary (pipes client).
- `gen/` - Go types generated from `mo-core` JSON Schemas (`go generate`;
  quicktype). Fixture roundtrip test keeps Go and Python in sync.

`@naxodev/gonx` (registered in the root `nx.json`) infers `build`/`test`/`lint`
nx targets from this go.mod, so `npx nx build dakp-workers-go` etc. work from
the repo root.

## scala/ (future)

When a scala worker is needed: mill build (`build.mill`), types via the same
mo-core JSON Schema codegen (quicktype supports scala), pipes via
`dagster-pipes-java` (dagster-io/community-integrations) through Java interop.
jdk + mill are already in the nix devshell.
