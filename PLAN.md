# multiomics monorepo: Nx + Dagster multi-KG layout

## Context

- `github.com/glusman-team/multiomics` is blank (Apache-2.0 LICENSE only). Becomes the monorepo at `~/Code/ISB/Multiomics` (cwd).
- Goal: one repo hosting (a) an Nx task graph + cached test runner over everything (pytest, go test, mill later), (b) a Dagster deployment with **several independent DAG code locations**, each building **its own knowledge graph**, sharing Python libs. Each DAG project is a self-contained workspace; cross-language code (Go workers, generated types) lives inside the DAG that runs it.
- DAG roadmap: **`dakp` first** (Dagster port of DAKP, off Airflow — scaffold now, full code migration phase 2), then other new KG DAGs, then `ctkp`. DAKP is public → no visibility issue. CTKP is private → folding it in means making `multiomics` private; deferred until CTKP migration starts (not blocking now). Scala workers deferred until a need shows up (flake already carries jdk+mill so nothing blocks it).
- Agent harness is part of the repo: pi via flake (`agent-of-empires`, same as `/etc/nixos`). Pi **stacks** context files — repo `AGENTS.md` *expands* the global `~/.pi/agent/AGENTS.md`, never overrides (verified; only `AGENTS.override.md` would, and we don't create one). `.pi/` skills/extensions/prompts merge with global after project trust. Symlinks expose the same files to Claude Code.
- Environment (verified): node 24.20.0 / npm 11 (nix), uv 0.12.17 (nix), go 1.26.7, python 3.14.7 system. Nx not in nixpkgs → `npx nx init`. dagster 1.13.24 needs Python `>=3.10,<3.15` → pin **3.12** via uv.

## Layout

```
multiomics/                     # git root = nx workspace root (no TypeScript anywhere)
├── package.json / nx.json      # nx + @naxodev/gonx (infers build/test/lint from every go.mod)
├── AGENTS.md                   # canonical agent doc: layout, conventions (pi stacks it with global — expands, never overrides)
├── CLAUDE.md -> AGENTS.md      # committed symlink
├── .pi/                        # project harness: skills/ (seeded), extensions/, prompts/, settings.json
├── .claude/skills -> ../.pi/skills
├── .envrc                      # use flake (nix-direnv) → cached devshell automatically
├── prek.toml                   # prek 0.5.3 (nixpkgs), two-stage DAKP-style:
│                               #   pre-commit (fast, auto-fix): ruff-check --fix, ruff-format,
│                               #     golangci-lint run --fix (*.go, gofmt linter on), treefmt (*.nix → nixfmt)
│                               #   pre-push (heavy gates): scripts/check-locks.sh (uv lock --check per touched
│                               #     dagster/{libs,projects}/*), npx nx affected -t test, nix flake check --no-build
├── secrets/                    # sops+age encrypted files (ciphertext only) + .sops.yaml recipient rules;
│                               #   runtime via `sops exec-env` — see Secrets section
├── flake.nix                   # minimal, /etc/nixos-style: inputs (nixpkgs-unstable, flake-parts, agent-of-empires),
│                               #   nixConfig caches (cache.nixos.org + nix-community cachix + AoE's when published),
│                               #   formatter=nixfmt-tree, checks.flake, modules list. Nothing else.
├── nix/modules/
│   ├── devshell.nix            # THE one devshell: uv, nodejs_24, go, gopls, golangci-lint, git, gh, prek,
│   │                           #   treefmt, jdk+mill (scala seam), pi, sops, age, actionlint, quicktype;
│   │                           #   env GOTOOLCHAIN=local, SOPS_AGE_KEY_FILE; shellHook asserts CLAUDE.md/.claude
│   │                           #   symlinks + prek install → devShells.default
│   └── dagster.nix             # DAGSTER_HOME=dagster/.dagster-home (gitignored); apps.#dagster-dev (dg dev);
│                               #   packages.#processes (process-compose: dagster-daemon + webserver,
│                               #   fixed ports, UI 3000, no auth — dagster OSS UI has none)
├── Makefile                    # make dev  = nix run .#dagster-dev   | make up   = nix run .#processes
│                               # make test = npx nx run-many -t test | make lint | make fmt | make hooks
├── .github/
│   ├── workflows/ci.yml        # python (uv sync+pytest per project), go (build/test/lint per module), nix flake check
│   └── ISSUE_TEMPLATE/         # DAKP-style bug_report (per-DAG stage checkboxes), feature_request, config.yml
└── dagster/                    # dg workspace — ALL python
    ├── dg.toml / .python-version (3.12)
    ├── projects/               # one project = one DAG = one code location = one workspace
    │   ├── dakp/               # FIRST: Dagster port of DAKP (scaffold + demo assets now, code migration phase 2)
    │   │   ├── pyproject.toml  # deps: dagster + mo-core/mo-kg/mo-dagster (editable path deps) + dakp pkg (phase 2)
    │   │   ├── project.json    # nx targets: test = uv run pytest (cached)
    │   │   ├── src/            # assets mirror DAKP stages: acquisition → extraction → NER → assertions → KG
    │   │   └── workers/go/     # DAKP-pattern Go extraction workers; own go.mod, no repo-wide go.work
    │   │                       #   internal/pipesutil (dagster-pipes-go + BLAKE3 cache reporting), gen/ (quicktype), extractor/
    │   └── <future-kg>/        # then ctkp (private — see Context)
    ├── libs/                   # shared uv packages, generic, Dagster-optional + project.json each
    │   ├── mo-core/            # domain models, pydantic schemas + export-json-schema script
    │   ├── mo-kg/              # KGX ndjson writer (ONLY sink) + KG QC
    │   └── mo-dagster/         # resources, IO managers, PipesSubprocessClient wrapper
    └── deployments/local/
```

## Design rules

- Per-DAG workspace: `cd dagster/projects/<name> && uv sync` is all a DAG needs. Never import across `projects/*`; shared code goes in `libs/*` or cross-code-location asset deps.
- New KG = `uvx create-dagster project projects/<name>` + wire libs + `project.json`.
- Migration (phase 2): packages → `projects/<name>/src/`, Go workers → `workers/go/`, original repo read-only. Scaffold already matches, so it's `git mv` + dep retarget.
- Workers isolated by language per DAG: `workers/<lang>/`, one build system each (go.mod today, mill later). Go stays per-DAG; promote to a shared module only when two DAGs need identical code.
- Workers are Pipes clients: `PipesSubprocessClient` → Go binary via `hupe1980/dagster-pipes-go` (pinned behind `internal/pipesutil`). JVM pipes exists (`dagster-pipes-java`) for future scala via Java interop.
- Types: `mo-core` (pydantic) is canonical → per-DAG `quicktype` codegen into `workers/go/gen/` (extraction/NER record schemas, not just KG structs) + Go↔Python fixture roundtrip test.
- Caching: Dagster asset memoization (`code_version`/`data_version`) + Go-worker BLAKE3 content-addressed cache reported via pipes metadata (DAKP-proven pattern).
- Nx test loop: each python package gets `project.json` with cached `test` (uv run pytest, inputs incl. `dagster/libs/**`); gonx covers go; scala later = project.json calling mill. `npx nx test <x>` / `run-many` / `affected -t test` all work; env vars declared once in devshell.nix, never hardcoded.
- Agents: AGENTS.md is canonical, never duplicated; skills live in `.pi/skills/`.

## Secrets in a public repo (comparison)

Constraint: `multiomics` is public; dagster resources, workers, and CI need API keys. Never commit plaintext. Options compared:

| Option | How it works | Secret bytes in repo? | Nix integration | Ubuntu (non-NixOS) fit | When to use |
|---|---|---|---|---|---|
| **Untracked `.env` + direnv/shellHook** (recommended default) | `dagster/.env` in `.gitignore`; `.envrc` (`use flake` + `dotenv_if_exists dagster/.env`) or devshell shellHook sources it into every shell; dagster resources read `os.environ` | No — values never leave the machine | Nix never sees values, only wires paths | Perfect (direnv + nix both fine) | Local dev secrets (9router key, model endpoints). Zero crypto, zero tooling. Limitation: not versioned, not shared via repo |
| **sops + age (encrypted files in repo)** (recommended for shared/deployed secrets) | `secrets/*.enc.env` committed, age-encrypted to named recipients; `.sops.yaml` creation rules; decrypt at runtime: `sops exec-env secrets/dagster.enc.env -- make up`, or in CI with `SOPS_AGE_KEY_FILE` from a GitHub secret; sops-nix module ready if a host ever runs NixOS/home-manager | Yes — but ciphertext only; diffs reviewable, re-encrypt to rotate | `sops`/`age` in devshell packages; apps wrap sops exec-env | Good — plain CLI, no NixOS activation needed | Secrets that must reach other machines or CI (deploy-time dagster keys, DB URLs). Encrypted-in-git = shareable + auditable |
| **agenix** | Secrets as `.age` files; decrypted at NixOS/home-manager **activation** into `/run/agenix` | Ciphertext in repo | First-class NixOS module | Poor — activation-based, no NixOS here | Skip: designed for NixOS hosts; adds rekey friction for no gain over sops on Ubuntu |
| **git-crypt** | Transparent git filter: marked files auto-encrypt/decrypt in working tree | Plaintext locally, ciphertext in git | None specific | OK (plain git filter) | Skip: needs out-of-band key distribution, whole-file locking, low maintenance; sops covers the same need with better ergonomics |
| **GitHub Actions secrets** | Values stored in repo settings; injected as CI env | Nothing in repo | CI-only (`secrets.*` in workflows) | n/a | CI-only values (AGE private key for sops decrypt, npm tokens). Never a runtime source — jobs are ephemeral |
| **flake-eval secrets (`builtins.getEnv` / imported file)** | Read env vars or an untracked nix file at eval time | No (but untracked files are invisible to flakes — needs `--impure` or absolute paths) | Native but purity-hostile | OK | **Avoid**: breaks flake purity, pollutes the store with secrets, leaks values into derivations. Env must flow at *runtime*, not eval time |

Adoption in this repo: (1) `.gitignore` gets `dagster/.env` + `dagster/.dagster-home/`; (2) devshell shellHook + `.envrc` load `dagster/.env` if present; (3) `sops` + `age` in devshell packages; (4) `secrets/` dir with `.sops.yaml` (age recipient list = Skye + deploy host) — encrypted files land only when a shared secret actually exists; (5) CI decrypts sops files with an Actions secret holding the age key; (6) workers/dagster resources read env only — consistent with the no-hardcoded-paths rule.

Examples:

```sh
# local dev: one untracked file, loaded by direnv/shellHook automatically
cat >> dagster/.env   # NINE_ROUTER_KEY=..., DB_URL=...   (never committed)

# shared/deployed: encrypted, committed
age-keygen -o ~/.config/sops/age/keys.txt            # once, per person/host
sops secrets/dagster.enc.env                          # edit; auto-encrypts on save (SOPS_AGE_KEY_FILE set by devshell)
sops exec-env secrets/dagster.enc.env -- make up      # runtime: decrypt into env of dagster only

# CI (.github/workflows/ci.yml)
#   env: SOPS_AGE_KEY: ${{ secrets.SOPS_AGE_KEY }}    # age private key in Actions secrets, used only by sops steps
```

## Steps

- [ ] 1. Clone: `git clone https://github.com/glusman-team/multiomics /tmp/multiomics && rsync -a /tmp/multiomics/ . && rm -rf /tmp/multiomics` (cwd non-empty, so `git clone .` would fail)
- [ ] 2. Nx root: `npx nx init`, `npm install`, `npm i -D @naxodev/gonx`, register in `nx.json` plugins
- [ ] 3. Secrets plumbing: `.gitignore` (`dagster/.env`, `dagster/.dagster-home/`), `.envrc` loads `dagster/.env` via `dotenv_if_exists`, `sops` + `age` added to devshell packages, `secrets/.sops.yaml` age-recipient rules, devshell sets `SOPS_AGE_KEY_FILE` (per Secrets section — no plaintext in git, no secrets at flake-eval time)
- [ ] 4. prek.toml with the hooks above; prek + treefmt in devshell; `prek install` in shellHook
- [ ] 5. Dagster workspace: `cd dagster && uvx create-dagster@latest workspace .` + `.python-version` = 3.12
- [ ] 6. First location: `uvx create-dagster project projects/dakp`; assets mirror DAKP stages (1:1 with phase-2 port)
- [ ] 7. Libs: `uv init --lib` mo-core / mo-kg / mo-dagster; path-dep them into the project (not in dg.toml)
- [ ] 8. Demo surface: mo-kg `write_kgx_nodes/edges` + `KGSink` protocol; 2-3 dakp demo assets → toy KGX ndjson
- [ ] 9. Go: `workers/go/` module, `internal/pipesutil`, `extractor/main.go`, root `.golangci.yml`, workers README (per-language rule)
- [ ] 10. Codegen: mo-core `export-json-schema` script; `go generate` (quicktype → `workers/go/gen/`); roundtrip test
- [ ] 11. Pipes demo: mo-dagster `PipesSubprocessClient` helper; dakp asset builds + launches extractor, reports materialization + BLAKE3 cache metadata
- [ ] 12. Nx test wiring: `namedInputs` + cached `test` targets per python package (as in Design rules)
- [ ] 13. Flake: minimal `flake.nix` + `nix/modules/{devshell,dagster}.nix` per Flake contents above; `.envrc` (`use flake`)
- [ ] 14. Agent harness: AGENTS.md (layout, formatting conventions, secrets policy, migration plan, cheat-sheet), seeded skills ("add-dag", "add-worker-go", "materialize-local"), committed symlinks
- [ ] 15. CI + issue templates: ci.yml (or single `npx nx affected -t test lint` job; sops decrypt via `SOPS_AGE_KEY` Actions secret when encrypted secrets exist), ISSUE_TEMPLATE ported from DAKP
- [ ] 16. Makefile + README (add-dag / add-worker recipes, secrets usage from the Secrets section, commands, AGENTS.md pointer)

### Phase 2 (deferred)

- [ ] DAKP code migration: `git mv` package → `projects/dakp/src/`, workers → `workers/go/`, port `dakp_build` to assets+Pipes, DAKP repo read-only
- [ ] CTKP migration same way — requires `multiomics` to go private (CTKP code is private)
- [ ] First scala worker if needed: `workers/scala/` + mill + project.json; pipes via `dagster-pipes-java`

## Verification

- `npx nx graph` — python projects (project.json) + go module (gonx) visible; `nx affected -t test` re-runs only dependents; re-run = cache hit
- `cd dagster && uvx dg list project` — lists `projects/dakp`; `make dev` / `nix run .#dagster-dev` → localhost:3000 shows the location; materialize demo assets; KGX ndjson appears; pipes metadata: cache miss then hit on re-run
- `uv run pytest` in project passes; `import mo_core, mo_kg` resolves; `go test ./...` in `workers/go/` proves schema parity
- `nix flake check` passes; `nix develop -c pi --version` resolves; `readlink CLAUDE.md` → AGENTS.md; `test -e .claude/skills`; substituters hit (no source builds)
- `prek run --all-files` clean; `actionlint` clean on ci.yml
- Secrets: `git check-ignore dagster/.env` exits 0 (untracked env never committable); `sops --encrypt` roundtrip on a test file works with devshell `SOPS_AGE_KEY_FILE`; repo history contains no plaintext keys (`git log -p | grep -c $TEST_KEY_VALUE` = 0)

## Deferred decisions (stated, not open)

- **CTKP visibility**: folding private CTKP code in requires `multiomics` to go private. Deferred to CTKP migration time because new KG DAGs + DAKP don't need the answer.
- **Scala workers**: no current need; flake already ships jdk+mill and the `workers/<lang>/` + pipes-java path is documented, so adding one later is additive.
