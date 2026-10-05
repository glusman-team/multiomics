.PHONY: dev up test lint fmt hooks help

# dagster dev server (dg dev, UI at http://localhost:3000)
dev:
	nix run .#dagster-dev

# full dagster runtime (process-compose: daemon + webserver, fixed ports, UI 3000)
up:
	nix run .#processes

# every cached test target across python and go
test:
	npx nx run-many -t test

lint:
	npx nx run-many -t lint

fmt:
	nix fmt

# install git hooks (prek, two-stage: pre-commit fast fixes, pre-push heavy gates)
hooks:
	prek install

help:
	@echo "make dev    - dagster dg dev (UI http://localhost:3000)"
	@echo "make up     - dagster daemon + webserver via process-compose"
	@echo "make test   - nx run-many -t test (all cached tests)"
	@echo "make lint   - nx run-many -t lint"
	@echo "make fmt    - nix fmt (treefmt/nixfmt-tree)"
	@echo "make hooks  - prek install"
