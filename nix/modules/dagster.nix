# Dagster runtime: launch apps + env. dagster itself runs via uv inside dagster/
# (wheels pinned by each uv.lock) - it is not a nix package.
#
# Fixed ports: UI on 3000 everywhere (dg dev and process-compose alike).
# No auth: the dagster OSS webserver has none - nothing to configure.
{ pkgs, ... }:
let
  dagsterDir = "dagster";
  withDagsterHome = ''
    export DAGSTER_HOME="$PWD/${dagsterDir}/.dagster-home"
    mkdir -p "$DAGSTER_HOME"
  '';
in
{
  # dev loop: dg dev loads every projects/* as a separate code location
  apps.dagster-dev = {
    type = "app";
    program = toString (
      pkgs.writeShellScript "dagster-dev" ''
        set -euo pipefail
        ${withDagsterHome}
        cd ${dagsterDir}
        exec ${pkgs.uv}/bin/uv run dg dev
      ''
    );
  };

  # deployable runtime: daemon + webserver under process-compose, same ports
  # Ubuntu systemd units can be generated from this same config later.
  packages.processes =
    let
      processComposeYaml = pkgs.writeText "process-compose.yaml" ''
        version: "0.5"
        processes:
          dagster-daemon:
            command: "uv run dagster-daemon run"
            working_dir: "${dagsterDir}"
          dagster-webserver:
            command: "uv run dagster-webserver -w workspace.yaml -h 0.0.0.0 -p 3000"
            working_dir: "${dagsterDir}"
      '';
    in
    pkgs.writeShellScriptBin "processes" ''
      set -euo pipefail
      ${withDagsterHome}
      exec ${pkgs.process-compose}/bin/process-compose --config ${processComposeYaml} up
    '';
}
