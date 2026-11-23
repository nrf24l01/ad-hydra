# Developer guide

Read [START_HERE](../START_HERE.md) and the [handoff](HANDOFF.md) before editing. Run commands below from the repository root. [SPEC](../SPEC.md) and explicit amendments define behavior; the implementation plan describes the original delivery sequence.

## Tools and build order

Linux development needs the Rust toolchain from `rust-toolchain.toml` (currently 1.99.0), a native C compiler/linker, `protoc`, Python 3 and the packages in `scripts/requirements.txt`. Frontend tooling is Bun 1.4.3 and Node 22.23.3 in CI. Use the checked-in Cargo and Bun lockfiles. `musl-tools` supplies the static-release linker on Debian/Ubuntu. WASM examples additionally need the appropriate Rust target.

```bash
rustup show
rustup target add wasm32-unknown-unknown x86_64-unknown-linux-musl
python3 -m venv /tmp/hydra-dev-venv
. /tmp/hydra-dev-venv/bin/activate
python3 -m pip install -r scripts/requirements.txt
(cd frontend && bun install --frozen-lockfile && bun run build)
cargo build --locked --workspace
```

Rebuild Vue before Cargo whenever frontend source, configuration, generated types or dependencies change. Core's build script embeds the exact `frontend/dist` bundle and rejects missing/stale assets. Do not build Vue while Cargo or the embedded browser smoke is using that output directory. Deployment needs no Bun or Node runtime.

## Run an isolated local instance

Use private throwaway state and high loopback ports. Do not point development binaries at an installed database. In the first terminal:

```bash
umask 077
export HYDRA_DEV_DIR="$(mktemp -d /tmp/hydra-dev.XXXXXX)"
python3 - <<'SETUP'
import json, os
from pathlib import Path
root = Path(os.environ['HYDRA_DEV_DIR'])
(root / 'proxy.json').write_text(json.dumps({
    'socket': str(root / 'proxy.sock'),
    'state_file': str(root / 'last-good.json'),
    'flag_pattern': '[A-Z0-9]{31}=',
    'snapshot': {'revision': 0, 'services': []}
}))
SETUP
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$HYDRA_DEV_DIR/tls.key" -out "$HYDRA_DEV_DIR/tls.crt" \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1'
target/debug/hydra-core --database "$HYDRA_DEV_DIR/hydra.sqlite3" \
  --key "$HYDRA_DEV_DIR/jwt.pem" create user developer
printf 'Development directory: %s\n' "$HYDRA_DEV_DIR"
target/debug/hydra-proxy --config "$HYDRA_DEV_DIR/proxy.json"
```

The account command prompts for a password. In a second terminal, set `HYDRA_DEV_DIR` to the printed directory and run:

```bash
export HYDRA_DEV_DIR=/tmp/hydra-dev.REPLACE_ME
target/debug/hydra-core --database "$HYDRA_DEV_DIR/hydra.sqlite3" \
  --key "$HYDRA_DEV_DIR/jwt.pem" --socket "$HYDRA_DEV_DIR/proxy.sock" serve \
  --listen 127.0.0.1:8080 --origin https://localhost:8080 \
  --tls-cert "$HYDRA_DEV_DIR/tls.crt" --tls-key "$HYDRA_DEV_DIR/tls.key"
```

Open `https://localhost:8080`, accept your local certificate and log in. Add an owned loopback upstream in Services, using separate application and listener ports as described in [configuration](CONFIGURATION.md). If 8080 is occupied, change both `--listen` and `--origin` and use that URL. Stop both processes with Ctrl-C before removing the throwaway directory.

For a quick real two-process relay fixture instead of manual configuration, run `python3 tests/e2e/relay.py` after building. It owns temporary state and loopback listeners.

The embedded HTTPS console is the reliable authentication development path. `bun run dev` uses the HTTPS backend in `frontend/vite.config.ts`; that certificate must be trusted by Node and the browser must satisfy Core's exact Origin/Secure-cookie policy. Plain HTTP Vite defaults do not reproduce production authentication. See [frontend details](vue-frontend.md).

## Source ownership

| Change | Primary location |
| --- | --- |
| Core bootstrap/CLI | `apps/hydra-core/src/app.rs`; `main.rs` only starts logging/runtime |
| REST and feature logic | `apps/hydra-core/src/modules/<feature>/`; shared HTTP handling in `src/api/` |
| SQLite connection and compression | `apps/hydra-core/src/infrastructure/db/`; feature SQL stays in its module; migrations in `migrations/` |
| Proxy sockets and TLS | `apps/hydra-proxy/src/transport/`; shared TCP/UDP context in `session.rs` |
| Parser wiring, history and telemetry | Proxy `src/inspection/`, `history/`, `telemetry/`; Core index under `modules/traffic/` |
| Rule types and evaluation | `crates/hydra-domain/`, `crates/hydra-filter-engine/` |
| Capture decoder SDK/host | `crates/hydra-protocol-sdk/`, `crates/hydra-protocol-runtime/` |
| Python/WASI traffic checkers | Proxy `src/request_checks/`; Core `modules/filters/checker*_http.rs` |
| Farm execution and submission | Core `modules/farm/`, `modules/flags/`, `infrastructure/{sandbox,host_process.rs,submitter_process.rs}` |
| UI | `frontend/src/features/<feature>/`, shared primitives under `components/`; HTTP only |

Core and Proxy never import each other. Only Core owns SQLite. Keep packet processing independent of HTTP, database calls and telemetry delivery. See [repository structure](../REPOSITORY_STRUCTURE.md) for the full dependency contract.

## Change a contract

For public HTTP changes, update real handler/schema declarations, then regenerate the OpenAPI snapshot and browser declarations:

```bash
cargo run --locked --quiet -p hydra-core -- openapi > contracts/openapi.yaml
python3 scripts/generate-frontend.py
(cd frontend && bun run build)
scripts/check-contracts.sh
python3 scripts/generate-frontend.py --check
```

The first Cargo command requires an existing current frontend bundle. Regenerating browser declarations changes a frontend input; rebuild Vue before the later Cargo-based checks. Commit the handler, generated snapshot and generated frontend files together. Never implement from `contracts/openapi.future-draft.yaml` without comparing it to SPEC and current handlers.

For internal gRPC, edit only `contracts/hydra/proxy/v1/proxy.proto`; `hydra-wire` generates Rust into Cargo OUT_DIR. Preserve field numbers, reserve removed ones and document mixed-version behavior. For persistent format changes, add a root migration and document backup/downgrade requirements; do not rewrite an already applied migration. Parser ABI changes also need SDK/host compatibility work and an updated [ABI guide](PLUGIN_ABI.md).

## Verify and release

Use [VERIFICATION](VERIFICATION.md) for focused checks and the CI acceptance sequence. Add meaningful regression coverage when behavior changes. Record commands actually run and distinguish a local smoke measurement from a sustained resource benchmark.

[CI_RELEASES](CI_RELEASES.md) explains static artifacts and publication. [INSTALL](INSTALL.md) covers standard upgrades, backups and recovery. Production upgrades need the correct service users, socket permissions and private state paths; follow the installer or the documented deployment scripts. Never commit local credentials, captures, databases or host-specific deployment details.
