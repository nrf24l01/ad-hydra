# Reproducible verification

Use the versions in `rust-toolchain.toml` and `.github/workflows/ci.yml`. Install prerequisites from the [developer guide](DEVELOPMENT.md). The workflow file is the exact CI command reference; the commands below are its application checks, run from repository root.

Build the frontend before Cargo checks so Core can embed it:

```bash
python3 -m pip install -r scripts/requirements.txt
(cd frontend && bun install --frozen-lockfile && bun run typecheck && bun run test && bun run build)
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
scripts/check-contracts.sh
python3 scripts/generate-frontend.py --check
python3 tests/compatibility/frontend_build.py
python3 tests/compatibility/installer.py
cargo build --locked --workspace
```

Run relevant real-process fixtures after building:

```bash
python3 tests/e2e/relay.py
python3 tests/e2e/proxy_runtime.py
python3 tests/e2e/udp_history.py
python3 tests/e2e/forwarding.py
python3 tests/e2e/tls.py
cargo build --locked --release --manifest-path plugins/examples/length-prefixed/Cargo.toml --target wasm32-unknown-unknown
HYDRA_WASM_SAMPLE="$PWD/plugins/examples/length-prefixed/target/wasm32-unknown-unknown/release/hydra_length_prefixed_example.wasm" python3 tests/e2e/http_filters.py
python3 tests/e2e/flags_controls.py
python3 tests/e2e/python_submitter.py
python3 tests/e2e/operations.py
python3 tests/e2e/request_checkers.py
python3 tests/e2e/service_retry.py
python3 tests/e2e/diagnostics.py
HYDRA_VERIFY_EMBEDDED=1 python3 tests/e2e/browser.py
cargo build --locked --release --workspace
```

The WASM target must be installed before building the standalone sample. Python submission checks require `uv` (CI pins 0.10.9); the browser smoke requires Chromium or Google Chrome and OpenSSL. Request-checker fixtures require Bubblewrap, libseccomp and permitted user namespaces. See [request checkers](REQUEST_CHECKERS.md), [sandbox prerequisites](sandbox-integration.md) and the CI workflow's Ubuntu AppArmor setup when a host denies namespace creation. Do not disable checks to disguise a missing prerequisite.

Ordinary fixtures use temporary files and owned local listeners. The operations fixture reports a failed Farm job when its execution sandbox is not configured. Request-checker and Python submitter fixtures do launch local workers; they do not contact competition targets. The separate Farm execution/kernel suites remain explicit opt-ins as described below. Build/package downloads can require network access. No Docker daemon is required.

For a focused change, run the affected crate/feature and corresponding fixture first; use the full sequence for integration/release. `HYDRA_BUILD_PROFILE=release` selects built native release binaries, or `HYDRA_BUILD_PROFILE=x86_64-unknown-linux-musl/release` selects built static binaries. Do not run the embedded browser smoke concurrently with frontend rebuilds: it temporarily hides `frontend/dist` to prove assets are embedded.

Resource smoke command, if systemd user scopes are available:

```sh
systemd-run --user --scope --quiet -p MemoryMax=2G -p CPUQuota=200% /usr/bin/python3 tests/e2e/relay.py
python3 tests/e2e/udp_history.py
```

Proxy sizes Tokio workers from available CPUs and bounds its blocking pool; Core uses its configured two workers. The relay test additionally sets two CPU affinity slots. Results and limitations belong in PROGRESS.md. Do not infer sustained performance from this short check.

`tests/sandbox/run-linux.py` and `tests/e2e/farm_execution.py` are explicit opt-in execution fixtures. They are excluded from ordinary CI. The former performs provisioning only within its own transient delegated test scope. They are not run when the operator requests no exploit-execution testing.
