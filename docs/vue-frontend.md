# Vue frontend

The console uses Vue 3, TypeScript, Vue Router, Vite, Bun and local shadcn-vue primitives. Feature pages remain under `frontend/src/features/`, authentication under `frontend/src/features/auth/`, application routing under `frontend/src/app/`, and shared API/UI code under `frontend/src/shared/` and `frontend/src/components/`. The TypeScript HTTP operation map under `frontend/src/generated/` is generated from the checked-in OpenAPI snapshot using `python3 scripts/generate-frontend.py`. Browsers use Core's HTTP API only.

Traffic is the landing page. Its left column starts with the configured-service dropdown and a searchable session list; selecting a session opens a separate scrolling stream pane on the right. TCP is the default transport, with advanced search still available. Session queries use the additive `newest_first=true` option; the API default remains oldest-first for existing clients. Cursors must be reused with the same ordering and filters. The sidebar loads 100 records per page and keeps at most 500, merges fresh closure/counter metadata without replacing the selected stream, and refreshes visible pages every five seconds. Decoded messages remain chronological, paginated and capped at 500 per preview.

The shared stream viewer shows client traffic in red, server traffic in blue and invalid UTF-8 byte annotations in yellow. It offers terminal playback, structured HTTP/decoder fields, and captured-byte hex/UTF-8/ASCII views. TCP chunks and UDP datagrams have saved-history views; history is bounded and can be partial, expired or unavailable. The separate one-shot live sample still captures only the next chunk with a 30-second expiry. Saved data cannot be reconstructed once every retained copy is gone. Both layouts and text views escape payload content rather than rendering packet-controlled HTML. See [traffic behavior and retention](traffic-view.md) for ordering, budgets and decoder limits.

Build from the repository root:

```sh
(cd frontend && bun install --frozen-lockfile && bun run typecheck && bun run test && bun run build)
cargo build --locked --workspace
```

Bun 1.4.3 installs the pinned lockfile; CI also pins Node22.23.3 for Vite/vue-tsc executable scripts. These are build tools only. TypeScript5.9.3 is pinned because the installed vue-tsc does not support the native TypeScript7 compiler layout. Vite writes ignored `frontend/dist/index.html` and hashed JavaScript/CSS under `frontend/dist/assets/`. Core's build script embeds those exact files into Cargo `OUT_DIR` using `include_bytes!`; it never runs a package manager or Vite. Production requires only the Core and Proxy binaries and their documented runtime configuration/state. No Bun/Node process, frontend checkout, asset directory or separate frontend service is required.

Core refuses a missing bundle, source/configuration/package/lockfile modifications newer than the oldest output file, symlink/nonregular assets, unsupported asset types, more than 256 files or more than 32 MiB. This freshness check uses filesystem modification times; it is not a source-content checksum and preserved timestamps can bypass it. The tracked configuration inputs include Vite `.env`, `.env.local`, `.env.production` and other `.env.*` mode files. Shell-provided `VITE_*` values cannot be checked with filesystem timestamps: rerun the frontend build whenever those build-time values change. Always rebuild Vue after checkout or frontend changes. Cargo watches frontend input paths and the output tree. The frontend root directory timestamp also participates in freshness checks so deleting or renaming root environment/configuration inputs requires a rebuild. Do not run Vite builds concurrently with Cargo builds or the embedded verification smoke.

The root document uses `Cache-Control: no-cache`. Hashed assets use immutable one-year caching, explicit MIME types and `X-Content-Type-Options: nosniff`. Only exact embedded asset URLs are served; unknown or traversal URLs return 404. Vue Router uses hash navigation (`/#/...`), so navigation and refresh need no arbitrary HTML fallback route. Authentication retains access tokens in memory and rotates the existing Secure/HttpOnly refresh cookie through the HTTP API.

For development, run `bun run dev` in `frontend/` with Core running at the Vite development proxy target configured in `vite.config.ts`. Use the embedded native HTTPS build to validate Secure cookie login/refresh/logout behavior. `VITE_API_BASE_URL` is a browser-visible build-time API base URL; the default is `/api` and the generated client appends `/v1`.

The standard browser smoke uses a real local HTTPS Core and Proxy plus Chromium:

```sh
HYDRA_VERIFY_EMBEDDED=1 python3 tests/e2e/browser.py
```

The opt-in environment variable temporarily renames `frontend/dist` before starting Core, then restores it in cleanup. Each harness-owned rename preserves the frontend parent directory timestamp, so this probe alone does not invalidate the bundle; do not modify the frontend tree concurrently with the smoke. This proves the built executable serves its Vue document and hashed assets without the frontend files at runtime. It also checks Vue mounting, login, session restoration on reload, page navigation and logout. Run it only after the frontend and Rust builds finish. The CI workflow builds/typechecks/tests Vue before Cargo checks/tests and runs this smoke after building both processes; no new exploit execution or kernel sandbox test is introduced by this frontend migration.

The isolated build-script regression fixture checks edits, deletion and renaming of root Vite/environment configuration without changing the real frontend output:

```sh
python3 tests/compatibility/frontend_build.py
```

Tooling references: [Vue/Vite single-file components](https://vuejs.org/guide/quick-start.html), [shadcn-vue Vite setup](https://www.shadcn-vue.com/docs/installation/vite), and [Vite production builds](https://vite.dev/guide/build.html).
