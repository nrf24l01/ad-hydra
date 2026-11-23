# CI releases

`.github/workflows/ci.yml` runs on main pushes affecting source/build/test/workflow paths, matching pull requests, and manual dispatch. The verification job builds Vue into Core, validates contracts, runs Rust/frontend/integration tests and produces static x86_64 Linux binaries. The existing Actions ZIP/tar artifact remains downloadable for 30 days.

The final **Publish latest Linux binaries to S3** job depends on successful verification. Set the repository Actions variable `HYDRA_PUBLISH_S3` to `true` to enable publication on main pushes and manual main dispatches. It downloads the artifact from that exact run, checks the current main SHA, and skips superseded builds. Publication is serialized; an in-progress upload is not canceled by a newer run.

## Public GitHub releases and installer sources

After verification, **Publish public GitHub install archive** runs on main pushes and manual main dispatches. It checks that the build is still main's current commit, serializes publication, and replaces the moving `hydra-latest` release with `hydra-linux-x86_64-musl.tar.gz`. The archive contains both binaries, `SHA256SUMS` and `BUILD.txt` (commit and target). Actions artifacts retain the build-specific SHA in their names for 30 days; the public release is a moving latest build, not an immutable history of releases.

The installer accepts these sources:

| Option | Download and authorization |
| --- | --- |
| `--source github` | Full archive from the public release; no token |
| `--source github-auth` | Verified Actions artifact ZIP; Actions-read token |
| `--source s3` | Two binary objects with integrity metadata; stdin credentials; default source |
| `--archive PATH` | Already downloaded Actions ZIP or release tar archive |

See [installation](INSTALL.md) for full commands and recovery. S3 is optional and a skipped S3 job does not mean the public release failed. Its enabling variable and secrets must be configured separately; never put their values in source control.

## Build caches

The verification runner is pinned to Ubuntu24.04. Caches cover:

| Cache | Invalidation and behavior |
| --- | --- |
| Bun executable | Managed by setup-bun for pinned Bun1.4.3 |
| Bun package downloads | OS/architecture, Bun version and `frontend/bun.lock`; compatible downloads survive lockfile changes |
| pip downloads/wheels | OS/architecture, runner release and `scripts/requirements.txt`; pinned packages are still installed normally |
| Rust toolchain | `rust-toolchain.toml`, including rustfmt, clippy and native/musl/WASM target libraries |
| Cargo downloads and compiled dependencies | Rust version/environment, Cargo manifests/locks and build configuration; root and standalone length-prefixed plugin workspaces |

Cargo caching includes native debug/release, musl release and WASM sample targets. The cache action removes redundant registry source extraction, incremental state and workspace build outputs to keep the saved dependency cache useful. Main runs can save it even after a later check fails; PRs restore the shared main cache but do not save the Cargo cache. Cache actions are pinned to immutable commits.

The Vue production bundle rebuilds every run before embedding into Core. Tests and validation are never skipped on a cache hit. Runtime SQLite files, credentials, traffic, worker logs and temporary execution environments are not cached. The uv integration test gives child processes private temporary cache directories; setup-uv's global package cache is therefore deliberately disabled. Apt metadata is refreshed normally; protobuf and musl tools are installed together in one step.

The first run populates the new caches; a later run is required to measure warm-cache speed. The previous uncached successful verification took21m51s, including6m28s for native release and5m54s for static musl build. No speedup is claimed before a cache-hit run completes. Inspect **Actions → Caches**, `gh cache list`, or the cache-action log output for hit/miss and size information. Bump the toolchain cache's `v1` key segment to discard it manually when changing its target set.

## S3 destination

| Setting | Value |
| --- | --- |
| API endpoint | `https://s3.example.com` |
| Region | `example-region` |
| Bucket | `example-hydra-bucket` |
| Core object | `hydra-core` |
| Proxy object | `hydra-proxy` |

Only these two fixed keys are written. Each successful build overwrites them; no commit prefixes, archives, manifests or timestamped objects are uploaded to S3. Other existing bucket contents are untouched. Both objects carry `commit`, `sha256` and `target` metadata and a revalidation cache policy.

GitHub repository secrets:

- `HYDRA_S3_ACCESS_KEY_ID`
- `HYDRA_S3_SECRET_ACCESS_KEY`
- `HYDRA_S3_ENDPOINT`
- `HYDRA_S3_REGION`
- `HYDRA_S3_BUCKET`

To configure a value, use repository **Settings → Secrets and variables → Actions**, or `gh secret set NAME` and enter its value through the hidden prompt. Never place credentials in this repository, workflow literals, URLs or command arguments.

The runner needs Ubuntu 24.04's AWS CLI, GitHub CLI, Python, tar, readelf and sha256sum. The publish script forces path-style Signature V4 against the custom HTTPS endpoint. The release key needs object upload permission for the two keys; bucket listing and object read permission are not required. Before either upload, it checks the archive's binary hashes, BUILD.txt commit/target and static ELF headers. Each upload sends Content-MD5 and validates Garage's single-part ETag response against that checksum. SHA-256 is retained as object metadata.

Two separate S3 objects cannot be replaced atomically: if one upload fails, the job fails and rerunning publication replaces both. There is no extra staging object/manifest because the release bucket is intended to contain only the latest pair. Object metadata identifies the commit for each binary.

## Reproduce publication

With credentials already exported securely, download the verified Actions artifact and run:

```bash
export AWS_DEFAULT_REGION=example-region
export HYDRA_S3_ENDPOINT=https://s3.example.com
export HYDRA_S3_BUCKET=example-hydra-bucket
export GITHUB_SHA=FULL_VERIFIED_COMMIT_SHA
scripts/publish-s3.sh /path/to/hydra-linux-x86_64-musl.tar.gz
```

For routine publication, push to main or manually dispatch **Rust verification** on main. Tests must pass before the upload job starts.
