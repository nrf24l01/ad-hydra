# Install Hydra from a build

Use an x86_64 Linux machine running systemd (Debian/Ubuntu or a dnf-based distribution). Hydra runs as two services; Rust, Node and Docker are not needed on the machine.

Copy the single [install-hydra.sh](../scripts/install-hydra.sh) file onto the VM, then run:

```bash
sudo bash install-hydra.sh
```

No Git checkout, AWS CLI, Rust or Node is required. On supported apt/dnf systems, the script installs missing curl, Python 3, OpenSSL, CA certificates and service-account tools automatically. curl 7.75 or newer is required for built-in AWS Signature V4.

Enter these values when prompted, one line at a time:

| Prompt | Value |
| --- | --- |
| Endpoint URL | Your HTTPS S3 API endpoint |
| Region | Your bucket region |
| Bucket | Your bucket name |
| Access key ID | Your read-capable S3 key ID |
| Secret access key | Your S3 secret |
| Session token | Press Enter for ordinary keys; supply a token for temporary credentials |

Access-key, secret and token prompts hide typed input. All S3 values are read from stdin; credentials are passed to curl through its stdin configuration, never command arguments or credential files. They are not stored in Hydra configuration, service units or installer logs. For automation, supply the same six newline-separated values from an existing secure stdin source; EOF after the fifth field also means no session token. Do not put credential literals into shell history or command arguments. Save the script first rather than piping its source into bash: stdin is needed for the S3 answers.

To download the complete public toolkit directly from the latest successful main build, run:

```bash
sudo bash install-hydra.sh --source github
```

The installer downloads the full Core and Proxy archive from the latest public GitHub Release without a token. The release contains SHA-256 checksums and build commit metadata. Set `HYDRA_GITHUB_REPOSITORY=owner/name` for a fork (default `nrf24l01/ad-hydra`). Use `--source github-auth` to select an unexpired Actions artifact, which requires an Actions-read token. `--source s3` selects S3 explicitly; S3 remains the default.

curl downloads exactly `hydra-core` and `hydra-proxy` using path-style signed HTTPS GET requests. The key needs read/GetObject access to those objects; CI's write-only publisher key will not work. No bucket-listing or write permissions are needed.

Before any installed files or services change, both downloads must pass SHA-256 checks against the `x-amz-meta-sha256` object metadata, have matching `x-amz-meta-commit` values, declare target `x86_64-unknown-linux-musl`, and contain x86_64 ELF binaries. If CI replaces one object while downloading, a mixed pair is rejected: rerun after publication finishes. Missing metadata, HTTP errors and redirects fail clearly. Each binary is limited to 64 MiB and each curl invocation to 300 seconds, with a process-level file-size bound for older curl versions too.

To download/verify without installing, run:

```bash
bash install-hydra.sh --download-only /tmp/hydra-verified
```

The output directory must not exist. It contains the two verified binaries plus locally generated `SHA256SUMS` and `BUILD.txt`; no S3 credentials are saved.

For offline installation, download the `hydra-linux-x86_64-musl-<commit SHA>` artifact from a successful `Rust verification` run in the repository's Actions tab. Pass its ZIP directly, or extract the contained `hydra-linux-x86_64-musl.tar.gz` and pass that:

```bash
sudo bash install-hydra.sh --archive /path/to/actions-artifact.zip
# or
sudo bash install-hydra.sh --archive /path/to/hydra-linux-x86_64-musl.tar.gz
```

The public release contains the static x86_64 Linux binaries and SHA256 checksums. `--source github-auth` downloads the Actions ZIP and requires a token. For noninteractive authenticated downloads, `HYDRA_GITHUB_TOKEN`, `GH_TOKEN` or `GITHUB_TOKEN` can provide it. `HYDRA_ARCHIVE_SHA256` optionally pins an offline archive. `HYDRA_DOWNLOAD_URL` remains a legacy HTTPS index override.

## Installer output and download errors

Installer status and error messages use ANSI colors when output is attached to a terminal with a usable `TERM`. Redirected output remains plain text. Set `NO_COLOR=1` to disable color.

A public GitHub install needs the `hydra-latest` release and its Linux archive. HTTP 404 means that release or asset is unavailable. Check the repository's Actions page for the release build result. Authenticated workflow artifacts use `--source github-auth`.

If `sudo` prints `unable to resolve host`, check that the VM's hostname resolves locally. On Debian/Ubuntu, `/etc/hostname` and an appropriate hostname entry in `/etc/hosts` should agree. This warning is separate from a GitHub download error.

## Open the console

The installer creates private state, service users, a localhost TLS certificate, systemd units, and a random initial account through Hydra's CLI. Read its credentials as root:

```bash
sudo cat /root/hydra-install-credentials.json
```

From your workstation, forward the console port and open **https://localhost:8080**:

```bash
ssh -L 8080:127.0.0.1:8080 root@YOUR_MACHINE
```

Accept the generated certificate for your own machine. Configure Services with each listener/upstream pair, choose its decoder and enable it. Configure authorized Farm teams explicitly. Competition service applications are installed separately; this script installs Hydra itself. Swagger is at `/docs/`.

Python execution uses pinned `uv` (installed if absent). Host execution remains an explicit Settings choice; the initial mode requires sandbox configuration before exploit execution. No submission destination is preconfigured. The default flag pattern is ForcAD’s `[A-Z0-9]{31}=`; configure another pattern when the competition differs.

## Upgrade and recovery

Running the same command again upgrades binaries while preserving existing users, database, certificates, settings, service units and their drop-ins. Existing accounts are never reset; the initial credential file is only created when the database has no users. Use your existing credentials after an upgrade.

The installer stops Core before taking a consistent SQLite backup, then verifies both services, gRPC and the HTTPS console. Failed installation restores the prior binaries, configuration and database and restarts services that were previously running. Backups are root-private under `/var/backups/hydra/`; retain or remove old backups according to your recovery needs. Newly installed OS packages, service users and directories remain after rollback.

Standard paths are `/etc/hydra/`, `/var/lib/hydra-core/`, `/var/lib/hydra-proxy/` and `/usr/local/bin/hydra-{core,proxy}`. Existing deployments with custom Core database, key or operations paths must use their normal deployment procedure. This script does not overwrite custom unit files. Console access stays on loopback; Proxy listeners bind the addresses configured in Services, including low ports through its restricted bind capability.
