# Linux systemd deployment

The supplied units run the two actual release executables, with independent restart policies and private state. They use separate `hydra-core` and `hydra-proxy` identities. Only Core can traverse its private SQLite/key/upload directories. A shared `hydra-control` group grants access to the mode0660 UDS; Proxy still requires the exact configured Core peer UID. Core and Proxy have no process dependency: Core retries an absent Proxy; Proxy forwards with saved state while Core is down. No ACL helper or third permanent process is required.

Provision as an operator; do not execute these administrative commands as part of a build:

```sh
groupadd --system hydra-control
useradd --system --user-group --groups hydra-control --home-dir /nonexistent --shell /usr/bin/nologin hydra-core
useradd --system --user-group --groups hydra-control --home-dir /nonexistent --shell /usr/bin/nologin hydra-proxy
install -m 0755 target/release/hydra-core target/release/hydra-proxy /usr/local/bin/
install -d -m 0755 -o root -g root /etc/hydra
install -d -m 0700 -o hydra-core -g hydra-core /var/lib/hydra-core
install -d -m 0700 -o hydra-proxy -g hydra-control /var/lib/hydra-proxy
install -m 0644 deploy/systemd/hydra-core.service deploy/systemd/hydra-proxy.service /etc/systemd/system/
```

Copy and edit `config/proxy.example.json` as `/etc/hydra/proxy.json`: set socket `/run/hydra-proxy/proxy.sock`, state `/var/lib/hydra-proxy/last-good.json`, and explicit competition listeners/upstreams. Set `socket_mode` to432 (octal0660) and `allowed_core_uid` to the numeric output of `id -u hydra-core`. Set owner hydra-proxy and mode0600 on Proxy config. For listeners below1024, grant only `CAP_NET_BIND_SERVICE` through the Proxy unit if required by the host; the supplied examples use unprivileged ports. Copy `config/operations.example.json` to `/etc/hydra/operations.json`, owner hydra-core mode0600; configure actual competition pattern/checker or keep them disabled. Provision a trusted TLS certificate and private key at the paths in the Core unit, readable only by hydra-core, with key mode0600. Change `--origin` to the exact HTTPS origin exposed to users. Core binds loopback, so remote operators need an authenticated local tunnel or appropriately configured HTTPS frontend.

Create the initial account through the CLI hidden prompt:

```sh
runuser -u hydra-core -- /usr/local/bin/hydra-core --database /var/lib/hydra-core/hydra.sqlite3 --key /var/lib/hydra-core/jwt.pem create user alice
systemctl daemon-reload
systemctl enable --now hydra-proxy.service hydra-core.service
```

Do not enable exploit execution until a writable cgroup subtree with cpu/memory/pids controllers has been delegated and provisioned. These baseline units deliberately leave operations sandbox configuration null. A supported deployment must place Core in a supervisor leaf, enable the delegated subtree controllers outside the application, allow unprivileged user namespaces, and grant Core write access to its own job subtree. Set a short private runtime directory such as `/run/hydra-core/jobs` (create0700). Consult `docs/sandbox-integration.md`; setting a path in JSON alone does not establish delegation. Missing prerequisites produce failed runs.

Stop Core for backup, then archive the whole `/var/lib/hydra-core` directory preserving modes/ownership. Archive `/var/lib/hydra-proxy` separately. Core signing key and flag database are secrets. Restore matching state/key files together; do not replace Proxy state while it is running. Proxy's private directory includes completed zstd stream-history records (and backward-readable legacy gzip records) and one-shot samples; keep its owner/mode intact. In-progress history is held in memory and is not recovered after restart. A normal Proxy restart preserves completed history.

For Python workers install a trusted `uv` release, system Python and CA certificates.
Set `sandbox.uv` to its absolute executable path if it is not `/usr/bin/uv`. First-run
wheel preparation requires HTTPS/DNS access to PyPI; exploit execution itself remains
offline apart from its explicit target broker. Keep the short private runtime directory
on persistent storage when exact resolved Python environments must survive restarts,
and provision a filesystem quota if a hard disk cap is required. See
[Python preparation limits](../docs/sandbox-integration.md#python-packages-and-uv).

## Farm host runner

Host execution is an explicit alternative to the default sandbox mode. Configure it as described in [Farm](../docs/farm.md#host-process-execution); host jobs run with Core's OS-user and systemd permissions. Python uses uv, a persistent host-runner home/cache and a temporary run directory. See the Farm guide for the actual storage scope. Traffic checkers are a separate Proxy feature with their own networkless `/state` mounts; see [request checker storage](../docs/REQUEST_CHECKERS.md#where-files-are-stored).

The [installer guide](../docs/INSTALL.md) is the reference for standard generated units and `/etc/hydra/core-settings/operations.json`. The hand-installed example above deliberately uses `/etc/hydra/operations.json`; always inspect the active unit arguments to find the effective path before backup or recovery.

