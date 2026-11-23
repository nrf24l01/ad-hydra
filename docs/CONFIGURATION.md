# Configure Hydra after installation

This guide takes you from a fresh install to a working listener, visible traffic, and a filter on one of your own services. For installation and console login, see the [README](../README.md#install-and-open-the-console).

## 1. Put one service behind Proxy

Start with a service you can verify with a normal client. If the application and Hydra share a VM, run the application on a separate loopback port and give Hydra the port clients already use. For example, move a local HTTP application to `127.0.0.1:18000` before configuring Hydra to listen on port `8000`.

In **Services**, add a service with these settings:

| Setting | HTTP example |
| --- | --- |
| Name | `Web` |
| Transport | TCP |
| Listener | `0.0.0.0:8000` |
| Destination | `127.0.0.1:18000` |
| Capture decoder | HTTP |
| Color | Pick a color to identify its stream cards. |

`0.0.0.0` listens on all IPv4 interfaces. Use your intended interface address or `127.0.0.1` when the listener should be restricted. Keep **Enable proxy listener** off while preparing the upstream. Once the upstream responds and the listener port is free, enable the listener and save. Check that Proxy has applied the service configuration.

Send a regular request to the listener using the service's normal client. Open **Traffic**, select the service, and open the new stream. For HTTP, check that you see both the request and the response. For UDP, use the service's UDP client and choose UDP in the service form. See [service port mapping](service-port-mapping.md) for IPv6 and remote backends.

## 2. Choose the capture decoder

| Decoder | When to use it |
| --- | --- |
| HTTP | Plaintext HTTP/1.x. Inspect requests, responses, headers, and available body previews. |
| UTF-8 | Text protocols such as line-based prompts. |
| Raw | Unknown or binary protocols. Inspect captured bytes in hex. |
| Custom WASM | Protocols with a decoder you upload under **Protocols** and select in **Services**. |

For TLS passthrough, traffic remains encrypted. To inspect HTTPS, edit a TCP service and enable TLS termination with a PEM certificate chain and its matching private key, then choose the HTTP decoder. Clients must trust that certificate. Enable upstream TLS separately when the backend also expects TLS; set its server name for verification. Insecure upstream verification is an explicit option for self-signed competition backends. Certificate updates apply to new connections without restarting Proxy; existing connections keep their negotiated TLS session. ALPN offers HTTP/1.1 only. Private keys are not returned by the API or configuration export, so retain them in your private deployment backup. Adding a certificate cannot decrypt old saved sessions. See the [WASM decoder guide](CUSTOM_DECODERS.md) and [Traffic Explorer guide](traffic-view.md).

## 3. Create a defensive filter

In **Filters → Add filter**, choose the service. The default form matches a regex against the raw stream text window; choose **Advanced expression** for decoder fields and nested groups, and give the filter a name. For example, on your HTTP test service, choose **Client → server**, **HTTP path**, **equals**, and enter `/blocked-demo`. Choose **Block HTTP request** as the action. Field choices depend on the selected service's decoder. Use nested **AND** and **OR** groups to combine conditions.

Choose **Shadow** to count matches while letting requests through. You can also preview the rule on saved traffic from your service. When you choose **Enforce**, an enabled rule applies as soon as Proxy acknowledges the new revision. Check the rule card for the applied status and hit count. To see a blocked conversation in **Traffic**, select **Show blocked**.

Under **Also apply to**, select more services or click **Select all services**. The saved rule has one switch and one hit count. Select all includes the services available at the time you edit the rule. Add future services by editing the rule. The editor lists fields with the same name and type on every selected decoder. A transport-specific action must be valid for every selected service.

HTTP blocking requires a request Proxy can frame before forwarding. For transport limits, closing a TCP connection ends both directions. A UDP drop discards the whole datagram. See [rule behavior](rules-integration.md).

For Python or WASM checks that use private persistent files, see [request checkers](REQUEST_CHECKERS.md).

## 4. Set retention and service limits

- In **Settings**, set the saved traffic budget in MiB per service and choose the zstd compression level. Saved byte history and decoded traffic have separate budgets. Changes to the saved history policy apply live when Proxy is connected. Star a stream in **Traffic** to preserve it through normal rotation.
- In **Services**, edit the service and turn on **Enable service overload protection** to set its active session cap, rates, and idle timeout. The rates apply across the service. Use **Live service traffic** to check ordinary connection rates and drops before adjusting the limits. See [service protection and favourites](traffic-protection.md).
- In **Users / audit**, choose **Add user** to provision another operator. Each authenticated account has full Hydra privileges. Use an individual account per operator so audit entries identify who changed a setting.

## 5. Know which configuration to edit

| Where | What belongs there | When it applies |
| --- | --- | --- |
| **Services**, **Protocols**, and **Filters** in the console | Listeners, destinations, decoders, and rules | When Proxy acknowledges the new revision |
| **Settings** in the console | Operator settings saved to `/etc/hydra/core-settings/operations.json` | Traffic retention and compression apply live. Other Core settings may require a Core restart. |
| `/etc/hydra/proxy.json` | Local socket, state file, global transport limits, and passive traffic flag pattern | When `hydra-proxy` starts |
| systemd unit settings and `/etc/hydra/` TLS files | Console bind address, HTTPS origin, and certificate paths | When `hydra-core` starts |

An omitted `flag_pattern` in Core operations configuration and Proxy JSON uses `[A-Z0-9]{31}=` for the ForcAD default flag format. Set a custom regex for another format or set it to `null` to turn off traffic markers. See [traffic flag detection](traffic-view.md).

For routine remote access, keep the console bound to loopback and use the SSH tunnel from the README. If you change the console's public origin, match Core's configured origin to the exact HTTPS URL operators open, including its port when present.

## Troubleshoot the first service

| Symptom | Check |
| --- | --- |
| Listener has not applied | Confirm Proxy is connected and the requested IP and port are free on the VM. Check the Proxy logs for bind errors. |
| No upstream response | Check the application directly from the VM. Confirm the destination is the application's port and the transports match. |
| Sessions appear but decoded fields are missing | Check the selected decoder and whether the service traffic is encrypted. Open **Captured bytes** to inspect available data. |
| Filter is saved but traffic still passes | Check its service, condition direction, enabled state, mode, and applied revision. Use an ordinary request that matches the rule. |

Run these on the VM to check both services:

```bash
sudo systemctl status hydra-core hydra-proxy
sudo journalctl -u hydra-core -u hydra-proxy -n 100 --no-pager
```

Use the [installation guide](INSTALL.md#upgrade-and-recovery) for backup, upgrades, and rollback.
