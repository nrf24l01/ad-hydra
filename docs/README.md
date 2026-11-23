# Hydra documentation

Start with the [product README](../README.md) for installation and architecture. To resume development, read [START_HERE](../START_HERE.md), the [current handoff](HANDOFF.md), and the [developer guide](DEVELOPMENT.md).

## Product and engineering authority

| Document | Purpose |
| --- | --- |
| [SPEC](../SPEC.md) | Product requirements and explicit amendments |
| [AGENTS](../AGENTS.md) | Persistent engineering instructions |
| [Repository structure](../REPOSITORY_STRUCTURE.md) | Ownership and dependency boundaries |
| [Decisions](../DECISIONS.md) | Design choices and compatibility notes |
| [Implementation plan](../IMPLEMENTATION_PLAN.md) | Original phase order and acceptance criteria |
| [Progress](../PROGRESS.md) | Historical milestone evidence; newest handoff takes precedence over old status descriptions |

## Install and operate

- [Install, upgrade and recover](INSTALL.md), [configuration walkthrough](CONFIGURATION.md), and [manual systemd deployment](../deploy/README.md)
- [Service port mapping](service-port-mapping.md), [traffic viewer](traffic-view.md), and [service limits and favourites](traffic-protection.md)
- [Filter behavior](rules-integration.md) and [Python/WASI request checkers and persistent storage](REQUEST_CHECKERS.md)
- [Farm setup, runtimes, persistent files and logs](farm.md), [Python Farm authoring guide (Russian)](exploits-python.ru.md), and [flag operations](flags-operations.md)
- [Flag submission semantics](flags.md) and [forwarding with flag substitution](flag-forwarding.md)
- [CI artifacts, public releases and optional S3 publication](CI_RELEASES.md)

## Extend and verify

- [Development and local startup](DEVELOPMENT.md), [verification commands](VERIFICATION.md), and [frontend](vue-frontend.md)
- [Capture decoder tutorial](CUSTOM_DECODERS.md), [WASM ABI](PLUGIN_ABI.md), and [protocol integration](protocol-integration.md)
- [Core/Proxy control](control-integration.md), [critical telemetry replay](CRITICAL_SPOOL.md), and [performance evidence](PERFORMANCE.md)
- [Sandbox prerequisites](sandbox-integration.md) and [opt-in kernel tests](../tests/sandbox/README.md)

The generated public API is [contracts/openapi.yaml](../contracts/openapi.yaml); the internal wire source is [proxy.proto](../contracts/hydra/proxy/v1/proxy.proto). `openapi.future-draft.yaml`, `API_DESIGN_v1.1.md`, `ORIGINAL_HANDOFF_README.md`, dated design plans and older integration milestone paragraphs are historical references. They do not establish current implementation status or override SPEC amendments.
