//! Regression for the constructor-time Unix-socket path budget.
//!
//! `Sandbox::new` must refuse an owned, private, canonical runtime directory whose
//! absolute path cannot fit a 62-byte Unix-socket address, and it must do so *before*
//! any cgroup, process or network work happens. The fixture therefore keeps every
//! other precondition satisfied (absolute dirs, real executables, non-loopback
//! allowlist, mode 0700, canonical path, same owner) so the only check that can fail
//! is the length guard itself; a general `Err` or a different variant is a bug.
use super::{Config, RuntimeDirectory, Sandbox, SandboxError};
use std::path::{Path, PathBuf};

/// Constructor budget: `len + 1 + 32 (uuid simple) + 12 (suffix) < 108`, i.e. 62 bytes.
const MAX_RUNTIME_PATH: usize = 62;

/// 63 bytes is a legal single filename component and alone exceeds the socket budget,
/// so the rejection holds even when the CI temp root is as short as `/tmp`.
const OVERSIZED_COMPONENT: usize = 63;

#[test]
fn oversized_owned_private_canonical_runtime_path_is_rejected() {
    let root = short_scratch_root();
    // Owns cleanup for the whole fixture and also runs on assert panic.
    let _cleanup = RuntimeDirectory(root.clone());

    let component = "p".repeat(OVERSIZED_COMPONENT);
    assert_eq!(component.len(), OVERSIZED_COMPONENT);
    let long = root.join(&component);
    create_private_dir(&long);
    let runtime_dir = long
        .canonicalize()
        .expect("canonicalize oversized runtime dir");

    // Retained preconditions: the failed guard must be length, not ownership/privacy.
    assert_owned_private(&runtime_dir);
    assert_eq!(
        runtime_dir.canonicalize().expect("re-canonicalize"),
        runtime_dir,
        "fixture runtime dir must already be canonical"
    );
    let path_len = path_len(&runtime_dir);
    assert!(
        path_len > MAX_RUNTIME_PATH,
        "fixture runtime path must exceed {MAX_RUNTIME_PATH} bytes, got {path_len}: {}",
        runtime_dir.display()
    );

    let executable = std::env::current_exe().expect("current test executable");
    let target = "192.0.2.1:80"
        .parse()
        .expect("non-loopback allowlist target");
    let config = Config {
        delegated_cgroup: root.clone(),
        runtime_dir: runtime_dir.clone(),
        bwrap: executable.clone(),
        ip: executable.clone(),
        uv: "/usr/bin/uv".into(),
        helper: executable,
        allowlist: vec![target],
    };

    match Sandbox::new(config) {
        Err(SandboxError::Invalid(reason)) => {
            assert!(
                reason.contains("too long for Unix sockets"),
                "expected the runtime-path length guard, got: {reason}"
            );
            assert!(
                reason.contains("62"),
                "rejection must name the 62-byte socket limit, got: {reason}"
            );
        }
        Err(other) => panic!("expected SandboxError::Invalid(path too long), got {other}"),
        Ok(_) => panic!("oversized runtime path ({path_len} bytes) was accepted by Sandbox::new"),
    }
}

/// Control: the same config with a canonical root inside the budget is still accepted,
/// proving the regression is length-specific. Constructor only; nothing is executed.
#[test]
fn short_canonical_runtime_path_is_still_accepted() {
    let root = short_scratch_root();
    let _cleanup = RuntimeDirectory(root.clone());
    let path_len = path_len(&root);
    if path_len > MAX_RUNTIME_PATH {
        // Hosts whose TMPDIR is already deeper than the socket budget cannot show
        // the accepted case; the oversized case above still holds there.
        return;
    }
    assert_owned_private(&root);
    assert_eq!(root.canonicalize().expect("re-canonicalize"), root);

    let executable = std::env::current_exe().expect("current test executable");
    let target = "192.0.2.1:80"
        .parse()
        .expect("non-loopback allowlist target");
    let config = Config {
        delegated_cgroup: root.clone(),
        runtime_dir: root.clone(),
        bwrap: executable.clone(),
        ip: executable.clone(),
        uv: "/usr/bin/uv".into(),
        helper: executable,
        allowlist: vec![target],
    };
    match Sandbox::new(config) {
        Ok(_) => {}
        Err(error) => {
            panic!("canonical {path_len}-byte runtime path must be accepted, got {error}")
        }
    }
}

/// `<tmp>/hs-<16 hex>`: 16 hex keeps the approved scratch root inside the 62-byte
/// budget, so the oversized child component is what decides the outcome.
fn short_scratch_root() -> PathBuf {
    let id = uuid::Uuid::new_v4().simple().to_string();
    let root = std::env::temp_dir().join(format!("hs-{}", &id[..16]));
    create_private_dir(&root);
    root.canonicalize().expect("canonicalize scratch root")
}

fn create_private_dir(path: &Path) {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(path)
        .unwrap_or_else(|error| panic!("create {}: {error}", path.display()));
    // umask can only clear bits; pin exactly 0700 so the ownership guard passes.
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("chmod {}: {error}", path.display()));
}

fn assert_owned_private(path: &Path) {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path).expect("stat fixture dir");
    assert!(metadata.is_dir(), "fixture must be a real directory");
    assert_eq!(
        metadata.mode() & 0o077,
        0,
        "fixture must not be group/other accessible"
    );
    assert_eq!(
        metadata.uid(),
        std::fs::metadata("/proc/self")
            .expect("stat /proc/self")
            .uid(),
        "fixture must be owned by the test process"
    );
}

fn path_len(path: &Path) -> usize {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().len()
}
