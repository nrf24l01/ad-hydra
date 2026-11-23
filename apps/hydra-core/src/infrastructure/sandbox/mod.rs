//! Fail-closed Linux exploit isolation. No untrusted process runs before its cgroup is configured.
mod bridge;
mod cgroup;
mod command;
mod python;
#[cfg(test)]
mod runtime_path_tests;
pub use python::validate_python_packages;

pub use bridge::{BridgeConfig, Endpoint, bridge};
pub use command::worker_child;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::{io::AsyncReadExt, sync::Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub enum SandboxError {
    Invalid(String),
    Unavailable(String),
    Io(std::io::Error),
}
impl std::fmt::Display for SandboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(s) | Self::Unavailable(s) => f.write_str(s),
            Self::Io(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for SandboxError {}
impl From<std::io::Error> for SandboxError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
pub type Result<T> = std::result::Result<T, SandboxError>;

#[derive(Clone)]
pub struct Config {
    /// A dedicated, empty cgroup v2 subtree delegated to Core by the operator.
    pub delegated_cgroup: PathBuf,
    /// Private directory owned by Core, never the upload directory.
    pub runtime_dir: PathBuf,
    pub bwrap: PathBuf,
    pub ip: PathBuf,
    pub uv: PathBuf,
    pub helper: PathBuf,
    /// Fixed numeric endpoints; worker input never selects the broker destination.
    pub allowlist: Vec<SocketAddr>,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Runtime {
    Python,
    Shell,
    Elf,
}
#[derive(Clone)]
pub struct Job {
    /// Private immutable snapshot containing only the uploaded exploit (no symlinks).
    pub source_dir: PathBuf,
    pub entrypoint: String,
    pub runtime: Runtime,
    pub python_packages: Vec<String>,
    pub python_environment_key: String,
    pub target: SocketAddr,
    pub endpoints: Vec<Endpoint>,
    pub timeout: Duration,
    pub memory_bytes: u64,
    pub cpu_millis_per_second: u64,
    pub pids: u32,
}
#[derive(Debug)]
pub struct RunOutcome {
    pub success: bool,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub cancelled: bool,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_lost_bytes: u64,
    pub stderr_lost_bytes: u64,
}
#[derive(Clone)]
pub struct Sandbox {
    config: Arc<Config>,
    slots: Arc<Semaphore>,
}
struct RuntimeDirectory(PathBuf);
impl Drop for RuntimeDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
impl Sandbox {
    /// Concurrency is deliberately fixed at one for the initial small-host adapter.
    pub fn new(config: Config) -> Result<Self> {
        if config.allowlist.is_empty() || config.allowlist.len() > 256 {
            return Err(SandboxError::Invalid(
                "sandbox needs 1..=256 numeric TCP allowlist endpoints".into(),
            ));
        }
        for target in &config.allowlist {
            validate_target(*target)?;
        }
        for executable in [&config.bwrap, &config.ip, &config.helper] {
            if !executable.is_absolute() || !executable.is_file() {
                return Err(SandboxError::Unavailable(
                    "sandbox executable missing or not absolute".into(),
                ));
            }
        }
        if !config.runtime_dir.is_absolute() || !config.delegated_cgroup.is_absolute() {
            return Err(SandboxError::Invalid(
                "sandbox directories must be absolute".into(),
            ));
        }
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::symlink_metadata(&config.runtime_dir)?;
        if !metadata.is_dir()
            || metadata.mode() & 0o077 != 0
            || metadata.uid() != std::fs::metadata("/proc/self")?.uid()
            || config.runtime_dir.canonicalize()? != config.runtime_dir
        {
            return Err(SandboxError::Invalid(
                "sandbox runtime directory must be owned, private and canonical".into(),
            ));
        }
        use std::os::unix::ffi::OsStrExt;
        if config.runtime_dir.as_os_str().as_bytes().len() + 1 + 32 + 12 >= 108 {
            return Err(SandboxError::Invalid(
                "sandbox runtime path is too long for Unix sockets (maximum 62 bytes)".into(),
            ));
        }
        Ok(Self {
            config: Arc::new(config),
            slots: Arc::new(Semaphore::new(1)),
        })
    }
    pub async fn run(&self, job: Job, cancel: CancellationToken) -> Result<RunOutcome> {
        command::validate_job(&job, &self.config)?;
        let _permit = tokio::select! {
            permit = self.slots.acquire() => permit.map_err(|_| SandboxError::Unavailable("sandbox closed".into()))?,
            () = cancel.cancelled() => return Err(SandboxError::Invalid("cancelled before execution".into())),
        };
        let directory = self
            .config
            .runtime_dir
            .join(uuid::Uuid::new_v4().simple().to_string());
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
        // Declare before Group: cancellation drops Group (kills descendants) first.
        let _directory_cleanup = RuntimeDirectory(directory.clone());
        let group = cgroup::Group::create(&self.config.delegated_cgroup, &job)?;
        let result = async {
            let python = if matches!(job.runtime, Runtime::Python) {
                Some(python::prepare(&self.config, &job, &cancel, &group, &directory).await?)
            } else {
                None
            };
            self.run_in(&job, &cancel, &group, &directory, python.as_deref())
                .await
        }
        .await;
        // Kill before removing broker paths, including daemonized descendants.
        let killed = group.kill();
        killed?;
        result
    }
    async fn run_in(
        &self,
        job: &Job,
        cancel: &CancellationToken,
        group: &cgroup::Group,
        directory: &std::path::Path,
        python: Option<&std::path::Path>,
    ) -> Result<RunOutcome> {
        let gateway_cancel = CancellationToken::new();
        let _gateway_guard = gateway_cancel.clone().drop_guard();
        let mut gateways = tokio::task::JoinSet::new();
        let slots = Arc::new(Semaphore::new(16));
        for (index, endpoint) in job.endpoints.iter().enumerate() {
            let listener =
                tokio::net::UnixListener::bind(directory.join(format!("target-{index}.sock")))?;
            gateways.spawn(bridge::gateway(
                listener,
                endpoint.clone(),
                gateway_cancel.clone(),
                slots.clone(),
            ));
        }
        let bridge_config = BridgeConfig {
            target: job.target,
            endpoints: job.endpoints.clone(),
            entrypoint: job.entrypoint.clone(),
            runtime: job.runtime.clone(),
        };
        let args = command::outer_args(
            &self.config,
            job,
            directory,
            python,
            &serde_json::to_string(&bridge_config)
                .map_err(|e| SandboxError::Invalid(e.to_string()))?,
        );
        let mut child = tokio::process::Command::new(&self.config.helper)
            .arg("sandbox-child")
            .arg(group.path())
            .arg(&self.config.bwrap)
            .args(args)
            .env_clear()
            .env("PATH", "/usr/bin")
            .env("LANG", "C.UTF-8")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SandboxError::Unavailable("missing stdout pipe".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| SandboxError::Unavailable("missing stderr pipe".into()))?;
        let out = tokio::spawn(capture(stdout));
        let err = tokio::spawn(capture(stderr));
        let mut timed_out = false;
        let mut cancelled = false;
        let status = tokio::select! {
            status = child.wait() => status?,
            () = cancel.cancelled() => { cancelled = true; group.kill()?; child.wait().await? },
            () = tokio::time::sleep(job.timeout) => { timed_out = true; group.kill()?; child.wait().await? },
        };
        group.kill()?;
        gateway_cancel.cancel();
        while gateways.join_next().await.is_some() {}
        let (stdout, stdout_lost_bytes) = out
            .await
            .map_err(|_| SandboxError::Unavailable("stdout reader failed".into()))??;
        let (stderr, stderr_lost_bytes) = err
            .await
            .map_err(|_| SandboxError::Unavailable("stderr reader failed".into()))??;
        Ok(RunOutcome {
            success: status.success() && !timed_out && !cancelled,
            exit_code: status.code(),
            timed_out,
            cancelled,
            stdout,
            stderr,
            stdout_lost_bytes,
            stderr_lost_bytes,
        })
    }
}
fn validate_target(target: SocketAddr) -> Result<()> {
    if !target.is_ipv4()
        || target.port() == 0
        || target.ip().is_unspecified()
        || target.ip().is_multicast()
        || target.ip().is_loopback()
        || target.ip() == std::net::IpAddr::V4(std::net::Ipv4Addr::BROADCAST)
    {
        return Err(SandboxError::Invalid(
            "sandbox target must be a non-loopback unicast IPv4 endpoint".into(),
        ));
    }
    Ok(())
}
async fn capture(mut reader: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<(Vec<u8>, u64)> {
    // Keep a contiguous 32 KiB tail per stream; drain excess without growing allocation.
    let mut bytes = Vec::with_capacity(32768);
    let mut lost = 0u64;
    let mut chunk = [0u8; 4096];
    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        let overflow = (bytes.len() + n).saturating_sub(32768);
        bytes.drain(..overflow);
        bytes.extend_from_slice(&chunk[..n]);
        lost = lost.saturating_add(overflow as u64);
    }
    Ok((bytes, lost))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn runtime_directory_is_cleaned_on_scope_drop() {
        let path = std::env::temp_dir().join(format!("hydra-cleanup-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        {
            let _cleanup = RuntimeDirectory(path.clone());
            std::fs::write(path.join("broker-path"), b"fixture").unwrap();
        }
        assert!(!path.exists());
    }
    #[tokio::test]
    async fn output_is_bounded_and_reports_loss() {
        let input = vec![b'x'; 100_000];
        let (captured, lost) = capture(input.as_slice()).await.unwrap();
        assert_eq!(captured.len(), 32768);
        assert_eq!(lost, 100_000 - 32768);
    }
    #[test]
    fn unsafe_target_forms_fail_closed() {
        for address in [
            "127.0.0.1:80",
            "0.0.0.0:80",
            "224.0.0.1:80",
            "255.255.255.255:80",
            "192.0.2.1:0",
            "[::1]:80",
        ] {
            assert!(
                validate_target(address.parse().unwrap()).is_err(),
                "{address}"
            );
        }
    }
    #[tokio::test]
    async fn unavailable_cgroup_never_starts_upload() {
        // Unix socket paths also spend bytes on TMPDIR, so the product budget is 62 bytes
        // for the sandbox root. A short fixture name keeps this valid on scratch TMPDIRs:
        // 16 hex chars carry the lower 64 UUID bits (62 random after the v4 variant bits).
        let root =
            std::env::temp_dir().join(format!("hs-{:016x}", uuid::Uuid::new_v4().as_u64_pair().1));
        assert!(
            root.as_os_str().len() <= 62,
            "fixture root exceeds the 62-byte Unix socket budget: {root:?}"
        );
        std::fs::create_dir(&root).unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        std::fs::write(root.join("run.sh"), "echo MUST_NOT_START").unwrap();
        let executable = std::env::current_exe().unwrap();
        let target = "192.0.2.1:80".parse().unwrap();
        let config = Config {
            delegated_cgroup: root.clone(),
            runtime_dir: root.clone(),
            bwrap: executable.clone(),
            ip: executable.clone(),
            uv: "/usr/bin/uv".into(),
            helper: executable,
            allowlist: vec![target],
        };
        let sandbox = Sandbox::new(config).unwrap();
        let job = Job {
            source_dir: root.clone(),
            entrypoint: "run.sh".into(),
            runtime: Runtime::Shell,
            python_packages: vec![],
            python_environment_key: "test".into(),
            target,
            endpoints: vec![Endpoint {
                id: "test".into(),
                address: target,
                transport: "tcp".into(),
            }],
            timeout: Duration::from_secs(1),
            memory_bytes: 32 * 1024 * 1024,
            cpu_millis_per_second: 100,
            pids: 16,
        };
        assert!(matches!(
            sandbox.run(job, CancellationToken::new()).await,
            Err(SandboxError::Unavailable(_))
        ));
        std::fs::remove_dir_all(root).unwrap();
    }
    /// Real kernel integration, explicitly opt in with operator delegation and a TCP echo server.
    #[tokio::test]
    #[ignore = "requires writable delegated cgroup, bubblewrap, ip and non-loopback TCP echo target; see docs/sandbox-integration.md"]
    async fn actual_linux_network_limits_and_cancellation() {
        let delegated = std::env::var_os("HYDRA_SANDBOX_CGROUP").expect("set HYDRA_SANDBOX_CGROUP");
        let helper = std::env::var_os("HYDRA_SANDBOX_HELPER").expect("set HYDRA_SANDBOX_HELPER");
        let target: SocketAddr = std::env::var("HYDRA_SANDBOX_TARGET")
            .expect("set HYDRA_SANDBOX_TARGET to a non-loopback TCP echo endpoint")
            .parse()
            .unwrap();
        // Same 62-byte Unix socket budget as the upload fixture: keep the operator root short.
        let root =
            std::env::temp_dir().join(format!("hs-{:016x}", uuid::Uuid::new_v4().as_u64_pair().1));
        assert!(
            root.as_os_str().len() <= 62,
            "fixture root exceeds the 62-byte Unix socket budget: {root:?}"
        );
        std::fs::create_dir(&root).unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let source = root.join("source");
        std::fs::create_dir(&source).unwrap();
        let config = Config {
            delegated_cgroup: delegated.into(),
            runtime_dir: root.clone(),
            bwrap: "/usr/bin/bwrap".into(),
            ip: "/usr/bin/ip".into(),
            uv: "/usr/bin/uv".into(),
            helper: helper.into(),
            allowlist: vec![target],
        };
        let sandbox = Sandbox::new(config).unwrap();
        let mut job = Job {
            source_dir: source.clone(),
            entrypoint: "run.py".into(),
            runtime: Runtime::Python,
            python_packages: vec![],
            python_environment_key: "test".into(),
            target,
            endpoints: vec![Endpoint {
                id: "test".into(),
                address: target,
                transport: "tcp".into(),
            }],
            timeout: Duration::from_secs(10),
            memory_bytes: 128 * 1024 * 1024,
            cpu_millis_per_second: 500,
            pids: 32,
        };
        std::fs::write(
            source.join("run.py"),
            r#"import os, socket
assert 'HYDRA_SANDBOX_CGROUP' not in os.environ
assert not os.path.exists('/broker')
assert not os.path.exists('/sys/fs/cgroup')
assert not os.path.exists('/home')
s = socket.create_connection((os.environ['TARGET_HOST'], int(os.environ['TARGET_PORT'])), 3)
s.sendall(b'hydra-isolated')
assert s.recv(14) == b'hydra-isolated'
s.close()
try:
    socket.create_connection(('198.51.100.254', 4444), .2)
except OSError:
    pass
else:
    raise AssertionError('non-allowlisted network reachable')
print('REAL_SANDBOX_OK')
"#,
        )
        .unwrap();
        let result = sandbox
            .run(job.clone(), CancellationToken::new())
            .await
            .expect("real sandbox prerequisites must work, no fallback");
        assert!(result.success, "{:?}", result);
        assert!(String::from_utf8_lossy(&result.stdout).contains("REAL_SANDBOX_OK"));
        std::fs::write(source.join("run.sh"), "printf SHELL_ISOLATED_OK\n").unwrap();
        job.entrypoint = "run.sh".into();
        job.runtime = Runtime::Shell;
        let shell = sandbox
            .run(job.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert!(shell.success, "{:?}", shell);
        assert!(String::from_utf8_lossy(&shell.stdout).contains("SHELL_ISOLATED_OK"));
        std::fs::copy("/usr/bin/true", source.join("run-elf")).unwrap();
        job.entrypoint = "run-elf".into();
        job.runtime = Runtime::Elf;
        let elf = sandbox
            .run(job.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert!(elf.success, "{:?}", elf);
        job.entrypoint = "run.py".into();
        job.runtime = Runtime::Python;
        std::fs::write(
            source.join("run.py"),
            "import os,time\nif os.fork() == 0: time.sleep(300)\ntime.sleep(300)\n",
        )
        .unwrap();
        job.timeout = Duration::from_secs(1);
        let result = sandbox
            .run(job.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert!(result.timed_out);
        job.timeout = Duration::from_secs(10);
        let cancellation = CancellationToken::new();
        let trigger = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            trigger.cancel();
        });
        assert!(
            sandbox
                .run(job.clone(), cancellation)
                .await
                .unwrap()
                .cancelled
        );
        std::fs::write(source.join("run.py"), "import os,time\ncount=0\nwhile True:\n try:\n  pid=os.fork()\n except BlockingIOError:\n  print('PID_LIMIT_OK',count,flush=True); break\n if pid==0: time.sleep(300); os._exit(0)\n count+=1\n if count>256: raise AssertionError('pids.max ineffective')\n").unwrap();
        let pids = sandbox
            .run(job.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert!(pids.success, "{:?}", pids);
        assert!(String::from_utf8_lossy(&pids.stdout).contains("PID_LIMIT_OK"));
        std::fs::write(source.join("run.py"), "import time\nstart=time.monotonic(); cpu=time.process_time()\nwhile time.monotonic()-start < 1.2: pass\nwall=time.monotonic()-start; used=time.process_time()-cpu\nprint('CPU_MEASUREMENT',used,wall,flush=True)\nassert used < wall*.9, (used,wall)\n").unwrap();
        let cpu = sandbox
            .run(job.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert!(cpu.success, "{:?}", cpu);
        eprintln!("{}", String::from_utf8_lossy(&cpu.stdout));
        std::fs::write(
            source.join("run.py"),
            "x=[]\nwhile True: x.append(bytearray(1024*1024))\n",
        )
        .unwrap();
        let result = sandbox.run(job, CancellationToken::new()).await.unwrap();
        assert!(
            !result.success && !result.timed_out,
            "memory exhaustion must terminate within cgroup"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
