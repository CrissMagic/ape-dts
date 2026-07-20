# Linux binary packaging and deployment

APE DTS currently packages Linux binaries only. Windows and macOS are not binary targets.

## Build a package

The build host requires Rust, a running Docker daemon, and the pinned `cross` revision:

```bash
cargo install cross --git https://github.com/cross-rs/cross --rev 1d07d3f9cc465c435256f1aabc1d18024517891a --locked
```

The Bash packaging entry point also requires `tar` and either `sha256sum` or `shasum`. The scripts fail when a dependency is missing and do not install host dependencies.

| Script target | Binary target | Minimum runtime |
| --- | --- | --- |
| `centos7-x86_64` | Linux x86_64 | glibc 2.17 (CentOS/RHEL 7) |
| `linux-x86_64` | Linux x86_64 | glibc 2.31 |
| `linux-arm64` | Linux ARM64 | glibc 2.31 |

PowerShell build host:

```powershell
.\scripts\package.ps1 centos7-x86_64
.\scripts\package.ps1 linux-x86_64
.\scripts\package.ps1 linux-arm64
```

Bash build host:

```bash
bash ./scripts/package.sh centos7-x86_64
bash ./scripts/package.sh linux-x86_64
bash ./scripts/package.sh linux-arm64
```

An optional second argument changes the output directory; a relative path is resolved from the repository root. Each target uses an isolated Cargo target directory so objects built against different glibc baselines are not reused.

By default, the scripts write these files to `dist/`:

- `ape-dts-<version>-<target>.tar.gz`
- `ape-dts-<version>-<target>.tar.gz.sha256`

The archive contains `ape-dts`, `ape-dts.sh`, and `log4rs.yaml`. Task configuration is deployment-specific and is not included.

## Deploy to CentOS/RHEL 7

Upload the `centos7-x86_64` archive, its checksum, and the task configuration. Replace the example version with the generated filename:

```bash
PACKAGE=ape-dts-2.0.26-centos7-x86_64.tar.gz
sha256sum -c "${PACKAGE}.sha256"
mkdir -p /opt/ape-dts/2.0.26
tar -xzf "${PACKAGE}" -C /opt/ape-dts/2.0.26
cd /opt/ape-dts/2.0.26
./ape-dts --version
```

See the [Kafka task template](../templates/rdb_to_kafka.md) for a Kafka -> Kafka configuration.

## Manage a task

The control script requires Bash and `flock` from `util-linux`. CentOS/RHEL 7 includes both.

```bash
./ape-dts.sh start  [config-file] [log-directory]
./ape-dts.sh stop   [log-directory]
./ape-dts.sh status [log-directory]
```

For example:

```bash
./ape-dts.sh start /absolute/path/to/task_config.ini /var/log/ape-dts/task-name
./ape-dts.sh status /var/log/ape-dts/task-name
./ape-dts.sh stop /var/log/ape-dts/task-name
```

The default configuration is `task_config.ini`, and the default log directory is `logs`; both are resolved from the extracted directory. Relative configuration paths are resolved the same way, so `log4rs_file=./log4rs.yaml` works without changing the working directory.

`start` uses `nohup` and writes console output to `console.log`. The log directory also contains `ape-dts.pid`. Application rolling logs still follow `[runtime].log_dir`; keep it equal to the control-script log directory and use a different directory for each task.

`start` and `stop` use a non-blocking lock on the log directory. If another `start` or `stop` is already running for that directory, the new command fails immediately with exit code `1`; it is not queued. `stop` sends `SIGINT` and waits up to 30 seconds. A timeout returns `1` and does not send `SIGKILL`. Stopping an already stopped task returns `0`.

`status` returns:

| Exit code | Meaning |
| --- | --- |
| `0` | the package's `ape-dts` process is running |
| `3` | no running process was found |
| `1` | the PID file points to another process |
| `2` | invalid command arguments |
