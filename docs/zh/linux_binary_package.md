# Linux 二进制打包与部署

APE DTS 当前只打包 Linux 二进制，不提供 Windows 和 macOS 二进制目标。

## 生成安装包

构建机需要安装 Rust、启动 Docker，并安装固定版本的 `cross`：

```bash
cargo install cross --git https://github.com/cross-rs/cross --rev 1d07d3f9cc465c435256f1aabc1d18024517891a --locked
```

Bash 打包入口还需要 `tar`，以及 `sha256sum` 或 `shasum`。依赖缺失时脚本会明确失败，不会自动安装构建机依赖。

| 脚本目标 | 二进制目标 | 最低运行环境 |
| --- | --- | --- |
| `centos7-x86_64` | Linux x86_64 | glibc 2.17（CentOS/RHEL 7） |
| `linux-x86_64` | Linux x86_64 | glibc 2.31 |
| `linux-arm64` | Linux ARM64 | glibc 2.31 |

PowerShell 构建机：

```powershell
.\scripts\package.ps1 centos7-x86_64
.\scripts\package.ps1 linux-x86_64
.\scripts\package.ps1 linux-arm64
```

Bash 构建机：

```bash
bash ./scripts/package.sh centos7-x86_64
bash ./scripts/package.sh linux-x86_64
bash ./scripts/package.sh linux-arm64
```

第二个参数可指定输出目录；相对路径按仓库根目录解析。每个目标使用独立的 Cargo target 目录，避免复用基于不同 glibc 环境编译的对象文件。

脚本默认在 `dist/` 生成：

- `ape-dts-<version>-<target>.tar.gz`
- `ape-dts-<version>-<target>.tar.gz.sha256`

压缩包内包含 `ape-dts`、`ape-dts.sh` 和 `log4rs.yaml`。任务配置与部署环境相关，不会打入安装包。

## 部署到 CentOS/RHEL 7

上传 `centos7-x86_64` 压缩包、校验文件及任务配置。以下版本号仅为示例，应替换为实际生成的文件名：

```bash
PACKAGE=ape-dts-2.0.26-centos7-x86_64.tar.gz
sha256sum -c "${PACKAGE}.sha256"
mkdir -p /opt/ape-dts/2.0.26
tar -xzf "${PACKAGE}" -C /opt/ape-dts/2.0.26
cd /opt/ape-dts/2.0.26
./ape-dts --version
```

Kafka -> Kafka 配置见 [Kafka 任务模板](../templates/rdb_to_kafka.md)。

## 管理任务

控制脚本依赖 Bash 以及 `util-linux` 提供的 `flock`，CentOS/RHEL 7 默认包含这两个命令。

```bash
./ape-dts.sh start  [config-file] [log-directory]
./ape-dts.sh stop   [log-directory]
./ape-dts.sh status [log-directory]
```

示例：

```bash
./ape-dts.sh start /absolute/path/to/task_config.ini /var/log/ape-dts/task-name
./ape-dts.sh status /var/log/ape-dts/task-name
./ape-dts.sh stop /var/log/ape-dts/task-name
```

配置文件默认是 `task_config.ini`，日志目录默认是 `logs`，都按解压目录解析。配置中的相对路径也按该目录解析，因此 `log4rs_file=./log4rs.yaml` 无需调整工作目录即可生效。

`start` 使用 `nohup` 后台运行，并将控制台输出写入 `console.log`；日志目录还会保存 `ape-dts.pid`。应用滚动日志仍由 `[runtime].log_dir` 控制，应与控制脚本的日志目录保持一致，并为每个任务使用不同目录。

`start` 和 `stop` 对日志目录使用非阻塞锁。同一目录已有 `start` 或 `stop` 正在执行时，新命令会立即失败并返回 `1`，不会排队等待。`stop` 发送 `SIGINT` 并等待最多 30 秒；超时返回 `1`，且不会发送 `SIGKILL`。任务已经停止时再次执行 `stop` 返回 `0`。

`status` 返回码：

| 返回码 | 含义 |
| --- | --- |
| `0` | 当前安装包的 `ape-dts` 进程正在运行 |
| `3` | 未发现运行中的进程 |
| `1` | PID 文件指向其他进程 |
| `2` | 命令参数错误 |
