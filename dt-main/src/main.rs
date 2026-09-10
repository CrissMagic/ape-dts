use std::{
    backtrace::Backtrace,
    env, panic,
    process::{exit, ExitCode},
    time::Duration,
};

use clap::Parser;
use config_source::{ConfigSourceKind, NacosConfig};
use dt_common::{
    config::{ini_loader::IniLoader, task_config::TaskConfig},
    error::{DtError, DtResultExt, ErrorReport, Stage},
    log_error, log_error_report,
    logger::TaskLogger,
};
use dt_precheck::{config::task_config::PrecheckTaskConfig, do_precheck};
use dt_task::task_runner::TaskRunner;
use tokio::{signal::ctrl_c, spawn, time::sleep};

mod config_source;

const ENV_SHUTDOWN_TIMEOUT_SECS: &str = "SHUTDOWN_TIMEOUT_SECS";

#[derive(Debug, Parser)]
struct Args {
    #[arg(short = 'v', long = "version", alias = "versions")]
    version: bool,

    #[arg(short, long, value_name = "CONFIG", conflicts_with = "legacy_config")]
    config: Option<String>,

    #[arg(
        long = "config-path",
        value_name = "CONFIG",
        conflicts_with_all = ["config", "legacy_config"]
    )]
    config_path: Option<String>,

    #[arg(long = "config-source", value_enum, default_value = "local")]
    config_source: ConfigSourceKind,

    #[arg(long = "nacos-address")]
    nacos_address: Option<String>,

    #[arg(long = "nacos-dataid", alias = "nacos-data-id")]
    nacos_dataid: Option<String>,

    #[arg(long = "nacos-group", default_value = config_source::DEFAULT_NACOS_GROUP)]
    nacos_group: String,

    #[arg(value_name = "CONFIG")]
    legacy_config: Option<String>,

    #[arg(long)]
    init: bool,
}

impl Args {
    fn config_path(&self) -> Option<&str> {
        self.config_path
            .as_deref()
            .or(self.config.as_deref())
            .or(self.legacy_config.as_deref())
            .filter(|config| !config.is_empty())
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.version || matches!(self.legacy_config.as_deref(), Some("version")) {
            return Ok(());
        }

        match self.config_source {
            ConfigSourceKind::Local => {
                if self.config_path().is_none() {
                    anyhow::bail!(
                        "--config-path, --config, or positional CONFIG is required when --config-source=local"
                    );
                }
            }
            ConfigSourceKind::Nacos => {
                if self.config_path().is_some() {
                    anyhow::bail!(
                        "local CONFIG arguments can not be used when --config-source=nacos"
                    );
                }
                NacosConfig::new(
                    self.nacos_address.as_deref().unwrap_or_default(),
                    self.nacos_dataid.as_deref().unwrap_or_default(),
                    &self.nacos_group,
                )?;
            }
        }
        Ok(())
    }

    fn nacos_config(&self) -> anyhow::Result<NacosConfig> {
        NacosConfig::new(
            self.nacos_address.as_deref().unwrap_or_default(),
            self.nacos_dataid.as_deref().unwrap_or_default(),
            &self.nacos_group,
        )
    }
}

fn validate_config_str(config: &str) -> anyhow::Result<()> {
    let loader = IniLoader::new_from_str(config)?;
    parse_task_configs(&loader).map(|_| ())
}

fn parse_task_configs(
    loader: &IniLoader,
) -> anyhow::Result<(TaskConfig, Option<PrecheckTaskConfig>)> {
    let task_config = TaskConfig::from_loader(loader)?;
    let precheck_config = PrecheckTaskConfig::load_if_present(loader)?;
    Ok((task_config, precheck_config))
}

async fn load_config(args: &Args) -> anyhow::Result<(TaskConfig, Option<PrecheckTaskConfig>)> {
    let loader = match args.config_source {
        ConfigSourceKind::Local => {
            let config_path = args.config_path().ok_or_else(|| {
                DtError::MissingConfig(
                    "--config-path, --config, or positional CONFIG is required".to_string(),
                )
            })?;
            IniLoader::new(config_path)?
        }
        ConfigSourceKind::Nacos => {
            let nacos_config = args.nacos_config()?;
            let config =
                config_source::load_nacos_config_string(&nacos_config, validate_config_str).await?;
            IniLoader::new_from_str(&config)?
        }
    };
    parse_task_configs(&loader)
}

#[tokio::main]
async fn main() -> ExitCode {
    unsafe {
        env::set_var("RUST_BACKTRACE", "1");
    }

    let args = Args::parse();
    if let Err(err) = args.validate() {
        eprintln!("{err}");
        return ExitCode::from(2);
    }
    if args.version || matches!(args.legacy_config.as_deref(), Some("version")) {
        println!("dt-main {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }

    install_panic_hook();

    match execute(&args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let report = ErrorReport::from_anyhow(&error);
            eprintln!("{report}");
            log_error!("{report}");
            log_error_report!("{}", report.to_log_json());
            log::logger().flush();
            ExitCode::FAILURE
        }
    }
}

async fn execute(args: &Args) -> anyhow::Result<()> {
    let (task_config, precheck_config) = load_config(args).await?;
    TaskLogger::new(&task_config)
        .init(precheck_config.is_some())
        .await?;
    run(task_config, precheck_config, args.init).await
}

async fn run(
    task_config: TaskConfig,
    precheck_config: Option<PrecheckTaskConfig>,
    init: bool,
) -> anyhow::Result<()> {
    let task_id = task_config.global.task_id.clone();

    spawn(async {
        if ctrl_c().await.is_err() {
            return;
        }
        sleep(Duration::from_secs(
            env::var(ENV_SHUTDOWN_TIMEOUT_SECS)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
        ))
        .await;
        exit(0);
    });

    run_config(task_config, precheck_config, init)
        .await
        .task_id(task_id)
}

async fn run_config(
    task_config: TaskConfig,
    precheck_config: Option<PrecheckTaskConfig>,
    init: bool,
) -> anyhow::Result<()> {
    match precheck_config {
        Some(precheck_config) => do_precheck(task_config, precheck_config)
            .await
            .stage(Stage::Precheck)?,
        None => {
            TaskRunner::new(task_config)
                .stage(Stage::Bootstrap)?
                .start_task(init)
                .await
                .stage(Stage::Task)?;
        }
    }
    Ok(())
}

fn install_panic_hook() {
    panic::set_hook(Box::new(|panic_info| {
        let backtrace = Backtrace::capture();
        let report = format!("panic: {panic_info}\nbacktrace:\n{backtrace}");
        eprintln!("{report}");
        log_error!("{report}");
        log::logger().flush();
        // A panic in a spawned task or worker thread would otherwise leave the process
        // hanging, and the task would never be reported as failed.
        exit(1);
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_config_flag() {
        let args = Args::try_parse_from(["dt-main", "--config", "task_config.ini"]).unwrap();
        assert_eq!(args.config_path(), Some("task_config.ini"));
    }

    #[test]
    fn accepts_legacy_positional_config() {
        let args = Args::try_parse_from(["dt-main", "task_config.ini"]).unwrap();
        assert_eq!(args.config_path(), Some("task_config.ini"));
    }

    #[test]
    fn accepts_config_path_flag() {
        let args = Args::try_parse_from([
            "dt-main",
            "--config-source",
            "local",
            "--config-path",
            "task_config.ini",
        ])
        .unwrap();
        assert_eq!(args.config_path(), Some("task_config.ini"));
        assert!(args.validate().is_ok());
    }

    #[test]
    fn accepts_nacos_config_source() {
        let args = Args::try_parse_from([
            "dt-main",
            "--config-source",
            "nacos",
            "--nacos-address",
            "http://nacos:8848",
            "--nacos-dataid",
            "task_pg.ini",
        ])
        .unwrap();
        assert_eq!(args.config_source, ConfigSourceKind::Nacos);
        assert!(args.config_path().is_none());
        assert!(args.validate().is_ok());
    }

    #[test]
    fn rejects_local_config_for_nacos_source() {
        let args = Args::try_parse_from([
            "dt-main",
            "--config-source",
            "nacos",
            "--config-path",
            "task_config.ini",
            "--nacos-address",
            "http://nacos:8848",
            "--nacos-dataid",
            "task_pg.ini",
        ])
        .unwrap();
        assert!(args.validate().is_err());
    }

    #[test]
    fn version_does_not_require_config() {
        let args = Args::try_parse_from(["dt-main", "--version"]).unwrap();
        assert!(args.version);
        assert_eq!(args.config_path(), None);
        assert!(args.validate().is_ok());
    }

    #[test]
    fn accepts_legacy_version_command() {
        let args = Args::try_parse_from(["dt-main", "version"]).unwrap();
        assert_eq!(args.legacy_config.as_deref(), Some("version"));
    }

    #[test]
    fn rejects_config_flag_and_positional_config_together() {
        let err =
            Args::try_parse_from(["dt-main", "--config", "new.ini", "legacy.ini"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn rejects_config_path_and_config_flag_together() {
        let err = Args::try_parse_from([
            "dt-main",
            "--config-path",
            "new.ini",
            "--config",
            "legacy.ini",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn rejects_precheck_config_without_complete_task_config() {
        let config = "[precheck]\ndo_struct_init=false\ndo_cdc=false\n";

        assert!(PrecheckTaskConfig::new_from_str(config).is_ok());
        assert!(validate_config_str(config).is_err());
    }
}
