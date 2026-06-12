use std::env;

use clap::Parser;

use config_source::{ConfigSourceKind, NacosConfig};
use dt_precheck::{
    config::task_config::PrecheckTaskConfig, do_precheck, do_precheck_with_config_str,
};
use dt_task::task_runner::TaskRunner;

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
    if PrecheckTaskConfig::new_from_str(config).is_ok() {
        return Ok(());
    }
    TaskRunner::new_from_str(config).map(|_| ())
}

#[tokio::main]
async fn main() {
    unsafe {
        env::set_var("RUST_BACKTRACE", "1");
    }

    let args = Args::parse();
    if let Err(err) = args.validate() {
        eprintln!("{err}");
        std::process::exit(2);
    }
    if args.version || matches!(args.legacy_config.as_deref(), Some("version")) {
        println!("dt-main {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    tokio::spawn(async {
        tokio::signal::ctrl_c().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(
            std::env::var(ENV_SHUTDOWN_TIMEOUT_SECS)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
        ))
        .await;
        std::process::exit(0);
    });

    match args.config_source {
        ConfigSourceKind::Local => {
            let config = args.config_path().unwrap();
            if PrecheckTaskConfig::new(config).is_ok() {
                do_precheck(config).await;
            } else {
                let runner = TaskRunner::new(config).unwrap();
                runner.start_task(args.init).await.unwrap()
            }
        }
        ConfigSourceKind::Nacos => {
            let nacos_config = args.nacos_config().unwrap();
            let config =
                config_source::load_nacos_config_string(&nacos_config, validate_config_str)
                    .await
                    .unwrap();
            if PrecheckTaskConfig::new_from_str(&config).is_ok() {
                do_precheck_with_config_str(&config).await;
            } else {
                let runner = TaskRunner::new_from_str(&config).unwrap();
                runner.start_task(args.init).await.unwrap()
            }
        }
    }
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
}
