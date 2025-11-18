use std::env;

use dt_precheck::{config::task_config::PrecheckTaskConfig, do_precheck, do_precheck_with_config_str};
use dt_task::task_runner::TaskRunner;
use crate::config_source::{CliArgs, ConfigSourceKind, load_config_string};

const ENV_SHUTDOWN_TIMEOUT_SECS: &str = "SHUTDOWN_TIMEOUT_SECS";

#[tokio::main]
async fn main() {
    env::set_var("RUST_BACKTRACE", "1");

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

    let args = CliArgs::parse().expect("invalid startup arguments");
    match args.source {
        ConfigSourceKind::Local => {
            let task_config = args.config_path.clone().expect("--config-path required for local");
            if PrecheckTaskConfig::new(&task_config).is_ok() {
                do_precheck(&task_config).await;
            } else {
                let runner = TaskRunner::new(&task_config).unwrap();
                runner.start_task(true).await.unwrap()
            }
        }
        ConfigSourceKind::Nacos => {
            let config_str = load_config_string(&args).await.expect("failed to load nacos config");
            if PrecheckTaskConfig::new_from_str(&config_str).is_ok() {
                do_precheck_with_config_str(&config_str).await;
            } else {
                let runner = TaskRunner::new_from_str(&config_str).unwrap();
                runner.start_task(true).await.unwrap()
            }
        }
    }
}

mod config_source;
