use dt_common::config::task_config::TaskConfig;

use crate::{
    builder::prechecker_builder::PrecheckerBuilder, config::task_config::PrecheckTaskConfig,
};

pub mod builder;
pub mod config;
pub mod fetcher;
pub mod meta;
pub mod prechecker;

pub async fn do_precheck(config: &str) {
    let task_config = TaskConfig::new(config).unwrap();
    let precheck_config = PrecheckTaskConfig::new(config).unwrap();

    let checker_connector = PrecheckerBuilder::build(precheck_config.precheck, task_config);
    let result = checker_connector.verify_check_result().await;
    if let Err(e) = result {
        println!("precheck not passed.");
        panic!("precheck meet error: {}", e);
    }

    println!("precheck passed.");
}

pub async fn do_precheck_with_config_str(config_str: &str) {
    let task_config = TaskConfig::new_from_str(config_str).unwrap();
    let precheck_config = PrecheckTaskConfig::new_from_str(config_str).unwrap();

    let checker_connector = PrecheckerBuilder::build(precheck_config.precheck, task_config);
    let result = checker_connector.verify_check_result().await;
    if let Err(e) = result {
        println!("precheck not passed.");
        panic!("precheck meet error: {}", e);
    }

    println!("precheck passed.");
}
