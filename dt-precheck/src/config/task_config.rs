use anyhow::bail;
use configparser::ini::Ini;
use dt_common::{config::ini_loader::IniLoader, error::Error};

use super::precheck_config::PrecheckConfig;

const PRECHECK: &str = "precheck";

pub struct PrecheckTaskConfig {
    pub precheck: PrecheckConfig,
}

impl PrecheckTaskConfig {
    pub fn new(task_config_file: &str) -> anyhow::Result<Self> {
        let ini = IniLoader::new(task_config_file).ini;
        let precheck_config = Self::load_precheck_config(&ini)?;
        Ok(Self {
            precheck: precheck_config,
        })
    }

    pub fn new_from_str(config_str: &str) -> anyhow::Result<Self> {
        let ini = IniLoader::new_from_str(config_str).ini;
        let precheck_config = Self::load_precheck_config(&ini)?;
        Ok(Self {
            precheck: precheck_config,
        })
    }

    fn load_precheck_config(ini: &Ini) -> anyhow::Result<PrecheckConfig> {
        let (do_struct_opt, do_cdc_opt): (Option<String>, Option<String>) = (
            ini.get(PRECHECK, "do_struct_init"),
            ini.get(PRECHECK, "do_cdc"),
        );
        if let (Some(do_struct), Some(do_cdc)) = (do_struct_opt, do_cdc_opt) {
            Ok(PrecheckConfig {
                do_struct_init: do_struct.parse().unwrap(),
                do_cdc: do_cdc.parse().unwrap(),
            })
        } else {
            bail! {Error::ConfigError(
                "config is not valid for precheck.".into(),
            )}
        }
    }
}
