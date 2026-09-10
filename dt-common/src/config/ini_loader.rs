use std::{
    any::type_name,
    fs::File,
    io::{ErrorKind, Read},
    str::FromStr,
};

use anyhow::{Context, Error};
use configparser::ini::Ini;

use crate::error::DtError;

#[derive(Debug)]
pub struct IniLoader {
    pub ini: Ini,
}

impl IniLoader {
    pub fn new(ini_file: &str) -> anyhow::Result<Self> {
        let mut config_str = String::new();
        File::open(ini_file)
            .map_err(|error| {
                let context = if error.kind() == ErrorKind::NotFound {
                    DtError::MissingConfig(format!("path: {ini_file}"))
                } else {
                    DtError::IoFailed(format!("failed to open config file: {ini_file}"))
                };
                Error::new(error).context(context)
            })?
            .read_to_string(&mut config_str)
            .context(DtError::IoFailed(format!(
                "failed to read config file: {ini_file}"
            )))?;
        Self::new_from_str_with_source(&config_str, &format!("file: {ini_file}"))
    }

    pub fn new_from_str(config_str: &str) -> anyhow::Result<Self> {
        Self::new_from_str_with_source(config_str, "content")
    }

    fn new_from_str_with_source(config_str: &str, source: &str) -> anyhow::Result<Self> {
        let mut ini = Ini::new();
        // allow using comment symbols(; and #) in value
        // E.g. do_dbs=`a;`,`bcd`
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(config_str.to_string())
            .map_err(|_| DtError::InvalidConfig(format!("failed to parse config {source}")))?;
        Ok(Self { ini })
    }

    pub fn get_required<T>(&self, section: &str, key: &str) -> anyhow::Result<T>
    where
        T: FromStr,
    {
        if let Some(value) = self.ini.get(section, key) {
            if !value.is_empty() {
                return Self::parse_value(section, key, &value);
            }
        }
        Err(DtError::MissingConfigItem(format!(
            "config [{section}].{key} does not exist or is empty"
        ))
        .into())
    }

    pub fn get_optional<T>(&self, section: &str, key: &str) -> anyhow::Result<T>
    where
        T: Default,
        T: FromStr,
    {
        self.get_with_default(section, key, T::default())
    }

    pub fn get_with_default<T>(&self, section: &str, key: &str, default: T) -> anyhow::Result<T>
    where
        T: FromStr,
    {
        if let Some(value) = self.ini.get(section, key) {
            if !value.is_empty() {
                return Self::parse_value(section, key, &value);
            }
        }
        Ok(default)
    }

    pub fn contains(&self, section: &str, key: &str) -> bool {
        self.ini.get(section, key).is_some()
    }

    pub fn has_section(&self, section: &str) -> bool {
        self.ini
            .sections()
            .iter()
            .any(|candidate| candidate == section)
    }

    fn parse_value<T>(section: &str, key: &str, value: &str) -> anyhow::Result<T>
    where
        T: FromStr,
    {
        let value = value.parse::<T>().map_err(|_| {
            let rendered_value = if Self::is_sensitive_key(key) {
                "[redacted]"
            } else {
                value
            };
            DtError::InvalidConfig(format!(
                "config [{section}].{key}={rendered_value} can not be parsed as {}",
                type_name::<T>()
            ))
        })?;
        Ok(value)
    }

    fn is_sensitive_key(key: &str) -> bool {
        let key = key.to_ascii_lowercase();
        ["password", "secret", "token"]
            .iter()
            .any(|sensitive| key.contains(sensitive))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ErrorCode, ErrorReport, Stage};

    #[test]
    fn config_errors_do_not_add_metadata_context_to_the_debug_chain() {
        let loader = IniLoader { ini: Ini::new() };
        let error = loader
            .get_required::<usize>("runtime", "worker_threads")
            .unwrap_err();

        assert!(!format!("{error:?}").contains("__APE_DTS_ERROR_CONTEXT__"));
        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.code, ErrorCode::MissingConfigItem);
        assert_eq!(report.stage, Stage::Bootstrap);
        assert_eq!(
            report.details,
            ["config [runtime].worker_threads does not exist or is empty"]
        );
    }

    #[test]
    fn invalid_config_content_returns_typed_error() {
        let error = IniLoader::new_from_str("[unclosed-section")
            .expect_err("invalid INI content must fail");

        let report = ErrorReport::from_anyhow(&error);
        assert_eq!(report.code, ErrorCode::InvalidConfig);
        assert_eq!(report.stage, Stage::Bootstrap);
        assert_eq!(
            error.root_cause().to_string(),
            "failed to parse config content"
        );
    }

    #[test]
    fn empty_section_is_still_detected() {
        let loader = IniLoader::new_from_str("[resumer]\n").unwrap();

        assert!(loader.has_section("resumer"));
    }
}
