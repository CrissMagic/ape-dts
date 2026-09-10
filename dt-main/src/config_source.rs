use std::{env, time::Duration};

use anyhow::Context;
use clap::ValueEnum;
use configparser::ini::Ini;
use dt_common::error::DtError;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

pub const DEFAULT_NACOS_GROUP: &str = "DEFAULT_GROUP";

const ENV_NACOS_REQUEST_TIMEOUT_SECS: &str = "NACOS_REQUEST_TIMEOUT_SECS";
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 10;

const ALLOWED_SECTIONS: &[&str] = &[
    "global",
    "extractor",
    "sinker",
    "pipeline",
    "parallelizer",
    "runtime",
    "filter",
    "router",
    "resumer",
    "data_marker",
    "processor",
    "checker",
    "checker_cdc",
    "checker_output",
    "metacenter",
    "metrics",
    "precheck",
    "tracing",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ConfigSourceKind {
    Local,
    Nacos,
}

pub struct NacosConfig {
    address: String,
    data_id: String,
    group: String,
}

impl NacosConfig {
    pub fn new(address: &str, data_id: &str, group: &str) -> anyhow::Result<Self> {
        if address.trim().is_empty() {
            return Err(DtError::invalid_config(
                "--nacos-address is required when --config-source=nacos",
            )
            .into());
        }
        if data_id.trim().is_empty() {
            return Err(DtError::invalid_config(
                "--nacos-dataid is required when --config-source=nacos",
            )
            .into());
        }

        Ok(Self {
            address: address.trim().to_string(),
            data_id: data_id.trim().to_string(),
            group: group.trim().to_string(),
        })
    }
}

pub async fn load_nacos_config_string<F>(
    config: &NacosConfig,
    validate_config: F,
) -> anyhow::Result<String>
where
    F: Fn(&str) -> anyhow::Result<()>,
{
    let content = fetch_nacos(config).await?;
    prepare_config(&content, &validate_config)
}

fn request_timeout() -> anyhow::Result<Duration> {
    let secs = match env::var(ENV_NACOS_REQUEST_TIMEOUT_SECS) {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|secs| *secs > 0)
            .ok_or_else(|| {
                DtError::invalid_config(format!(
                    "{ENV_NACOS_REQUEST_TIMEOUT_SECS} must be a positive integer"
                ))
            })?,
        Err(env::VarError::NotPresent) => DEFAULT_REQUEST_TIMEOUT_SECS,
        Err(error) => {
            return Err(DtError::invalid_config(format!(
                "failed to read {ENV_NACOS_REQUEST_TIMEOUT_SECS}: {error}"
            ))
            .into());
        }
    };
    Ok(Duration::from_secs(secs))
}

fn prepare_config<F>(content: &str, validate_config: &F) -> anyhow::Result<String>
where
    F: Fn(&str) -> anyhow::Result<()>,
{
    validate_config_sections(content)?;
    validate_config(content).context("nacos config is not a valid task config")?;
    Ok(content.to_string())
}

fn validate_config_sections(content: &str) -> anyhow::Result<()> {
    let mut ini = Ini::new();
    ini.set_inline_comment_symbols(Some(&Vec::new()));
    ini.read(content.to_string()).map_err(|error| {
        DtError::invalid_config(format!("failed to parse Nacos config: {error}"))
    })?;

    let mut unknown_sections = ini
        .sections()
        .into_iter()
        .filter(|section| !ALLOWED_SECTIONS.contains(&section.as_str()))
        .collect::<Vec<_>>();
    unknown_sections.sort();
    if !unknown_sections.is_empty() {
        return Err(DtError::invalid_config(format!(
            "Nacos config contains unknown sections: {}",
            unknown_sections.join(", ")
        ))
        .into());
    }
    Ok(())
}

async fn fetch_nacos(config: &NacosConfig) -> anyhow::Result<String> {
    let url = format!(
        "{}/nacos/v1/cs/configs?dataId={}&group={}",
        config.address.trim_end_matches('/'),
        utf8_percent_encode(&config.data_id, NON_ALPHANUMERIC),
        utf8_percent_encode(&config.group, NON_ALPHANUMERIC),
    );
    let response = reqwest::Client::builder()
        .timeout(request_timeout()?)
        .build()
        .context("failed to build nacos http client")?
        .get(&url)
        .send()
        .await
        .context("failed to request nacos")?;
    if !response.status().is_success() {
        return Err(DtError::HttpRejected {
            status: response.status().as_u16(),
            detail: "Nacos config request was rejected".to_string(),
        }
        .into());
    }
    response
        .text()
        .await
        .context("failed to read nacos response body")
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        io::{Read, Write as _},
        net::TcpListener,
        sync::{Mutex, MutexGuard},
        thread,
        time::Duration,
    };

    use anyhow::anyhow;
    use dt_common::error::{ErrorCode, ErrorReport};

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        _lock: MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn new() -> Self {
            let lock = ENV_LOCK.lock().unwrap();
            unsafe {
                env::set_var(ENV_NACOS_REQUEST_TIMEOUT_SECS, "1");
            }
            Self { _lock: lock }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                env::remove_var(ENV_NACOS_REQUEST_TIMEOUT_SECS);
            }
        }
    }

    fn start_http_server(status: &'static str, body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0_u8; 1024];
                let _ = stream.read(&mut buf);
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        format!("http://{address}")
    }

    #[test]
    fn request_timeout_uses_default_and_positive_env_override() {
        let _env = EnvGuard::new();

        unsafe {
            env::remove_var(ENV_NACOS_REQUEST_TIMEOUT_SECS);
        }
        assert_eq!(
            request_timeout().unwrap(),
            Duration::from_secs(DEFAULT_REQUEST_TIMEOUT_SECS)
        );

        unsafe {
            env::set_var(ENV_NACOS_REQUEST_TIMEOUT_SECS, "2");
        }
        assert_eq!(request_timeout().unwrap(), Duration::from_secs(2));
    }

    #[test]
    fn request_timeout_rejects_invalid_override() {
        let _env = EnvGuard::new();

        for value in ["0", "invalid"] {
            unsafe {
                env::set_var(ENV_NACOS_REQUEST_TIMEOUT_SECS, value);
            }
            let error = request_timeout().unwrap_err();

            assert_eq!(
                ErrorReport::from_anyhow(&error).code,
                ErrorCode::InvalidConfig
            );
            assert_eq!(
                error.root_cause().to_string(),
                "NACOS_REQUEST_TIMEOUT_SECS must be a positive integer"
            );
        }
    }

    #[test]
    fn validate_config_sections_accepts_all_current_sections() {
        validate_config_sections(
            r#"[extractor]
db_type=mysql

[checker_cdc]
is_enabled=true

[checker_output]
output_type=logs

[tracing]
enabled=true

[router]
topic_map=*.*:topic_a
"#,
        )
        .unwrap();
    }

    #[test]
    fn validate_config_sections_rejects_unknown_sections() {
        let error = validate_config_sections(
            r#"[extractor]
db_type=mysql

[unknown_section]
foo=bar
"#,
        )
        .expect_err("unknown Nacos sections must fail");

        assert_eq!(
            ErrorReport::from_anyhow(&error).code,
            ErrorCode::InvalidConfig
        );
        assert_eq!(
            error.root_cause().to_string(),
            "Nacos config contains unknown sections: unknown_section"
        );
    }

    #[tokio::test]
    async fn load_nacos_config_preserves_valid_content() {
        let _env = EnvGuard::new();
        let address = start_http_server(
            "200 OK",
            r#"[extractor]
db_type=mysql

[router]
topic_map=*.*:topic_a
"#,
        );
        let config = NacosConfig::new(&address, "task.ini", DEFAULT_NACOS_GROUP).unwrap();

        let loaded = load_nacos_config_string(&config, |_| Ok(())).await.unwrap();

        assert_eq!(
            loaded,
            "[extractor]\ndb_type=mysql\n\n[router]\ntopic_map=*.*:topic_a\n"
        );
    }

    #[tokio::test]
    async fn load_nacos_config_fails_when_fetch_fails() {
        let _env = EnvGuard::new();
        let config =
            NacosConfig::new("http://127.0.0.1:1", "task.ini", DEFAULT_NACOS_GROUP).unwrap();

        load_nacos_config_string(&config, |_| Ok(()))
            .await
            .expect_err("Nacos fetch failure must not fall back to cached content");
    }

    #[tokio::test]
    async fn load_nacos_config_fails_when_validation_fails() {
        let _env = EnvGuard::new();
        let address = start_http_server("200 OK", "[extractor]\ndb_type=bad\n");
        let config = NacosConfig::new(&address, "task.ini", DEFAULT_NACOS_GROUP).unwrap();

        let error = load_nacos_config_string(&config, |content| {
            if content.contains("db_type=bad") {
                Err(anyhow!("bad config"))
            } else {
                Ok(())
            }
        })
        .await
        .expect_err("invalid Nacos config must fail without a fallback");

        assert_eq!(error.to_string(), "nacos config is not a valid task config");
        assert_eq!(error.root_cause().to_string(), "bad config");
    }

    #[tokio::test]
    async fn load_nacos_config_reports_non_success_http_status() {
        let _env = EnvGuard::new();
        let address = start_http_server("503 Service Unavailable", "unavailable");
        let config = NacosConfig::new(&address, "task.ini", DEFAULT_NACOS_GROUP).unwrap();

        let error = load_nacos_config_string(&config, |_| Ok(()))
            .await
            .expect_err("non-success Nacos response must fail");

        assert_eq!(
            error.root_cause().to_string(),
            "HTTP request was rejected with status 503: Nacos config request was rejected"
        );
    }
}
