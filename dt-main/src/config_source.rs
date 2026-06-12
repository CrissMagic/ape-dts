use std::{
    env, fs,
    io::Write,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context};
use clap::ValueEnum;
use configparser::ini::Ini;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

pub const DEFAULT_NACOS_GROUP: &str = "DEFAULT_GROUP";

const ENV_NACOS_CACHE_DIR: &str = "NACOS_CACHE_DIR";
const ENV_NACOS_CACHE_TTL_SECS: &str = "NACOS_CACHE_TTL_SECS";
const ENV_NACOS_REQUEST_TIMEOUT_SECS: &str = "NACOS_REQUEST_TIMEOUT_SECS";
const DEFAULT_CACHE_TTL_SECS: u64 = 300;
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
    "metacenter",
    "metrics",
    "precheck",
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

struct CachedConfig {
    content: String,
    expired: bool,
}

impl NacosConfig {
    pub fn new(address: &str, data_id: &str, group: &str) -> anyhow::Result<Self> {
        if address.trim().is_empty() {
            return Err(anyhow!(
                "--nacos-address is required when --config-source=nacos"
            ));
        }
        if data_id.trim().is_empty() {
            return Err(anyhow!(
                "--nacos-dataid is required when --config-source=nacos"
            ));
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
    let cached = load_cache(config);
    match fetch_nacos(config).await {
        Ok(fresh) => match prepare_config(&fresh, &validate_config) {
            Ok(filtered) => {
                if let Err(err) = save_cache(config, &filtered) {
                    eprintln!("warn: save nacos cache failed: {err}");
                }
                Ok(filtered)
            }
            Err(err) => load_cached_config(cached, err, &validate_config),
        },
        Err(err) => load_cached_config(cached, err, &validate_config),
    }
}

fn cache_dir() -> PathBuf {
    env::var(ENV_NACOS_CACHE_DIR)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(".nacos_cache"))
}

fn cache_ttl() -> Duration {
    let secs = env::var(ENV_NACOS_CACHE_TTL_SECS)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CACHE_TTL_SECS);
    Duration::from_secs(secs)
}

fn request_timeout() -> Duration {
    let secs = env::var(ENV_NACOS_REQUEST_TIMEOUT_SECS)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_REQUEST_TIMEOUT_SECS);
    Duration::from_secs(secs)
}

fn encode_cache_component(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn cache_key(config: &NacosConfig) -> String {
    format!(
        "nacos--{}--{}--{}",
        encode_cache_component(&config.address),
        encode_cache_component(&config.data_id),
        encode_cache_component(&config.group)
    )
}

fn load_cache(config: &NacosConfig) -> Option<CachedConfig> {
    let path = cache_dir().join(cache_key(config));
    let meta = fs::metadata(&path).ok()?;
    let modified = meta.modified().ok()?;
    let expired = SystemTime::now()
        .duration_since(modified)
        .map(|age| age > cache_ttl())
        .unwrap_or(false);
    let content = fs::read_to_string(&path).ok()?;
    Some(CachedConfig { content, expired })
}

fn save_cache(config: &NacosConfig, content: &str) -> anyhow::Result<()> {
    let dir = cache_dir();
    fs::create_dir_all(&dir).context("failed to create nacos cache dir")?;
    let key = cache_key(config);
    let path = dir.join(&key);
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let tmp_path = dir.join(format!("{key}.{}.tmp", suffix));
    let mut file = fs::File::create(&tmp_path).context("failed to create nacos cache file")?;
    if let Err(err) = file.write_all(content.as_bytes()) {
        let _ = fs::remove_file(&tmp_path);
        return Err(err).context("failed to write nacos cache file");
    }
    if let Err(err) = file.sync_all() {
        let _ = fs::remove_file(&tmp_path);
        return Err(err).context("failed to sync nacos cache file");
    }
    drop(file);
    if path.exists() {
        fs::remove_file(&path).context("failed to replace nacos cache file")?;
    }
    if let Err(err) = fs::rename(&tmp_path, &path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(err).context("failed to move nacos cache file into place");
    }
    Ok(())
}

fn prepare_config<F>(content: &str, validate_config: &F) -> anyhow::Result<String>
where
    F: Fn(&str) -> anyhow::Result<()>,
{
    let filtered = filter_config_sections(content)?;
    validate_config(&filtered).context("nacos config is not a valid task config")?;
    Ok(filtered)
}

fn load_cached_config<F>(
    cached: Option<CachedConfig>,
    cause: anyhow::Error,
    validate_config: &F,
) -> anyhow::Result<String>
where
    F: Fn(&str) -> anyhow::Result<()>,
{
    let Some(cached) = cached else {
        return Err(cause);
    };
    eprintln!(
        "warn: fetch or validate nacos failed ({}), using {}cached config.",
        cause,
        if cached.expired { "expired " } else { "" },
    );
    prepare_config(&cached.content, validate_config)
        .with_context(|| format!("cached nacos config is invalid after nacos failure: {cause}"))
}

fn filter_config_sections(content: &str) -> anyhow::Result<String> {
    let mut ini = Ini::new();
    ini.set_inline_comment_symbols(Some(&Vec::new()));
    ini.read(content.to_string()).map_err(|e| anyhow!(e))?;

    let mut out = String::new();
    for section in ALLOWED_SECTIONS {
        if let Some(props) = ini.get_map_ref().get(*section) {
            out.push_str(&format!("[{}]\n", section));
            for (key, value) in props {
                out.push_str(&format!("{}={}\n", key, value.as_deref().unwrap_or("")));
            }
            out.push('\n');
        }
    }
    Ok(out)
}

async fn fetch_nacos(config: &NacosConfig) -> anyhow::Result<String> {
    let url = format!(
        "{}/nacos/v1/cs/configs?dataId={}&group={}",
        config.address.trim_end_matches('/'),
        utf8_percent_encode(&config.data_id, NON_ALPHANUMERIC),
        utf8_percent_encode(&config.group, NON_ALPHANUMERIC),
    );
    let response = reqwest::Client::builder()
        .timeout(request_timeout())
        .build()
        .context("failed to build nacos http client")?
        .get(&url)
        .send()
        .await
        .context("failed to request nacos")?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "nacos returned non-success status: {}",
            response.status()
        ));
    }
    response
        .text()
        .await
        .context("failed to read nacos response body")
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        io::{Read, Write as _},
        net::TcpListener,
        path::PathBuf,
        sync::{Mutex, MutexGuard},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use anyhow::anyhow;

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        _lock: MutexGuard<'static, ()>,
        cache_dir: PathBuf,
    }

    impl EnvGuard {
        fn new() -> Self {
            let lock = ENV_LOCK.lock().unwrap();
            let suffix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let cache_dir = env::temp_dir().join(format!(
                "ape-dts-nacos-cache-{}-{}",
                std::process::id(),
                suffix
            ));
            unsafe {
                env::set_var(ENV_NACOS_CACHE_DIR, &cache_dir);
                env::set_var(ENV_NACOS_REQUEST_TIMEOUT_SECS, "1");
                env::remove_var(ENV_NACOS_CACHE_TTL_SECS);
            }
            Self {
                _lock: lock,
                cache_dir,
            }
        }

        fn cache_path(&self, config: &NacosConfig) -> PathBuf {
            self.cache_dir.join(cache_key(config))
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                env::remove_var(ENV_NACOS_CACHE_DIR);
                env::remove_var(ENV_NACOS_REQUEST_TIMEOUT_SECS);
                env::remove_var(ENV_NACOS_CACHE_TTL_SECS);
            }
            let _ = fs::remove_dir_all(&self.cache_dir);
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
    fn cache_key_uses_encoded_components_without_collisions() {
        let left = NacosConfig::new("http://nacos/a", "b", "c").unwrap();
        let right = NacosConfig::new("http://nacos", "a", "b_c").unwrap();

        assert_ne!(cache_key(&left), cache_key(&right));
        assert!(cache_key(&left).contains("http%3A%2F%2Fnacos%2Fa"));
        assert!(cache_key(&right).contains("b%5Fc"));
    }

    #[test]
    fn request_timeout_uses_positive_env_override() {
        let _env = EnvGuard::new();

        unsafe {
            env::set_var(ENV_NACOS_REQUEST_TIMEOUT_SECS, "2");
        }
        assert_eq!(request_timeout(), Duration::from_secs(2));

        unsafe {
            env::set_var(ENV_NACOS_REQUEST_TIMEOUT_SECS, "0");
        }
        assert_eq!(
            request_timeout(),
            Duration::from_secs(DEFAULT_REQUEST_TIMEOUT_SECS)
        );
    }

    #[test]
    fn filter_config_sections_keeps_allowed_sections_only() {
        let filtered = filter_config_sections(
            r#"[extractor]
db_type=mysql

[ignored]
foo=bar

[router]
topic_map=*.*:topic_a
"#,
        )
        .unwrap();

        assert!(filtered.contains("[extractor]"));
        assert!(filtered.contains("db_type=mysql"));
        assert!(filtered.contains("[router]"));
        assert!(!filtered.contains("[ignored]"));
        assert!(!filtered.contains("foo=bar"));
    }

    #[tokio::test]
    async fn load_nacos_config_saves_filtered_valid_config() {
        let env = EnvGuard::new();
        let address = start_http_server(
            "200 OK",
            r#"[extractor]
db_type=mysql

[ignored]
foo=bar
"#,
        );
        let config = NacosConfig::new(&address, "task.ini", DEFAULT_NACOS_GROUP).unwrap();

        let loaded = load_nacos_config_string(&config, |_| Ok(())).await.unwrap();

        assert!(loaded.contains("[extractor]"));
        assert!(!loaded.contains("[ignored]"));
        assert_eq!(fs::read_to_string(env.cache_path(&config)).unwrap(), loaded);
    }

    #[tokio::test]
    async fn load_nacos_config_uses_cache_when_fetch_fails() {
        let env = EnvGuard::new();
        let config =
            NacosConfig::new("http://127.0.0.1:1", "task.ini", DEFAULT_NACOS_GROUP).unwrap();
        save_cache(&config, "[extractor]\ndb_type=mysql\n").unwrap();

        let loaded = load_nacos_config_string(&config, |_| Ok(())).await.unwrap();

        assert!(loaded.contains("db_type=mysql"));
        assert_eq!(
            fs::read_to_string(env.cache_path(&config)).unwrap(),
            "[extractor]\ndb_type=mysql\n"
        );
    }

    #[tokio::test]
    async fn invalid_fresh_config_does_not_replace_cache() {
        let env = EnvGuard::new();
        let address = start_http_server("200 OK", "[extractor]\ndb_type=bad\n");
        let config = NacosConfig::new(&address, "task.ini", DEFAULT_NACOS_GROUP).unwrap();
        save_cache(&config, "[extractor]\ndb_type=good\n").unwrap();

        let loaded = load_nacos_config_string(&config, |filtered| {
            if filtered.contains("db_type=bad") {
                Err(anyhow!("bad config"))
            } else {
                Ok(())
            }
        })
        .await
        .unwrap();

        let cached = fs::read_to_string(env.cache_path(&config)).unwrap();
        assert!(loaded.contains("db_type=good"));
        assert!(cached.contains("db_type=good"));
        assert!(!cached.contains("db_type=bad"));
    }
}
