use std::{collections::HashMap, env, fs, io::Write, path::PathBuf, time::{Duration, SystemTime}};

use anyhow::{anyhow, Context};
use configparser::ini::Ini;

const DEFAULT_GROUP: &str = "DEFAULT_GROUP";
const ENV_NACOS_CACHE_DIR: &str = "NACOS_CACHE_DIR";
const ENV_NACOS_CACHE_TTL_SECS: &str = "NACOS_CACHE_TTL_SECS";

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigSourceKind {
    Local,
    Nacos,
}

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub source: ConfigSourceKind,
    pub config_path: Option<String>,
    pub nacos_address: Option<String>,
    pub nacos_dataid: Option<String>,
    pub nacos_group: String,
}

impl CliArgs {
    pub fn parse() -> anyhow::Result<Self> {
        let mut kv: HashMap<String, String> = HashMap::new();
        let mut positional: Vec<String> = Vec::new();
        let mut args = env::args().skip(1); // skip program name
        while let Some(arg) = args.next() {
            if let Some((k, v)) = arg.split_once('=') {
                if k.starts_with("--") {
                    kv.insert(k.trim_start_matches('-').to_string(), v.to_string());
                } else {
                    positional.push(arg);
                }
            } else if arg.starts_with("--") {
                if let Some(val) = args.next() {
                    kv.insert(arg.trim_start_matches('-').to_string(), val);
                } else {
                    kv.insert(arg.trim_start_matches('-').to_string(), String::new());
                }
            } else {
                positional.push(arg);
            }
        }

        // default source=local
        let source = match kv.get("config-source").map(|s| s.as_str()).unwrap_or("local") {
            "local" => ConfigSourceKind::Local,
            "nacos" => ConfigSourceKind::Nacos,
            other => return Err(anyhow!(format!("--config-source must be 'local' or 'nacos', got '{}'.", other))),
        };

        // legacy single-arg path support
        let config_path = kv.get("config-path").cloned().or_else(|| positional.get(0).cloned());

        let nacos_group = kv
            .get("nacos-group")
            .cloned()
            .unwrap_or_else(|| DEFAULT_GROUP.to_string());

        let nacos_address = kv.get("nacos-address").cloned();
        let nacos_dataid = kv.get("nacos-dataid").cloned();

        let args = Self { source, config_path, nacos_address, nacos_dataid, nacos_group };
        args.validate()?;
        Ok(args)
    }

    fn validate(&self) -> anyhow::Result<()> {
        match self.source {
            ConfigSourceKind::Local => {
                if self.config_path.as_deref().unwrap_or("").is_empty() {
                    return Err(anyhow!("--config-path is required when --config-source=local (or pass a single positional path)."));
                }
            }
            ConfigSourceKind::Nacos => {
                if self.nacos_address.as_deref().unwrap_or("").is_empty() {
                    return Err(anyhow!("--nacos-address is required when --config-source=nacos."));
                }
                if self.nacos_dataid.as_deref().unwrap_or("").is_empty() {
                    return Err(anyhow!("--nacos-dataid is required when --config-source=nacos."));
                }
            }
        }
        Ok(())
    }
}

fn cache_dir() -> PathBuf {
    env::var(ENV_NACOS_CACHE_DIR)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(".nacos_cache"))
}

fn ttl_secs() -> u64 {
    env::var(ENV_NACOS_CACHE_TTL_SECS)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300)
}

fn cache_key(address: &str, dataid: &str, group: &str) -> String {
    // sanitize unsafe filename chars
    let mut key = format!("{}_{}_{}", address, dataid, group);
    for ch in [":", "/", "\\", "?", "&", "=", "#", " "] { key = key.replace(ch, "_"); }
    key
}

fn load_cache(address: &str, dataid: &str, group: &str) -> Option<String> {
    let dir = cache_dir();
    let path = dir.join(cache_key(address, dataid, group));
    let meta = fs::metadata(&path).ok()?;
    let modified = meta.modified().ok()?;
    let age = SystemTime::now().duration_since(modified).ok()?;
    let ttl = Duration::from_secs(ttl_secs());
    let content = fs::read_to_string(&path).ok()?;
    if age <= ttl {
        Some(content)
    } else {
        // expired but still return for potential downgrade on network failure
        Some(content)
    }
}

fn save_cache(address: &str, dataid: &str, group: &str, content: &str) -> anyhow::Result<()> {
    let dir = cache_dir();
    fs::create_dir_all(&dir).ok();
    let path = dir.join(cache_key(address, dataid, group));
    let mut f = fs::File::create(&path).context("failed to create cache file")?;
    f.write_all(content.as_bytes()).context("failed to write cache file")?;
    Ok(())
}

fn filter_config_sections(content: &str) -> anyhow::Result<String> {
    let mut ini = Ini::new();
    ini.set_inline_comment_symbols(Some(&Vec::new()));
    ini.read(content.to_string()).map_err(|e| anyhow!(e))?;
    let allow_sections = vec![
        "extractor","sinker","pipeline","parallelizer","runtime","filter","router",
        "resumer","data_marker","processor","meta_center","metrics","precheck",
    ];

    let mut out = String::new();
    for section in allow_sections {
        if let Some(props) = ini.get_map_ref().get(section) {
            out.push_str(&format!("[{}]\n", section));
            for (k, v) in props.iter() {
                let vv = v.as_deref().unwrap_or("");
                out.push_str(&format!("{}={}\n", k, vv));
            }
            out.push('\n');
        }
    }
    Ok(out)
}

pub async fn load_config_string(args: &CliArgs) -> anyhow::Result<String> {
    match args.source {
        ConfigSourceKind::Local => {
            let path = args.config_path.as_ref().unwrap();
            let content = fs::read_to_string(path)
                .with_context(|| format!("failed to read ini file: {}", path))?;
            Ok(content)
        }
        ConfigSourceKind::Nacos => {
            let address = args.nacos_address.as_ref().unwrap();
            let dataid = args.nacos_dataid.as_ref().unwrap();
            let group = &args.nacos_group;

            if let Some(cached) = load_cache(address, dataid, group) {
                // return cached first if network fails later; we'll try fresh fetch now
                match fetch_nacos(address, dataid, group).await {
                    Ok(fresh) => {
                        save_cache(address, dataid, group, &fresh).ok();
                        let filtered = filter_config_sections(&fresh)?;
                        return Ok(filtered);
                    }
                    Err(err) => {
                        let filtered = filter_config_sections(&cached)?;
                        eprintln!(
                            "warn: fetch nacos failed ({}), using cached config.",
                            err
                        );
                        return Ok(filtered);
                    }
                }
            } else {
                let fresh = fetch_nacos(address, dataid, group).await?;
                save_cache(address, dataid, group, &fresh).ok();
                let filtered = filter_config_sections(&fresh)?;
                Ok(filtered)
            }
        }
    }
}

async fn fetch_nacos(address: &str, dataid: &str, group: &str) -> anyhow::Result<String> {
    let url = format!(
        "{}/nacos/v1/cs/configs?dataId={}&group={}",
        address.trim_end_matches('/'),
        percent_encoding::utf8_percent_encode(dataid, percent_encoding::NON_ALPHANUMERIC),
        percent_encoding::utf8_percent_encode(group, percent_encoding::NON_ALPHANUMERIC),
    );
    let client = reqwest::Client::new();
    let resp = client.get(&url).send().await.context("failed to request nacos")?;
    if !resp.status().is_success() {
        return Err(anyhow!(format!("nacos returned non-success status: {}", resp.status())));
    }
    let body = resp.text().await.context("failed to read nacos response body")?;
    Ok(body)
}