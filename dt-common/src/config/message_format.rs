use std::{fmt, str::FromStr};

use super::json_template_type::JsonTemplateType;

#[derive(Clone, Debug, Default, PartialEq)]
pub enum MessageFormat {
    #[default]
    Avro,
    Json,
    JsonTemplate(JsonTemplateType),
}

impl MessageFormat {
    pub fn json_template(&self, configured: JsonTemplateType) -> JsonTemplateType {
        match self {
            Self::JsonTemplate(template) => template.clone(),
            _ => configured,
        }
    }
}

impl FromStr for MessageFormat {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "avro" => Ok(Self::Avro),
            "json" => Ok(Self::Json),
            "cloudcanal" => Ok(Self::JsonTemplate(JsonTemplateType::CloudCanal)),
            normalized if normalized.starts_with("json_template:") => {
                let template = normalized.strip_prefix("json_template:").unwrap();
                Ok(Self::JsonTemplate(JsonTemplateType::from_str(template)?))
            }
            _ => Err(format!("invalid message format: {value}")),
        }
    }
}

impl fmt::Display for MessageFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Avro => formatter.write_str("avro"),
            Self::Json => formatter.write_str("json"),
            Self::JsonTemplate(template) => write!(formatter, "json_template:{template}"),
        }
    }
}
