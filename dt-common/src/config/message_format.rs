use std::str::FromStr;
use super::json_template_type::JsonTemplateType;

#[derive(Clone, Debug, PartialEq)]
pub enum MessageFormat {
    Avro,
    Json,
    /// JSON 模板格式，支持不同的模板类型
    JsonTemplate(JsonTemplateType),
}

impl FromStr for MessageFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "avro" => Ok(MessageFormat::Avro),
            "json" => Ok(MessageFormat::Json),
            // 支持 json_template:template_type 格式
            s if s.starts_with("json_template:") => {
                let template_type = s.strip_prefix("json_template:").unwrap();
                let json_template_type = JsonTemplateType::from_str(template_type)?;
                Ok(MessageFormat::JsonTemplate(json_template_type))
            }
            // 支持简化格式，直接使用模板类型名称
            "cloudcanal" => Ok(MessageFormat::JsonTemplate(JsonTemplateType::CloudCanal)),
            _ => Err(format!("Invalid message format: {}", s)),
        }
    }
}

impl Default for MessageFormat {
    fn default() -> Self {
        MessageFormat::Avro
    }
}

impl ToString for MessageFormat {
    fn to_string(&self) -> String {
        match self {
            MessageFormat::Avro => "avro".to_string(),
            MessageFormat::Json => "json".to_string(),
            MessageFormat::JsonTemplate(template_type) => {
                format!("json_template:{}", template_type.to_string())
            }
        }
    }
}