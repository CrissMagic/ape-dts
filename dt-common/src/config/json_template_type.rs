use std::str::FromStr;

/// JSON 模板类型枚举，用于支持不同消费端系统的消息格式
#[derive(Clone, Debug, PartialEq)]
pub enum JsonTemplateType {
    /// 标准 JSON 格式（默认）
    Standard,
    /// CloudCanal 格式，包含 action、before、data、db、schema、table 等字段
    CloudCanal,
}

impl FromStr for JsonTemplateType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "standard" => Ok(JsonTemplateType::Standard),
            "cloudcanal" => Ok(JsonTemplateType::CloudCanal),
            _ => Err(format!("不支持的 JSON 模板类型: {}", s)),
        }
    }
}

impl Default for JsonTemplateType {
    fn default() -> Self {
        JsonTemplateType::Standard
    }
}

impl ToString for JsonTemplateType {
    fn to_string(&self) -> String {
        match self {
            JsonTemplateType::Standard => "standard".to_string(),
            JsonTemplateType::CloudCanal => "cloudcanal".to_string(),
        }
    }
}