use std::{fmt, str::FromStr};

#[derive(Clone, Debug, Default, PartialEq)]
pub enum JsonTemplateType {
    #[default]
    Standard,
    CloudCanal,
}

impl FromStr for JsonTemplateType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "standard" => Ok(Self::Standard),
            "cloudcanal" => Ok(Self::CloudCanal),
            _ => Err(format!("unsupported JSON template: {value}")),
        }
    }
}

impl fmt::Display for JsonTemplateType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Standard => "standard",
            Self::CloudCanal => "cloudcanal",
        })
    }
}
