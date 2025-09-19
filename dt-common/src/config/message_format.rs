use std::str::FromStr;

#[derive(Clone, Debug, PartialEq)]
pub enum MessageFormat {
    Avro,
    Json,
}

impl FromStr for MessageFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "avro" => Ok(MessageFormat::Avro),
            "json" => Ok(MessageFormat::Json),
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
        }
    }
}