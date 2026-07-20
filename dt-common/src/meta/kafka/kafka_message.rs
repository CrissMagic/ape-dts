#[derive(Debug, Clone)]
pub struct KafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
    pub position: String,
}

impl KafkaMessage {
    pub fn get_data_size(&self) -> u64 {
        let key_size = self.key.as_ref().map_or(0, |key| key.len());
        let payload_size = self.payload.as_ref().map_or(0, |payload| payload.len());
        let header_size: usize = self
            .headers
            .iter()
            .map(|(key, value)| key.len() + value.as_ref().map_or(0, |value| value.len()))
            .sum();
        (key_size + payload_size + header_size) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::KafkaMessage;

    #[test]
    fn kafka_message_size_counts_present_key_payload_and_headers() {
        let message = KafkaMessage {
            topic: "source".to_string(),
            partition: 0,
            offset: 1,
            key: Some(vec![1, 2]),
            payload: Some(vec![3, 4, 5]),
            headers: vec![("h1".to_string(), Some(vec![6])), ("h2".to_string(), None)],
            position: String::new(),
        };

        assert_eq!(message.get_data_size(), 10);
    }

    #[test]
    fn kafka_message_size_preserves_null_key_and_payload() {
        let message = KafkaMessage {
            topic: "source".to_string(),
            partition: 0,
            offset: 1,
            key: None,
            payload: None,
            headers: vec![("h".to_string(), Some(vec![1, 2]))],
            position: String::new(),
        };

        assert_eq!(message.get_data_size(), 3);
    }
}
