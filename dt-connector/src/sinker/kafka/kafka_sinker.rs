use anyhow::{bail, Context};
use async_trait::async_trait;
use kafka::producer::{Producer, Record};
use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
};
use tokio::time::{Duration, Instant};

use crate::{call_batch_fn, rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};
use dt_common::{
    config::config_enums::DbType,
    meta::{
        avro::avro_converter::AvroConverter,
        ddl_meta::ddl_data::DdlData,
        dt_data::{DtData, DtItem},
        kafka::kafka_message::KafkaMessage,
        row_data::RowData,
        row_type::RowType,
    },
    utils::limit_queue::LimitedQueue,
};

pub struct KafkaSinker {
    pub batch_size: usize,
    pub router: RdbRouter,
    pub producer: Producer,
    pub raw_producer: FutureProducer,
    pub raw_queue_timeout_secs: u64,
    pub avro_converter: AvroConverter,
    pub source_db_type: DbType,
    pub skip_confirmed_noop_updates: bool,
    pub base_sinker: BaseSinker,
}

struct KafkaSinkMessage {
    topic: String,
    key: String,
    value: Vec<u8>,
}

#[async_trait]
impl Sinker for KafkaSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
        data.retain_mut(|row_data| {
            Self::prepare_row_for_send(
                row_data,
                &self.source_db_type,
                self.skip_confirmed_noop_updates,
            )
        });
        if data.is_empty() {
            return Ok(());
        }

        call_batch_fn!(self, data, Self::send_avro);
        Ok(())
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        let mut messages = Vec::new();
        for ddl_data in data {
            let target_topics = self.get_ddl_target_topics(&ddl_data.default_schema);
            let payload = self.avro_converter.ddl_data_to_avro_value(ddl_data).await?;
            for topic in target_topics {
                messages.push(KafkaSinkMessage {
                    key: String::new(),
                    value: payload.clone(),
                    topic,
                });
            }
        }
        self.send_messages(&messages)?;
        Ok(())
    }

    async fn sink_raw(&mut self, data: Vec<DtItem>, _batch: bool) -> anyhow::Result<()> {
        let mut data_count = 0;
        let mut data_size = 0;
        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);

        for item in data {
            match item.dt_data {
                DtData::KafkaRaw { message } => {
                    data_count += 1;
                    data_size += message.get_data_size();
                    self.send_raw_message(&message).await?;
                }
                DtData::Begin {} | DtData::Commit { .. } | DtData::Heartbeat {} => {}
                _ => bail!("KafkaSinker sink_raw only supports KafkaRaw data"),
            }
        }

        if data_count > 0 {
            rts.push((start_time.elapsed().as_millis() as u64, data_count));
            self.base_sinker
                .update_batch_monitor(data_count, data_size)
                .await?;
            self.base_sinker.update_monitor_rt(&rts).await?;
        }
        Ok(())
    }

    async fn refresh_meta(&mut self, data: Vec<DdlData>) -> anyhow::Result<()> {
        self.avro_converter.refresh_meta(&data);
        Ok(())
    }
}

impl KafkaSinker {
    fn prepare_row_for_send(
        row_data: &mut RowData,
        source_db_type: &DbType,
        skip_confirmed_noop_updates: bool,
    ) -> bool {
        if source_db_type != &DbType::Pg {
            return true;
        }

        let should_skip = skip_confirmed_noop_updates
            && row_data.row_type == RowType::Update
            && row_data.contains_unchanged_toast()
            && match (&row_data.before, &row_data.after) {
                (Some(before), Some(after)) => {
                    before.keys().all(|col| after.contains_key(col))
                        && after.iter().all(|(col, value)| {
                            value.is_unchanged_toast()
                                || before
                                    .get(col)
                                    .is_some_and(|before_value| before_value.is_same_value(value))
                        })
                }
                _ => false,
            };
        if should_skip {
            return false;
        }

        if let Some(after) = &mut row_data.after {
            after.retain(|_, value| !value.is_unchanged_toast());
        }
        true
    }

    async fn send_avro(
        &mut self,
        data: &mut [RowData],
        sinked_count: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let task_id = self
            .base_sinker
            .task_id_for_rows(&data[sinked_count..sinked_count + batch_size]);
        self.base_sinker.ensure_monitor_for(&task_id);
        let mut data_size = 0;

        let mut messages = Vec::new();
        for row_data in data.iter_mut().skip(sinked_count).take(batch_size) {
            data_size += row_data.get_data_size();
            row_data.convert_raw_string();
            let target_topics = self.get_row_target_topics(&row_data.schema, &row_data.tb);
            let key = self.avro_converter.row_data_to_avro_key(row_data).await?;
            let payload = self.avro_converter.row_data_to_avro_value(row_data).await?;
            for topic in target_topics {
                messages.push(KafkaSinkMessage {
                    key: key.clone(),
                    value: payload.clone(),
                    topic,
                });
            }
        }

        // TODO: Currently measuring RT for the entire message batch,
        //       as kafka producer involves internal per-broker merging logic,
        //       making it impossible to see individual broker RT. This can be optimized in the future.
        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        self.send_messages(&messages)?;
        rts.push((
            start_time.elapsed().as_millis() as u64,
            messages.len() as u64,
        ));

        self.base_sinker
            .update_batch_monitor_for(&task_id, batch_size as u64, data_size)
            .await?;
        self.base_sinker.update_monitor_rt_for(&task_id, &rts).await
    }

    fn send_messages(&mut self, messages: &[KafkaSinkMessage]) -> anyhow::Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        let records: Vec<Record<'_, &str, &[u8]>> = messages
            .iter()
            .map(|message| Record {
                key: message.key.as_str(),
                value: message.value.as_slice(),
                topic: message.topic.as_str(),
                partition: -1,
            })
            .collect();
        self.producer.send_all(&records)?;
        Ok(())
    }

    fn get_row_target_topics(&self, schema: &str, tb: &str) -> Vec<String> {
        let fanout_topics = self.router.get_fanout_topics(schema, tb);
        if fanout_topics.is_empty() {
            return vec![self.router.get_topic(schema, tb).to_string()];
        }
        fanout_topics.to_vec()
    }

    fn get_ddl_target_topics(&self, default_schema: &str) -> Vec<String> {
        let fanout_topics = self.router.get_fanout_topics(default_schema, "");
        if fanout_topics.is_empty() {
            return vec![self.router.get_topic(default_schema, "").to_string()];
        }
        fanout_topics.to_vec()
    }

    async fn send_raw_message(&self, message: &KafkaMessage) -> anyhow::Result<()> {
        let target_topics = self
            .router
            .get_fanout_topics_for_kafka_topic(&message.topic);
        if target_topics.is_empty() {
            bail!(
                "missing [router].topic_fanout_map target topics for Kafka source topic '{}'",
                message.topic
            );
        }

        let queue_timeout = Duration::from_secs(self.raw_queue_timeout_secs);
        for topic in target_topics {
            let mut record = FutureRecord::<[u8], [u8]>::to(topic);
            if let Some(payload) = message.payload.as_deref() {
                record = record.payload(payload);
            }
            if let Some(key) = message.key.as_deref() {
                record = record.key(key);
            }
            if !message.headers.is_empty() {
                record = record.headers(Self::owned_headers(&message.headers));
            }

            self.raw_producer
                .send(record, queue_timeout)
                .await
                .map_err(|(error, _message)| error)
                .context("failed to send raw Kafka message")?;
        }
        Ok(())
    }

    fn owned_headers(headers: &[(String, Option<Vec<u8>>)]) -> OwnedHeaders {
        headers.iter().fold(
            OwnedHeaders::new_with_capacity(headers.len()),
            |owned, (key, value)| {
                let value: Option<&[u8]> = value.as_deref();
                owned.insert(Header {
                    key: key.as_str(),
                    value,
                })
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dt_common::{
        config::config_enums::DbType,
        meta::{col_value::ColValue, row_type::RowType},
    };

    use super::*;

    #[test]
    fn confirmed_unchanged_toast_update_is_not_sent() {
        let before = HashMap::from([
            ("id".to_string(), ColValue::Long(1)),
            ("name".to_string(), ColValue::String("same".to_string())),
        ]);
        let after = HashMap::from([
            ("id".to_string(), ColValue::Long(1)),
            ("name".to_string(), ColValue::String("same".to_string())),
            ("content".to_string(), ColValue::UnchangedToast),
        ]);
        let mut row_data = RowData::new(
            String::new(),
            "public".to_string(),
            "items".to_string(),
            0,
            RowType::Update,
            Some(before),
            Some(after),
        );

        assert!(!KafkaSinker::prepare_row_for_send(
            &mut row_data,
            &DbType::Pg,
            true
        ));
    }

    #[test]
    fn key_only_before_can_confirm_update_with_only_unchanged_toast_fields() {
        let before = HashMap::from([("id".to_string(), ColValue::Long(1))]);
        let after = HashMap::from([
            ("id".to_string(), ColValue::Long(1)),
            ("content".to_string(), ColValue::UnchangedToast),
        ]);
        let mut row_data = RowData::new(
            String::new(),
            "public".to_string(),
            "items".to_string(),
            0,
            RowType::Update,
            Some(before),
            Some(after),
        );

        assert!(!KafkaSinker::prepare_row_for_send(
            &mut row_data,
            &DbType::Pg,
            true
        ));
    }

    #[test]
    fn changed_update_is_sent_without_unchanged_toast_fields() {
        let before = HashMap::from([
            ("id".to_string(), ColValue::Long(1)),
            ("name".to_string(), ColValue::String("before".to_string())),
        ]);
        let after = HashMap::from([
            ("id".to_string(), ColValue::Long(1)),
            ("name".to_string(), ColValue::String("after".to_string())),
            ("content".to_string(), ColValue::UnchangedToast),
        ]);
        let mut row_data = RowData::new(
            String::new(),
            "public".to_string(),
            "items".to_string(),
            0,
            RowType::Update,
            Some(before),
            Some(after),
        );

        assert!(KafkaSinker::prepare_row_for_send(
            &mut row_data,
            &DbType::Pg,
            true
        ));
        let after = row_data.require_after().unwrap();
        assert_eq!(
            after.get("name"),
            Some(&ColValue::String("after".to_string()))
        );
        assert!(!after.contains_key("content"));
    }

    #[test]
    fn update_with_incomplete_before_is_sent_without_unchanged_toast_fields() {
        let before = HashMap::from([("id".to_string(), ColValue::Long(1))]);
        let after = HashMap::from([
            ("id".to_string(), ColValue::Long(1)),
            ("name".to_string(), ColValue::String("same".to_string())),
            ("content".to_string(), ColValue::UnchangedToast),
        ]);
        let mut row_data = RowData::new(
            String::new(),
            "public".to_string(),
            "items".to_string(),
            0,
            RowType::Update,
            Some(before),
            Some(after),
        );

        assert!(KafkaSinker::prepare_row_for_send(
            &mut row_data,
            &DbType::Pg,
            true
        ));
        let after = row_data.require_after().unwrap();
        assert_eq!(
            after.get("name"),
            Some(&ColValue::String("same".to_string()))
        );
        assert!(!after.contains_key("content"));
    }

    #[test]
    fn filtering_disabled_sends_update_without_unchanged_toast_fields() {
        let before = HashMap::from([
            ("id".to_string(), ColValue::Long(1)),
            ("name".to_string(), ColValue::String("same".to_string())),
        ]);
        let after = HashMap::from([
            ("id".to_string(), ColValue::Long(1)),
            ("name".to_string(), ColValue::String("same".to_string())),
            ("content".to_string(), ColValue::UnchangedToast),
        ]);
        let mut row_data = RowData::new(
            String::new(),
            "public".to_string(),
            "items".to_string(),
            0,
            RowType::Update,
            Some(before),
            Some(after),
        );

        assert!(KafkaSinker::prepare_row_for_send(
            &mut row_data,
            &DbType::Pg,
            false
        ));
        assert!(!row_data.require_after().unwrap().contains_key("content"));
    }

    #[test]
    fn non_pg_row_is_unchanged_by_pg_kafka_filter() {
        let before = HashMap::from([("id".to_string(), ColValue::Long(1))]);
        let after = HashMap::from([
            ("id".to_string(), ColValue::Long(1)),
            ("content".to_string(), ColValue::UnchangedToast),
        ]);
        let mut row_data = RowData::new(
            String::new(),
            "test_db".to_string(),
            "items".to_string(),
            0,
            RowType::Update,
            Some(before),
            Some(after),
        );

        assert!(KafkaSinker::prepare_row_for_send(
            &mut row_data,
            &DbType::Mysql,
            true
        ));
        assert!(row_data.require_after().unwrap().contains_key("content"));
    }
}
