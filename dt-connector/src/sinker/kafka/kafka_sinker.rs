use anyhow::bail;
use async_trait::async_trait;
use kafka::producer::{Producer, Record};
use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
};
use tokio::time::{Duration, Instant};

use dt_common::{
    config::message_format::MessageFormat,
    meta::{
        avro::avro_converter::AvroConverter,
        ddl_meta::ddl_data::DdlData,
        dt_data::{DtData, DtItem},
        json::json_converter::JsonConverter,
        kafka::kafka_message::KafkaMessage,
        row_data::RowData,
    },
    utils::limit_queue::LimitedQueue,
};

use crate::{call_batch_fn, rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};

pub struct KafkaSinker {
    pub batch_size: usize,
    pub router: RdbRouter,
    pub producer: Producer,
    pub raw_producer: FutureProducer,
    pub raw_queue_timeout_secs: u64,
    pub avro_converter: AvroConverter,
    pub json_converter: JsonConverter,
    pub message_format: MessageFormat,
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
        if data.is_empty() {
            return Ok(());
        }

        match &self.message_format {
            MessageFormat::Avro => {
                call_batch_fn!(self, data, Self::send_avro);
            }
            MessageFormat::Json | MessageFormat::JsonTemplate(_) => {
                call_batch_fn!(self, data, Self::send_json);
            }
        };
        Ok(())
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        let mut messages = Vec::new();
        for ddl_data in data {
            let target_topics = self.get_ddl_target_topics(&ddl_data.default_schema);
            let payload = match &self.message_format {
                MessageFormat::Avro => self.avro_converter.ddl_data_to_avro_value(ddl_data).await?,
                MessageFormat::Json | MessageFormat::JsonTemplate(_) => self
                    .json_converter
                    .ddl_data_to_json_value(ddl_data)
                    .await?
                    .into_bytes(),
            };
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
        self.json_converter.refresh_meta(&data);
        Ok(())
    }
}

impl KafkaSinker {
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

    async fn send_json(
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
            let key = self.json_converter.row_data_to_json_key(row_data).await?;
            let payload = self
                .json_converter
                .row_data_to_json_value(row_data.clone())
                .await?
                .into_bytes();
            for topic in target_topics {
                messages.push(KafkaSinkMessage {
                    key: key.clone(),
                    value: payload.clone(),
                    topic,
                });
            }
        }

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

            if let Err(err) = self.raw_producer.send(record, queue_timeout).await {
                bail!("failed in kafka raw producer, error: {:?}", err);
            }
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
