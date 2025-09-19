use std::sync::Arc;

use async_trait::async_trait;
use kafka::producer::{Producer, Record};
use tokio::time::Instant;

use crate::{call_batch_fn, rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};
use dt_common::{
    config::message_format::MessageFormat,
    meta::{
        avro::avro_converter::AvroConverter, 
        json::json_converter::JsonConverter,
        ddl_meta::ddl_data::DdlData, 
        row_data::RowData
    },
    monitor::monitor::Monitor,
    utils::limit_queue::LimitedQueue,
};

pub struct KafkaSinker {
    pub batch_size: usize,
    pub router: RdbRouter,
    pub producer: Producer,
    pub avro_converter: AvroConverter,
    pub json_converter: JsonConverter,
    pub message_format: MessageFormat,
    pub monitor: Arc<Monitor>,
}

#[async_trait]
impl Sinker for KafkaSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        match self.message_format {
            MessageFormat::Avro => {
                call_batch_fn!(self, data, Self::send_avro);
            }
            MessageFormat::Json => {
                call_batch_fn!(self, data, Self::send_json);
            }
        }
        Ok(())
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        let mut messages = Vec::new();
        for ddl_data in data {
            let topic = self.router.get_topic(&ddl_data.default_schema, "");
            let payload = match self.message_format {
                MessageFormat::Avro => self.avro_converter.ddl_data_to_avro_value(ddl_data).await?,
                MessageFormat::Json => self.json_converter.ddl_data_to_json_value(ddl_data).await?.into_bytes(),
            };
            messages.push(Record {
                key: String::new(),
                value: payload,
                topic,
                partition: -1,
            });
        }
        self.producer.send_all(&messages)?;
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
        let mut data_size = 0;

        let mut messages = Vec::new();
        for row_data in data.iter_mut().skip(sinked_count).take(batch_size) {
            data_size += row_data.data_size;

            row_data.convert_raw_string();
            let topic = self.router.get_topic(&row_data.schema, &row_data.tb);
            let key = self.avro_converter.row_data_to_avro_key(row_data).await?;
            let payload = self
                .avro_converter
                .row_data_to_avro_value(row_data.clone())
                .await?;
            messages.push(Record {
                key,
                value: payload,
                topic,
                partition: -1,
            });
        }

        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        self.producer.send_all(&messages)?;
        // TODO: Currently measuring RT for the entire message batch,
        //       as kafka producer involves internal per-broker merging logic,
        //       making it impossible to see individual broker RT. This can be optimized in the future.
        rts.push((
            start_time.elapsed().as_millis() as u64,
            messages.len() as u64,
        ));

        BaseSinker::update_batch_monitor(&self.monitor, batch_size as u64, data_size as u64)
            .await?;
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await
    }

    async fn send_json(
        &mut self,
        data: &mut [RowData],
        sinked_count: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let mut data_size = 0;

        let mut messages = Vec::new();
        for row_data in data.iter_mut().skip(sinked_count).take(batch_size) {
            data_size += row_data.data_size;

            row_data.convert_raw_string();
            let topic = self.router.get_topic(&row_data.schema, &row_data.tb);
            let key = self.json_converter.row_data_to_json_key(row_data).await?;
            let payload = self
                .json_converter
                .row_data_to_json_value(row_data.clone())
                .await?
                .into_bytes();
            messages.push(Record {
                key,
                value: payload,
                topic,
                partition: -1,
            });
        }

        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        self.producer.send_all(&messages)?;
        rts.push((
            start_time.elapsed().as_millis() as u64,
            messages.len() as u64,
        ));

        BaseSinker::update_batch_monitor(&self.monitor, batch_size as u64, data_size as u64)
            .await?;
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await
    }
}
