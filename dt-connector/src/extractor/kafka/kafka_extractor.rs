use std::{sync::Arc, time::Duration};

use anyhow::Context;
use async_trait::async_trait;
use dt_common::{
    log_info, log_warn,
    meta::{
        avro::avro_converter::AvroConverter, dt_data::DtData, kafka::kafka_message::KafkaMessage,
        position::Position, syncer::Syncer,
    },
};
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Headers,
    ClientConfig, Message, Offset, TopicPartitionList,
};
use tokio::{
    sync::Mutex,
    time::{interval, MissedTickBehavior},
};

use crate::{
    extractor::{
        base_extractor::{BaseExtractor, ExtractState},
        resumer::recovery::Recovery,
    },
    Extractor,
};

pub struct KafkaExtractor {
    pub base_extractor: BaseExtractor,
    pub extract_state: ExtractState,
    pub url: String,
    pub group: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub ack_interval_secs: u64,
    pub avro_converter: AvroConverter,
    pub raw_forward: bool,
    pub syncer: Arc<Mutex<Syncer>>,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

#[async_trait]
impl Extractor for KafkaExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        if let Some(recovery) = &self.recovery {
            if let Some(position) = recovery.get_cdc_resume_position().await {
                match &position {
                    Position::Kafka { offset, .. } => {
                        self.offset = offset.to_owned();
                        log_info!("cdc recovery from offset:[{}]", offset);
                    }
                    _ => {
                        log_warn!("position:{} is not a valid kafka position", position);
                    }
                }
            }
        }

        log_info!(
            "KafkaCdcExtractor starts, topic: {}, partition: {}, offset: {}, raw_forward: {}",
            self.topic,
            self.partition,
            self.offset,
            self.raw_forward
        );
        let consumer = self.create_consumer()?;
        if self.ack_interval_secs == 0 {
            return if self.raw_forward {
                self.extract_raw(&consumer).await
            } else {
                self.extract_avro(&consumer).await
            };
        }

        let syncer = self.syncer.clone();
        let topic = self.topic.clone();
        let partition = self.partition;
        let ack_interval_secs = self.ack_interval_secs;
        let raw_forward = self.raw_forward;
        let extract = async {
            if raw_forward {
                self.extract_raw(&consumer).await
            } else {
                self.extract_avro(&consumer).await
            }
        };

        tokio::select! {
            result = extract => result,
            result = Self::ack_committed_offsets(
                &consumer,
                syncer,
                topic,
                partition,
                ack_interval_secs,
            ) => result,
        }
    }
}

impl KafkaExtractor {
    async fn extract_avro(&mut self, consumer: &StreamConsumer) -> anyhow::Result<()> {
        loop {
            let msg = consumer.recv().await?;
            if let Some(payload) = msg.payload() {
                let dt_data = self
                    .avro_converter
                    .avro_value_to_dt_data(payload.to_vec())?;
                let position = Position::Kafka {
                    topic: self.topic.clone(),
                    partition: self.partition,
                    offset: msg.offset(),
                };
                self.base_extractor
                    .push_dt_data(&mut self.extract_state, dt_data, position)
                    .await?;
            }
        }
    }

    async fn extract_raw(&mut self, consumer: &StreamConsumer) -> anyhow::Result<()> {
        loop {
            let msg = consumer
                .recv()
                .await
                .with_context(|| format!("KafkaCdcExtractor failed, topic: {}", self.topic))?;
            let position = Position::Kafka {
                topic: msg.topic().to_string(),
                partition: msg.partition(),
                offset: msg.offset(),
            };
            let headers = msg
                .headers()
                .map(|headers| {
                    headers
                        .iter()
                        .map(|header| {
                            (
                                header.key.to_string(),
                                header.value.map(|value| value.to_vec()),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            let message = KafkaMessage {
                topic: msg.topic().to_string(),
                partition: msg.partition(),
                offset: msg.offset(),
                key: msg.key().map(|key| key.to_vec()),
                payload: msg.payload().map(|payload| payload.to_vec()),
                headers,
                position: position.to_string(),
            };
            self.base_extractor
                .push_dt_data(
                    &mut self.extract_state,
                    DtData::KafkaRaw { message },
                    position,
                )
                .await?;
        }
    }

    async fn ack_committed_offsets(
        consumer: &StreamConsumer,
        syncer: Arc<Mutex<Syncer>>,
        topic: String,
        partition: i32,
        ack_interval_secs: u64,
    ) -> anyhow::Result<()> {
        let mut ticker = interval(Duration::from_secs(ack_interval_secs));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await;
        let mut last_acked_offset = None;

        loop {
            ticker.tick().await;
            let committed_position = syncer.lock().await.committed_position.clone();
            let Some(next_offset) =
                Self::next_commit_offset(&committed_position, &topic, partition)?
            else {
                continue;
            };
            if last_acked_offset == Some(next_offset) {
                continue;
            }

            let mut offsets = TopicPartitionList::new();
            offsets
                .add_partition_offset(&topic, partition, Offset::Offset(next_offset))
                .with_context(|| {
                    format!(
                        "KafkaCdcExtractor failed to build commit offset, topic: {}, partition: {}, offset: {}",
                        topic, partition, next_offset
                    )
                })?;
            consumer
                .commit(&offsets, CommitMode::Sync)
                .with_context(|| {
                    format!(
                        "KafkaCdcExtractor failed to commit group offset, topic: {}, partition: {}, offset: {}",
                        topic, partition, next_offset
                    )
                })?;
            log_info!(
                "KafkaCdcExtractor committed group offset, topic: {}, partition: {}, offset: {}",
                topic,
                partition,
                next_offset
            );
            last_acked_offset = Some(next_offset);
        }
    }

    fn next_commit_offset(
        position: &Position,
        expected_topic: &str,
        expected_partition: i32,
    ) -> anyhow::Result<Option<i64>> {
        match position {
            Position::None => Ok(None),
            Position::Kafka {
                topic,
                partition,
                offset,
            } => {
                if topic != expected_topic || *partition != expected_partition {
                    anyhow::bail!(
                        "KafkaCdcExtractor committed position does not match assigned topic/partition, expected: {}-{}, actual: {}-{}",
                        expected_topic,
                        expected_partition,
                        topic,
                        partition
                    );
                }
                Ok(Some(offset.checked_add(1).with_context(|| {
                    format!(
                        "KafkaCdcExtractor committed offset overflow, topic: {}, partition: {}, offset: {}",
                        topic, partition, offset
                    )
                })?))
            }
            _ => anyhow::bail!(
                "KafkaCdcExtractor expected Kafka committed position, actual: {}",
                position
            ),
        }
    }

    fn create_consumer(&self) -> anyhow::Result<StreamConsumer> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.url);
        config.set("group.id", &self.group);
        config.set("auto.offset.reset", "latest");
        config.set("session.timeout.ms", "10000");
        if self.ack_interval_secs > 0 {
            config.set("enable.auto.commit", "false");
            config.set("enable.auto.offset.store", "false");
        }

        let consumer: StreamConsumer = config
            .create()
            .context("KafkaCdcExtractor failed to create consumer")?;
        // only support extract data from one topic, one partition
        let mut tpl = TopicPartitionList::new();
        if self.offset >= 0 {
            tpl.add_partition_offset(&self.topic, self.partition, Offset::Offset(self.offset))
                .with_context(|| {
                    format!(
                        "KafkaCdcExtractor failed to set start offset, topic: {}, partition: {}, offset: {}",
                        self.topic, self.partition, self.offset
                    )
                })?;
        } else {
            tpl.add_partition(&self.topic, self.partition);
        }
        consumer.assign(&tpl).with_context(|| {
            format!(
                "KafkaCdcExtractor failed to assign topic/partition, topic: {}, partition: {}",
                self.topic, self.partition
            )
        })?;
        Ok(consumer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_commit_offset_is_after_the_sunk_message() {
        let position = Position::Kafka {
            topic: "source.topic".to_string(),
            partition: 0,
            offset: 42,
        };

        assert_eq!(
            KafkaExtractor::next_commit_offset(&position, "source.topic", 0).unwrap(),
            Some(43)
        );
    }

    #[test]
    fn next_commit_offset_rejects_unexpected_topic_or_partition() {
        let wrong_topic = Position::Kafka {
            topic: "other.topic".to_string(),
            partition: 0,
            offset: 42,
        };
        let wrong_partition = Position::Kafka {
            topic: "source.topic".to_string(),
            partition: 1,
            offset: 42,
        };

        assert!(KafkaExtractor::next_commit_offset(&wrong_topic, "source.topic", 0).is_err());
        assert!(KafkaExtractor::next_commit_offset(&wrong_partition, "source.topic", 0).is_err());
    }
}
