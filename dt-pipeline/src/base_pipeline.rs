use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use dt_common::{
    config::sinker_config::SinkerConfig,
    error::{DtResultExt, Stage},
    log_error, log_finished, log_info, log_position, log_warn,
    meta::{
        dcl_meta::dcl_data::DclData,
        ddl_meta::ddl_data::DdlData,
        dt_data::{DtData, DtItem},
        dt_queue::DtQueue,
        position::Position,
        row_data::RowData,
        syncer::Syncer,
    },
    monitor::{
        counter_type::CounterType, task_metrics::TaskMetricsType, task_monitor::MonitorType,
        task_monitor_handle::TaskMonitorHandle,
    },
    runtime_trace,
};
use dt_connector::{
    data_marker::DataMarker,
    extractor::resumer::{recorder::Recorder, utils::ResumerUtil},
    Sinker,
};
use dt_parallelizer::{DataSize, Parallelizer};
use tokio::{
    sync::{Mutex, RwLock},
    time::{Duration, Instant},
};

use crate::{lua_processor::LuaProcessor, Pipeline};

pub struct BasePipeline {
    pub buffer: Arc<DtQueue>,
    pub parallelizer: Box<dyn Parallelizer + Send + Sync>,
    pub sinker_config: SinkerConfig,
    pub sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    pub shut_down: Arc<AtomicBool>,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
    pub syncer: Arc<Mutex<Syncer>>,
    pub monitor: TaskMonitorHandle,
    pub pending_snapshot_finished: HashMap<String, Position>,
    pub data_marker: Option<Arc<RwLock<DataMarker>>>,
    pub lua_processor: Option<LuaProcessor>,
    pub recorder: Option<Arc<dyn Recorder + Send + Sync>>,
    pub propagate_checkpoint_to_sinker: bool,
}

enum SinkMethod {
    Raw,
    Ddl,
    Dcl,
    Dml,
    Struct,
}

#[async_trait]
impl Pipeline for BasePipeline {
    async fn stop(&mut self) -> anyhow::Result<()> {
        let final_position = {
            let syncer = self.syncer.lock().await;
            Self::final_commit_position(&syncer)
        };
        for sinker in self.sinkers.iter_mut() {
            sinker
                .lock()
                .await
                .close_with_position(final_position.as_ref())
                .await?;
        }
        self.parallelizer.close().await.stage(Stage::Parallelizer)
    }

    async fn start(&mut self) -> anyhow::Result<()> {
        log_info!(
            "{} starts, parallel_size: {}, checkpoint_interval_secs: {}",
            self.parallelizer.get_name(),
            self.sinkers.len(),
            self.checkpoint_interval_secs
        );

        let mut last_sink_time = Instant::now();
        let mut last_checkpoint_time = Instant::now();
        let mut last_received_position = Position::None;
        let mut last_commit_positions = HashMap::new();
        let mut record_time = Instant::now();

        loop {
            let shutting_down = self.shut_down.load(Ordering::Acquire);
            let buffer_empty = self.buffer.is_empty();
            let pending_finish_empty = self.pending_snapshot_finished.is_empty();
            let has_pending_work =
                !buffer_empty || !pending_finish_empty || self.parallelizer.has_pending_data();

            if shutting_down && !has_pending_work {
                break;
            }

            if !has_pending_work {
                self.buffer.wait_for_data(Duration::from_secs(1)).await;
            }

            // to avoid too many sub counters, only add counter when buffer is not empty
            if !self.buffer.is_empty() {
                self.monitor
                    .add_counter(
                        self.monitor.default_task_id(),
                        CounterType::BufferSize,
                        self.buffer.len() as u64,
                    )
                    .await;
            }
            if record_time.elapsed().as_secs() > 1 {
                let len = self.buffer.len() as u64;
                let size = self.buffer.get_curr_size();
                self.monitor.set_counter(
                    self.monitor.default_task_id(),
                    CounterType::QueuedRecordCurrent,
                    len,
                );
                self.monitor.set_counter(
                    self.monitor.default_task_id(),
                    CounterType::QueuedByteCurrent,
                    size,
                );
                record_time = Instant::now();
            }

            // some sinkers need to accumulate data to a big batch and sink
            let data = if !shutting_down
                && last_sink_time.elapsed().as_secs() < self.batch_sink_interval_secs
                && !self.buffer.is_full()
            {
                Vec::new()
            } else {
                last_sink_time = Instant::now();
                self.parallelizer
                    .drain(self.buffer.as_ref())
                    .await
                    .stage(Stage::Parallelizer)?
            };

            if let Some(data_marker) = &mut self.data_marker {
                if !data.is_empty() {
                    data_marker.write().await.data_origin_node = data[0].data_origin_node.clone();
                }
            }

            // process all row_data_items in buffer at a time
            let (data_size, last_received, last_commits) = match self.get_sink_method(&data) {
                SinkMethod::Ddl => self.sink_ddl(data).await?,
                SinkMethod::Dcl => self.sink_dcl(data).await?,
                SinkMethod::Dml => self.sink_dml(data).await?,
                SinkMethod::Raw => self.sink_raw(data).await?,
                SinkMethod::Struct => self.sink_struct(data).await?,
            };

            if let Some(position) = &last_received {
                self.syncer.lock().await.received_position = position.to_owned();
                last_received_position = position.to_owned();
            }
            for position in last_commits {
                last_commit_positions
                    .insert(ResumerUtil::get_key_from_position(&position), position);
            }

            last_checkpoint_time = self
                .record_checkpoint(
                    Some(last_checkpoint_time),
                    &last_received_position,
                    &last_commit_positions,
                )
                .await?;

            self.monitor
                .add_counter(
                    self.monitor.default_task_id(),
                    CounterType::SinkedRecordTotal,
                    data_size.count,
                )
                .await
                .add_counter(
                    self.monitor.default_task_id(),
                    CounterType::SinkedByteTotal,
                    data_size.bytes,
                )
                .await;

            self.try_finish_snapshot_tasks().await?;

            runtime_trace::instrument_wait("yield_now.pipeline.run", tokio::task::yield_now())
                .await;
        }

        self.record_checkpoint(None, &last_received_position, &last_commit_positions)
            .await?;
        self.try_finish_snapshot_tasks().await?;
        Ok(())
    }
}

impl BasePipeline {
    fn final_commit_position(syncer: &Syncer) -> Option<Position> {
        (!matches!(syncer.committed_position, Position::None))
            .then_some(syncer.committed_position.clone())
    }

    async fn sink_raw(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Vec<Position>)> {
        let (data_count, last_received_position, commit_positions) =
            Self::fetch_raw(&all_data, &mut self.pending_snapshot_finished);
        if data_count > 0 {
            let data_size = self
                .parallelizer
                .sink_raw(all_data, &self.sinkers)
                .await
                .stage(Stage::Parallelizer)?;
            Ok((data_size, last_received_position, commit_positions))
        } else {
            Ok((
                DataSize::default(),
                last_received_position,
                commit_positions,
            ))
        }
    }

    async fn sink_struct(
        &mut self,
        mut all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Vec<Position>)> {
        let mut data = Vec::new();
        for i in all_data.drain(..) {
            if let DtData::Struct { struct_data } = i.dt_data {
                data.push(struct_data);
            }
        }
        if data.is_empty() {
            return Ok((DataSize::default(), None, Vec::new()));
        }

        let data_size = self
            .parallelizer
            .sink_struct(data, &self.sinkers)
            .await
            .stage(Stage::Parallelizer)?;

        Ok((data_size, None, Vec::new()))
    }

    async fn sink_dml(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Vec<Position>)> {
        let (mut data, last_received_position, last_commit_position) =
            Self::fetch_dml(all_data, &mut self.pending_snapshot_finished);
        let commit_positions = last_commit_position.into_iter().collect();
        if data.is_empty() {
            return Ok((
                DataSize::default(),
                last_received_position,
                commit_positions,
            ));
        }

        // execute lua processor
        if let Some(lua_processor) = &self.lua_processor {
            data = lua_processor.process(data)?;
        }

        let data_size = self
            .parallelizer
            .sink_dml(data, &self.sinkers)
            .await
            .stage(Stage::Parallelizer)?;
        Ok((data_size, last_received_position, commit_positions))
    }

    async fn sink_ddl(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Vec<Position>)> {
        let (data, last_received_position, last_commit_position) =
            Self::fetch_ddl(all_data, &mut self.pending_snapshot_finished);
        let commit_positions: Vec<_> = last_commit_position.clone().into_iter().collect();
        if !data.is_empty() {
            let data_size = self
                .parallelizer
                .sink_ddl(data.clone(), &self.sinkers)
                .await
                .stage(Stage::Parallelizer)?;
            // only part of sinkers will execute sink_ddl, but all sinkers should refresh metadata
            for sinker in self.sinkers.iter_mut() {
                sinker.lock().await.refresh_meta(data.clone()).await?;
            }
            self.monitor
                .add_counter(
                    self.monitor.default_task_id(),
                    CounterType::DDLRecordTotal,
                    data_size.count,
                )
                .await;
            Ok((data_size, last_received_position, commit_positions))
        } else {
            Ok((
                DataSize::default(),
                last_received_position,
                commit_positions,
            ))
        }
    }

    async fn sink_dcl(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Vec<Position>)> {
        let (data, last_received_position, last_commit_position) =
            Self::fetch_dcl(all_data, &mut self.pending_snapshot_finished);
        let commit_positions = last_commit_position.into_iter().collect();
        let data_size = DataSize {
            count: data.len() as u64,
            bytes: 0,
        };
        if data_size.count > 0 {
            self.parallelizer
                .sink_dcl(data, &self.sinkers)
                .await
                .stage(Stage::Parallelizer)?;
        }
        Ok((data_size, last_received_position, commit_positions))
    }

    pub fn fetch_raw(
        data: &[DtItem],
        pending_snapshot_finished: &mut HashMap<String, Position>,
    ) -> (u64, Option<Position>, Vec<Position>) {
        let mut data_count = 0;
        let mut last_received_position = Option::None;
        let mut commit_positions = HashMap::new();
        for i in data.iter() {
            match &i.dt_data {
                DtData::Commit { .. } => {
                    if Self::collect_snapshot_finished(&i.position, pending_snapshot_finished) {
                        continue;
                    }
                    Self::collect_commit_position(&mut commit_positions, &i.position);
                    last_received_position = Some(i.position.clone());
                    continue;
                }
                DtData::Heartbeat {} | DtData::Ddl { .. } => {
                    Self::collect_commit_position(&mut commit_positions, &i.position);
                    last_received_position = Some(i.position.clone());
                    continue;
                }
                DtData::Begin {} => {
                    continue;
                }

                DtData::Redis { .. } | DtData::KafkaRaw { .. } => {
                    last_received_position = Some(i.position.clone());
                    Self::collect_commit_position(&mut commit_positions, &i.position);
                    data_count += 1;
                }

                _ => {
                    last_received_position = Some(i.position.clone());
                    data_count += 1;
                }
            }
        }

        let mut commit_positions: Vec<(String, Position)> = commit_positions.into_iter().collect();
        commit_positions.sort_by(|left, right| left.0.cmp(&right.0));
        (
            data_count,
            last_received_position,
            commit_positions
                .into_iter()
                .map(|(_, position)| position)
                .collect(),
        )
    }

    fn collect_commit_position(
        commit_positions: &mut HashMap<String, Position>,
        position: &Position,
    ) {
        if matches!(position, Position::None) {
            return;
        }

        commit_positions.insert(
            ResumerUtil::get_key_from_position(position),
            position.clone(),
        );
    }

    fn fetch_dml(
        mut data: Vec<DtItem>,
        pending_snapshot_finished: &mut HashMap<String, Position>,
    ) -> (Vec<RowData>, Option<Position>, Option<Position>) {
        let mut dml_data = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match i.dt_data {
                DtData::Commit { .. } => {
                    if Self::collect_snapshot_finished(&i.position, pending_snapshot_finished) {
                        continue;
                    }
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }
                DtData::Heartbeat {} => {
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }

                DtData::Dml { row_data } => {
                    last_received_position = Some(i.position);
                    dml_data.push(row_data);
                }

                _ => {}
            }
        }

        (dml_data, last_received_position, last_commit_position)
    }

    fn fetch_ddl(
        mut data: Vec<DtItem>,
        pending_snapshot_finished: &mut HashMap<String, Position>,
    ) -> (Vec<DdlData>, Option<Position>, Option<Position>) {
        let mut result = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match i.dt_data {
                DtData::Commit { .. } => {
                    if Self::collect_snapshot_finished(&i.position, pending_snapshot_finished) {
                        continue;
                    }
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }
                DtData::Heartbeat {} => {
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }

                DtData::Ddl { ddl_data } => {
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    result.push(ddl_data);
                }

                _ => {}
            }
        }

        (result, last_received_position, last_commit_position)
    }

    fn fetch_dcl(
        mut data: Vec<DtItem>,
        pending_snapshot_finished: &mut HashMap<String, Position>,
    ) -> (Vec<DclData>, Option<Position>, Option<Position>) {
        let mut result = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match i.dt_data {
                DtData::Commit { .. } => {
                    if Self::collect_snapshot_finished(&i.position, pending_snapshot_finished) {
                        continue;
                    }
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                }
                DtData::Heartbeat {} => {
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                }

                DtData::Dcl { dcl_data } => {
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    result.push(dcl_data);
                }

                _ => {}
            }
        }

        (result, last_received_position, last_commit_position)
    }

    fn get_sink_method(&self, data: &Vec<DtItem>) -> SinkMethod {
        for i in data {
            match i.dt_data {
                DtData::Struct { .. } => return SinkMethod::Struct,
                DtData::Ddl { .. } => return SinkMethod::Ddl,
                DtData::Dcl { .. } => return SinkMethod::Dcl,
                DtData::Dml { .. } => return SinkMethod::Dml,
                DtData::Redis { .. } | DtData::KafkaRaw { .. } => return SinkMethod::Raw,
                DtData::Begin {} | DtData::Commit { .. } | DtData::Heartbeat {} => continue,
            }
        }
        SinkMethod::Raw
    }

    async fn try_finish_snapshot_tasks(&mut self) -> anyhow::Result<()> {
        let finished_task_ids: Vec<String> =
            self.pending_snapshot_finished.keys().cloned().collect();

        for task_id in finished_task_ids {
            let Some(finish_position) = self.pending_snapshot_finished.remove(&task_id) else {
                continue;
            };

            self.handle_snapshot_finished_control_item(&finish_position)
                .await?;

            self.monitor
                .with_type(MonitorType::Sinker)
                .unregister_monitor(&task_id);
            self.monitor
                .add_no_window_metrics(TaskMetricsType::FinishedProgressCount, 1);
            log_finished!("{}", finish_position.to_string());
            if let Some(handler) = &self.recorder {
                if let Err(err) = handler.record_position(&finish_position).await {
                    log_error!(
                        "failed to record finish position: {}, err: {:#}",
                        finish_position,
                        err
                    );
                }
            }
        }

        Ok(())
    }

    async fn handle_snapshot_finished_control_item(
        &mut self,
        finish_position: &Position,
    ) -> anyhow::Result<()> {
        if matches!(finish_position, Position::RdbSnapshotFinished { .. }) {
            // The table's data has already been sunk when the finished position reaches this
            // point. Forward it as a control item so sinkers and checker can react to snapshot
            // lifecycle events.
            let item = DtItem {
                dt_data: DtData::Commit { xid: String::new() },
                position: finish_position.clone(),
                data_origin_node: String::new(),
            };
            for sinker in self.sinkers.iter_mut() {
                sinker.lock().await.handle_control_item(&item).await?;
            }
        }
        Ok(())
    }

    fn collect_snapshot_finished(
        position: &Position,
        pending_snapshot_finished: &mut HashMap<String, Position>,
    ) -> bool {
        if let Position::RdbSnapshotFinished { db, schema, tb, .. } = position {
            pending_snapshot_finished.insert(
                TaskMonitorHandle::task_id_from_db_schema_tb(db, schema, tb),
                position.clone(),
            );
            true
        } else {
            false
        }
    }

    async fn record_checkpoint(
        &self,
        last_checkpoint_time: Option<Instant>,
        last_received_position: &Position,
        last_commit_positions: &HashMap<String, Position>,
    ) -> anyhow::Result<Instant> {
        if let Some(last) = last_checkpoint_time {
            if last.elapsed().as_secs() < self.checkpoint_interval_secs {
                return Ok(last);
            }
        }

        if !matches!(last_received_position, Position::None) {
            // extracting chunks will sink None position.
            log_position!("current_position | {}", last_received_position.to_string());
        }
        let mut commit_positions: Vec<(&String, &Position)> =
            last_commit_positions.iter().collect();
        commit_positions.sort_by(|left, right| left.0.cmp(right.0));
        for (_, position) in commit_positions.iter() {
            log_position!("checkpoint_position | {}", position.to_string());
        }

        // A CDC row position inside an open transaction is only a receive watermark. Advancing
        // the durable checkpoint to it could skip the rest of that transaction after restart.
        // Snapshot positions remain resumable chunk watermarks, while raw streams add their
        // independently committable positions to commit_positions in fetch_raw.
        let fallback_position = (!matches!(
            last_received_position,
            Position::MysqlCdc { .. } | Position::PgCdc { .. }
        ))
        .then_some(last_received_position);
        let checker_position = commit_positions
            .last()
            .map(|(_, position)| *position)
            .or(fallback_position);

        if self.propagate_checkpoint_to_sinker
            && checker_position.is_some_and(|position| !matches!(position, Position::None))
        {
            // Lifecycle-aware decorators are attached to the last sinker so their close hook
            // runs after every regular sinker has closed.
            if let Some(sinker) = self.sinkers.last() {
                if let Err(err) = sinker
                    .lock()
                    .await
                    .record_checkpoint(checker_position.expect("checked above"))
                    .await
                {
                    if last_checkpoint_time.is_none() {
                        return Err(err);
                    }
                    log_warn!("sinker checkpoint hook failed: {}", err);
                }
            }
        }
        if let Some(handler) = &self.recorder {
            if commit_positions.is_empty() {
                if let Some(position) = fallback_position {
                    if let Err(e) = handler.record_position(position).await {
                        if last_checkpoint_time.is_none() {
                            return Err(e);
                        }
                        log_error!("failed to record position: {}, err: {:#}", position, e);
                    }
                }
            } else {
                for (_, position) in commit_positions.iter() {
                    if let Err(e) = handler.record_position(position).await {
                        if last_checkpoint_time.is_none() {
                            return Err(e);
                        }
                        log_error!("failed to record position: {}, err: {:#}", position, e);
                    }
                }
            }
        }

        if let Some(checker_position) = checker_position.filter(|p| !matches!(p, Position::None)) {
            let mut syncer = self.syncer.lock().await;
            syncer.committed_position = checker_position.clone();
            if !last_commit_positions.is_empty() {
                syncer.committed_positions = last_commit_positions.clone();
            }
        }

        self.monitor.set_counter(
            self.monitor.default_task_id(),
            CounterType::Timestamp,
            last_received_position.to_timestamp(),
        );

        Ok(Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{atomic::AtomicBool, Arc, Mutex as StdMutex},
        time::Duration,
    };

    use async_trait::async_trait;
    use dt_common::{
        config::sinker_config::SinkerConfig,
        meta::{
            dt_data::{DtData, DtItem},
            dt_queue::DtQueue,
            kafka::kafka_message::KafkaMessage,
            position::Position,
            redis::redis_entry::RedisEntry,
            row_data::RowData,
            row_type::RowType,
            syncer::Syncer,
        },
        monitor::{task_monitor::MonitorType, task_monitor_handle::TaskMonitorHandle},
    };
    use dt_connector::{extractor::resumer::utils::ResumerUtil, Sinker};
    use dt_parallelizer::{
        base_parallelizer::BaseParallelizer, serial_parallelizer::SerialParallelizer,
    };
    use tokio::sync::Mutex;

    use super::{BasePipeline, Pipeline};

    #[derive(Default)]
    struct RecordingState {
        sunk_items: usize,
        checkpoints: Vec<Position>,
        controls: Vec<Position>,
        fail_checkpoint: bool,
        closed_positions: Vec<Option<Position>>,
    }

    struct RecordingSinker(Arc<StdMutex<RecordingState>>);

    #[async_trait]
    impl Sinker for RecordingSinker {
        async fn sink_dml(&mut self, data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
            self.0.lock().unwrap().sunk_items += data.len();
            Ok(())
        }

        async fn sink_raw(&mut self, data: Vec<DtItem>, _batch: bool) -> anyhow::Result<()> {
            self.0.lock().unwrap().sunk_items += data.len();
            Ok(())
        }

        async fn record_checkpoint(&mut self, position: &Position) -> anyhow::Result<()> {
            if self.0.lock().unwrap().fail_checkpoint {
                anyhow::bail!("checkpoint unavailable");
            }
            self.0.lock().unwrap().checkpoints.push(position.clone());
            Ok(())
        }

        async fn close_with_position(&mut self, position: Option<&Position>) -> anyhow::Result<()> {
            self.0
                .lock()
                .unwrap()
                .closed_positions
                .push(position.cloned());
            Ok(())
        }

        async fn handle_control_item(&mut self, item: &DtItem) -> anyhow::Result<()> {
            self.0.lock().unwrap().controls.push(item.position.clone());
            Ok(())
        }
    }

    fn mysql_position(next_event_position: u32) -> Position {
        Position::MysqlCdc {
            server_id: "1".to_string(),
            binlog_filename: "mysql-bin.000001".to_string(),
            next_event_position,
            gtid_set: String::new(),
            timestamp: String::new(),
        }
    }

    fn dml_item(position: Position) -> DtItem {
        DtItem {
            dt_data: DtData::Dml {
                row_data: RowData {
                    db: String::new(),
                    schema: "public".to_string(),
                    tb: "events".to_string(),
                    chunk_id: 0,
                    row_type: RowType::Insert,
                    before: None,
                    after: Some(HashMap::new()),
                    data_size: 1,
                    is_not_origin: false,
                },
            },
            position,
            data_origin_node: String::new(),
        }
    }

    async fn make_shutdown_pipeline(
        queued: Vec<DtItem>,
        staged: Vec<DtItem>,
        batch_sink_interval_secs: u64,
    ) -> (
        BasePipeline,
        Arc<Mutex<Syncer>>,
        Arc<StdMutex<RecordingState>>,
    ) {
        let buffer = Arc::new(DtQueue::new(8, 0, None, None));
        for item in queued {
            buffer.push(item).await.unwrap();
        }
        assert!(!buffer.is_full());

        let mut base_parallelizer = BaseParallelizer::default();
        base_parallelizer.popped_data.extend(staged);
        let state = Arc::new(StdMutex::new(RecordingState::default()));
        let syncer = Arc::new(Mutex::new(Syncer::default()));
        let pipeline = BasePipeline {
            buffer,
            parallelizer: Box::new(SerialParallelizer { base_parallelizer }),
            sinker_config: SinkerConfig::Dummy,
            sinkers: vec![Arc::new(async_mutex::Mutex::new(Box::new(
                RecordingSinker(state.clone()),
            )))],
            shut_down: Arc::new(AtomicBool::new(true)),
            checkpoint_interval_secs: 60,
            batch_sink_interval_secs,
            syncer: syncer.clone(),
            monitor: TaskMonitorHandle::noop(MonitorType::Pipeline),
            pending_snapshot_finished: HashMap::new(),
            data_marker: None,
            lua_processor: None,
            recorder: None,
            propagate_checkpoint_to_sinker: true,
        };

        (pipeline, syncer, state)
    }

    async fn shutdown_pipeline(
        queued: Vec<DtItem>,
        staged: Vec<DtItem>,
        batch_sink_interval_secs: u64,
    ) -> (Arc<Mutex<Syncer>>, Arc<StdMutex<RecordingState>>) {
        let (mut pipeline, syncer, state) =
            make_shutdown_pipeline(queued, staged, batch_sink_interval_secs).await;
        tokio::time::timeout(Duration::from_secs(1), pipeline.start())
            .await
            .expect("shutdown drain must not wait for batch interval")
            .unwrap();
        pipeline.stop().await.unwrap();
        (syncer, state)
    }

    fn redis_node_position(node_id: &str, repl_offset: u64) -> Position {
        Position::Redis {
            node_id: Some(node_id.to_string()),
            address: Some(format!("127.0.0.1:{repl_offset}")),
            repl_id: format!("repl-{node_id}"),
            repl_port: 10008,
            repl_offset,
            now_db_id: 0,
            timestamp: String::new(),
        }
    }

    fn redis_item(position: Position) -> DtItem {
        DtItem {
            dt_data: DtData::Redis {
                entry: RedisEntry::new(),
            },
            position,
            data_origin_node: String::new(),
        }
    }

    fn kafka_raw_item(position: Position) -> DtItem {
        DtItem {
            dt_data: DtData::KafkaRaw {
                message: KafkaMessage {
                    topic: "source.topic".to_string(),
                    partition: 0,
                    offset: 42,
                    key: None,
                    payload: Some(vec![1, 2, 3]),
                    headers: Vec::new(),
                    position: position.to_string(),
                },
            },
            position,
            data_origin_node: String::new(),
        }
    }

    #[test]
    fn fetch_raw_collects_latest_position_per_redis_node() {
        let mut pending_snapshot_finished = HashMap::new();
        let node_1_old = redis_node_position("node-1", 10);
        let node_2 = redis_node_position("node-2", 20);
        let node_1_new = redis_node_position("node-1", 30);
        let data = vec![
            redis_item(node_1_old),
            redis_item(node_2.clone()),
            redis_item(node_1_new.clone()),
        ];

        let (_, _, commit_positions) =
            BasePipeline::fetch_raw(&data, &mut pending_snapshot_finished);

        assert_eq!(commit_positions.len(), 2);
        let by_key: HashMap<_, _> = commit_positions
            .into_iter()
            .map(|position| (ResumerUtil::get_key_from_position(&position), position))
            .collect();
        assert_eq!(by_key.get("redis-node-node-1"), Some(&node_1_new));
        assert_eq!(by_key.get("redis-node-node-2"), Some(&node_2));
    }

    #[test]
    fn fetch_raw_collects_kafka_raw_position() {
        let mut pending_snapshot_finished = HashMap::new();
        let position = Position::Kafka {
            topic: "source.topic".to_string(),
            partition: 0,
            offset: 42,
        };
        let data = vec![kafka_raw_item(position.clone())];

        let (data_count, last_received_position, commit_positions) =
            BasePipeline::fetch_raw(&data, &mut pending_snapshot_finished);

        assert_eq!(data_count, 1);
        assert_eq!(last_received_position, Some(position.clone()));
        assert_eq!(commit_positions, vec![position]);
    }

    #[tokio::test]
    async fn shutdown_bypasses_batch_interval_and_drains_staged_data() {
        let queued_position = Position::Kafka {
            topic: "source.topic".to_string(),
            partition: 0,
            offset: 42,
        };
        let staged_position = Position::Kafka {
            topic: "source.topic".to_string(),
            partition: 1,
            offset: 84,
        };

        let (syncer, state) = shutdown_pipeline(
            vec![kafka_raw_item(queued_position.clone())],
            vec![kafka_raw_item(staged_position.clone())],
            60,
        )
        .await;

        let state = state.lock().unwrap();
        assert_eq!(state.sunk_items, 2);
        assert_eq!(state.checkpoints, vec![staged_position.clone()]);
        assert_eq!(state.closed_positions, vec![Some(staged_position)]);
        drop(state);
        let syncer = syncer.lock().await;
        assert_eq!(syncer.committed_positions.len(), 2);
    }

    #[tokio::test]
    async fn shutdown_does_not_checkpoint_partial_cdc_transaction() {
        let row_position = mysql_position(120);

        let (syncer, state) =
            shutdown_pipeline(vec![dml_item(row_position.clone())], Vec::new(), 15).await;

        let state = state.lock().unwrap();
        assert_eq!(state.sunk_items, 1);
        assert!(state.checkpoints.is_empty());
        drop(state);
        let syncer = syncer.lock().await;
        assert_eq!(syncer.received_position, row_position);
        assert_eq!(syncer.committed_position, Position::None);
        assert!(syncer.committed_positions.is_empty());
    }

    #[tokio::test]
    async fn final_checkpoint_failure_is_not_successful_shutdown() {
        let position = Position::Kafka {
            topic: "source.topic".into(),
            partition: 0,
            offset: 42,
        };
        let (mut pipeline, syncer, state) =
            make_shutdown_pipeline(vec![kafka_raw_item(position)], Vec::new(), 60).await;
        state.lock().unwrap().fail_checkpoint = true;
        let error = tokio::time::timeout(Duration::from_secs(1), pipeline.start())
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("checkpoint unavailable"));
        assert_eq!(syncer.lock().await.committed_position, Position::None);
    }

    #[tokio::test]
    async fn shutdown_before_snapshot_finished_does_not_complete_snapshot() {
        let row_position = Position::RdbSnapshot {
            db_type: "mysql".to_string(),
            db: String::new(),
            schema: "public".to_string(),
            tb: "events".to_string(),
            order_key: None,
        };

        let (syncer, state) =
            shutdown_pipeline(vec![dml_item(row_position.clone())], Vec::new(), 60).await;

        let state = state.lock().unwrap();
        assert_eq!(state.sunk_items, 1);
        assert!(state.controls.is_empty());
        assert_eq!(state.checkpoints, vec![row_position.clone()]);
        drop(state);
        assert_eq!(syncer.lock().await.committed_position, row_position);
    }
}
