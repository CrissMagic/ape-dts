# 将数据发送到 Kafka 并由用户自主消费

APE DTS 支持两类 Kafka 输出：RDB 数据编码后写入 Kafka，以及 Kafka 消息不解码直接转发到多个 Kafka topic。完整可运行配置见 [Kafka 任务模板](/docs/templates/rdb_to_kafka.md)，字段定义见 [配置详情](/docs/zh/config.md)。

## RDB -> Kafka fanout

- MySQL/PostgreSQL 的 snapshot 或 CDC 数据使用 Avro 编码；配置 `[router].topic_fanout_map` 后，同一份编码结果会写入多个目标 topic。
- DML 路由优先级为 `schema.table > schema.* > *.*`；DDL 使用 `schema.*` 或 `*.*`。
- 该路径发送相同的 key/value，不生成 Kafka headers。

## Kafka -> Kafka 原始转发

原始转发保留源消息的 key、payload 和 headers，包括 null key、null payload 以及值为 null 的 header，只替换目标 topic。`with_field_defs` 不参与该路径。

### 必要配置

- extractor 和 sinker 都必须配置 `db_type=kafka`，`[parallelizer].parallel_type` 必须为 `serial`。
- `[router].topic_fanout_map` 左侧必须与 `[extractor].topic` 完全一致，例如 `source.topic:target.a|target_b`。
- 一个任务只消费一个 `[extractor].topic` 和一个 `partition`。当前使用显式分区分配，不支持 consumer group rebalance；同一 `group`/`topic`/`partition` 只能运行一个任务实例。
- `offset` 必须为 `-1`，`ack_interval_secs` 必须显式配置为大于 `0`，并且不能配置 `[resumer]`。
- `[sinker].required_acks` 只允许 `one` 或 `all`，默认 `one`；`none` 和其他值会在加载配置时失败。生产环境需要当前所有 ISR 副本确认时使用 `all`。
- topic 名只支持字母、数字、`.`、`_`、`-`；空 target、重复 source/target、空白或其他分隔符都会配置失败。
- 如果 extractor 和 sinker 使用规范化后相同的 Kafka bootstrap server 集合，target 列表不能包含 `[extractor].topic`，否则配置加载会拒绝该自循环。静态校验无法识别同一集群的不同主机别名。

### 位点与失败处理

1. 每条源消息发送到所有映射的目标 topic，并逐个等待 producer 交付结果。
2. 当前 pipeline 批次的全部目标发送成功后，源 offset `N` 才成为最新可提交位点。
3. consumer 每隔 `ack_interval_secs` 同步提交一次最新位点，Kafka 中保存的 group offset 为 `N+1`。

任一目标发送失败、位点与配置的 topic/partition 不一致，或 group offset 同步提交失败，任务都会报错退出。重启从最近一次成功提交的 group offset 继续，交付语义为 at-least-once；最近一次提交之后已经成功发送的消息可能重复。新 group 没有已提交 offset 时从 `latest` 开始，不会自动读取历史消息。

第一版不保留源 partition 和 timestamp，不支持 key/header 改写、事务 producer 或 exactly-once。

# 自主消费数据

[python / golang consumer demo](https://github.com/apecloud/ape_dts_consumer_demo)
