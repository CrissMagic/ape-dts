# MySQL/Postgres/Kafka -> Kafka templates

Refer to [config details](/docs/en/config.md) for explanations of common fields.

# MySQL Snapshot
```
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
ignore_dbs=
do_dbs=
do_tbs=test_db_1.*,test_db_2.*
ignore_tbs=
do_events=insert

[sinker]
db_type=kafka
sink_type=write
batch_size=200
url=127.0.0.1:9093
with_field_defs=true

[router]
topic_map=*.*:default_topic,test_db_2.*:topic2,test_db_2.tb_1:topic3

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

- refer to [config details](/docs/en/config.md) for [router] topic_map

- [sinker]

| Config | Description | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| url | url of Kafka servers | 127.0.0.1:9093 | - |
| with_field_defs | when sending data to Kafka in avro format, include the definitions of data fields or not | true | true |
| message_format | message format for Kafka producer. Supports `avro`, `json`, `cloudcanal`, and `json_template:<type>` | avro | avro |
| json_template | JSON template used when `message_format=json`. Supports `standard` and `cloudcanal` | cloudcanal | standard |
| skip_confirmed_noop_updates | for PG -> Kafka, skip UPDATE events confirmed to have no actual field changes | false | true |

CloudCanal JSON can be configured either as `message_format=cloudcanal`,
`message_format=json_template:cloudcanal`, or `message_format=json` with
`json_template=cloudcanal`.

# MySQL CDC
```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=0
binlog_filename=
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
ignore_dbs=
do_dbs=
do_tbs=*.*
ignore_tbs=
do_events=insert,update,delete
do_ddls=*

[sinker]
db_type=kafka
sink_type=write
batch_size=200
url=127.0.0.1:9093
with_field_defs=true

[router]
topic_map=*.*:test

[parallelizer]
parallel_type=serial
parallel_size=1

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

# RDB -> Kafka fanout

Use `topic_fanout_map` when one MySQL/Postgres task should write the same encoded message to multiple Kafka topics.

```
[router]
topic_fanout_map=*.*:topic_a|topic_b,test_db_2.*:topic_c|topic_d,test_db_2.tb_1:topic_e|topic_f
```

RDB fanout priority for DML is `schema.table > schema.* > *.*`. DDL uses schema-level matching through `schema.*` or `*.*`. The message is encoded once and the same key/value is sent to every target topic. This path does not create Kafka headers.

# Kafka -> Kafka raw forwarding

Use Kafka raw forwarding when a Kafka message should be copied to other Kafka topics without decoding or changing its bytes.

```
[extractor]
db_type=kafka
extract_type=cdc
url=127.0.0.1:9093
group=ape_dts_raw_forward
topic=source.topic
partition=0
offset=-1
ack_interval_secs=5

[sinker]
db_type=kafka
sink_type=write
url=127.0.0.1:9093
ack_timeout_secs=5
required_acks=one

[router]
topic_fanout_map=source.topic:target.a|target_b|target-c

[parallelizer]
parallel_type=serial
parallel_size=1

[runtime]
log_dir=./logs/kafka-raw-partition-0
log_level=info
log4rs_file=./log4rs.yaml
```

Kafka raw forwarding preserves the source key, payload, and headers, including a null key, null payload, or header with a null value. It does not decode or re-encode the payload. The source and target `url` values may point to the same or different Kafka clusters.

Required behavior:

- `offset` must be `-1`, `ack_interval_secs` must be greater than `0`, `[resumer]` must be omitted, and `parallel_type` must be `serial`.
- `topic_fanout_map` uses the full Kafka topic name on the left side. It must contain an exact entry for `[extractor].topic`.
- `required_acks` must be `one` or `all` (default: `one`). Use `all` when all current in-sync replicas must acknowledge each message.
- One task consumes one configured source partition. Run only one task instance for the same `group`/`topic`/`partition`; explicit partition assignment does not provide consumer group rebalancing.

For each source message, every mapped target send is awaited. After all sends in the completed pipeline batch succeed, source offset `N` becomes committable. Every `ack_interval_secs`, the consumer synchronously stores the latest committable group offset as `N+1`. A send, position validation, or offset commit failure stops the task.

Delivery is at-least-once. Restart resumes at the latest successfully committed group offset, so messages sent since that periodic commit may be duplicated. A new group without a committed offset starts from `latest` and does not automatically read historical messages. A shorter `ack_interval_secs` reduces the replay window but increases group-offset commit frequency.

The first version supports only letters, digits, `.`, `_`, and `-` in Kafka topic names. It does not preserve the source partition or timestamp and does not support key/header rewriting, transactional producers, or exactly-once delivery.

When using the binary package, follow [Linux binary packaging and deployment](../en/linux_binary_package.md). Keep the control-script log directory equal to `[runtime].log_dir`, and use a different log directory for each partition task.

# Posgres Snapshot

The only difference with MySQL is [extractor]

```
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
batch_size=10000
```

# Postgres CDC

The only difference with MySQL is [extractor]

```
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
start_lsn=0/406DE430
slot_name=ape_test
```

For PG -> Kafka, `[sinker].skip_confirmed_noop_updates` defaults to `true`. It skips only UPDATE events containing `UnchangedToast` when the comparable `before` / `after` values are sufficient to confirm that all other fields are unchanged. Events that cannot be confirmed as no-ops are still sent. Sent PG events omit `UnchangedToast` fields from `after`; set the option to `false` to send confirmed no-op events as well. This option does not affect PG -> RDB.

If the Lua processor is enabled, its existing conversion changes `UnchangedToast` to an ordinary `NULL` before the Kafka sinker sees the event. In that case this filter cannot identify or skip the event, and the emitted value follows the Lua output. The option does not change Lua behavior.
