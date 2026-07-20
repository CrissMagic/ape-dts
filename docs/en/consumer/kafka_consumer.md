# Send data to Kafka for consumers

APE DTS supports two Kafka output paths: encoding RDB data before writing it to Kafka, and forwarding Kafka messages to multiple Kafka topics without decoding them. See the [Kafka task template](/docs/templates/rdb_to_kafka.md) for complete configurations and [config details](/docs/en/config.md) for field definitions.

## RDB -> Kafka fanout

- MySQL/PostgreSQL snapshot or CDC data is encoded as Avro. With `[router].topic_fanout_map`, the same encoded result is written to multiple target topics.
- DML routing priority is `schema.table > schema.* > *.*`; DDL uses `schema.*` or `*.*`.
- This path sends the same key/value and does not create Kafka headers.

## Kafka -> Kafka raw forwarding

Raw forwarding preserves the source key, payload, and headers, including a null key, null payload, or header with a null value. Only the target topic changes. `with_field_defs` does not apply to this path.

### Required configuration

- Both extractor and sinker must use `db_type=kafka`, and `[parallelizer].parallel_type` must be `serial`.
- The left side of `[router].topic_fanout_map` must exactly match `[extractor].topic`, for example `source.topic:target.a|target_b`.
- One task consumes one `[extractor].topic` and one `partition`. Explicit partition assignment is used, so consumer group rebalancing is not supported; run only one task instance for a given `group`/`topic`/`partition`.
- `offset` must be `-1`, `ack_interval_secs` must be explicitly greater than `0`, and no `[resumer]` section may be configured.
- `[sinker].required_acks` accepts only `one` or `all` and defaults to `one`; `none` and other values fail during config loading. Use `all` when production durability requires acknowledgements from all current in-sync replicas.
- Topic names support letters, digits, `.`, `_`, and `-`; empty targets, duplicate sources/targets, whitespace, and other delimiters are rejected.

### Position and failure handling

1. Each source message is sent to every mapped target topic, awaiting each producer delivery result.
2. Source offset `N` becomes the latest committable position only after every target send in the current pipeline batch succeeds.
3. Every `ack_interval_secs`, the consumer synchronously commits the latest position; Kafka stores it as group offset `N+1`.

A target send failure, a topic/partition position mismatch, or a synchronous group-offset commit failure stops the task. Restart resumes from the most recently committed group offset, providing at-least-once delivery; messages successfully sent after that commit may be duplicated. A new group with no committed offset starts from `latest` and does not automatically consume historical messages.

The first version does not preserve the source partition or timestamp and does not support key/header rewriting, transactional producers, or exactly-once delivery.

# Consumer

[python / golang consumer demo](https://github.com/apecloud/ape_dts_consumer_demo)
