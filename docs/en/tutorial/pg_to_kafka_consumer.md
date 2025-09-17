# Send Postgres data to Kafka

Refer to [Send data to Kafka](/docs/en/consumer/kafka_consumer.md) for consumers.

# Prerequisites
- [prerequisites](./prerequisites.md)

- This article is for quick start, refer to [templates](/docs/templates/rdb_to_kafka.md) and [common configs](/docs/en/config.md) for more details.

# Message Format Support

APE DTS supports two message formats when sending data to Kafka:
- **Avro format** (default): Binary format with schema registry support
- **JSON format**: Human-readable JSON format

You can configure the message format using the `message_format` parameter in the `[sinker]` section:
- `message_format=avro` (default)
- `message_format=json`

# Prepare Postgres instance
Refer to [pg to pg](./pg_to_pg.md)

# Prepare Kafka instance
Refer to [mysql to kafka](./mysql_to_kafka_consumer.md)

# Send Snapshot data to Kafka
## Prepare data
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

CREATE SCHEMA test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
INSERT INTO test_db.tb_1 VALUES(1,1),(2,2),(3,3),(4,4);
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s

[sinker]
db_type=kafka
sink_type=write
url=127.0.0.1:9093
with_field_defs=true
message_format=avro
message_format=avro

[filter]
do_dbs=test_db
do_events=insert

[router]
topic_map=*.*:test

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

# Send CDC data to Kafka

## Drop replication slot if exists
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

SELECT pg_drop_replication_slot('ape_test') FROM pg_replication_slots WHERE slot_name = 'ape_test';
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
slot_name=ape_test

[filter]
do_dbs=test_db,test_db_2
do_events=insert,update,delete
do_ddls=*

[router]
topic_map=*.*:test

[sinker]
db_type=kafka
sink_type=write
url=127.0.0.1:9093
with_field_defs=true

[parallelizer]
parallel_type=serial
parallel_size=1

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Make changes in Postgres
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

CREATE SCHEMA test_db_2;
CREATE TABLE test_db_2.tb_2(id int, value int, primary key(id));
INSERT INTO test_db_2.tb_2 VALUES(1,1);
UPDATE test_db_2.tb_2 SET value=100000 WHERE id=1;
DELETE FROM test_db_2.tb_2;
```

# Run Kafka consumer demo

[python / golang consumer demo](https://github.com/apecloud/ape_dts_consumer_demo)