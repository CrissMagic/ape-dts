# Send MySQL data to Kafka

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

# Prepare MySQL instance
Refer to [mysql to mysql](./mysql_to_mysql.md)

# Prepare Kafka instance
- start zookeeper
```
rm -rf /tmp/ape_dts/kafka/zookeeper_data
mkdir -p /tmp/ape_dts/kafka/zookeeper_data

docker run --name some-zookeeper \
-p 2181:2181 \
-v "/tmp/ape_dts/kafka/zookeeper_data:/bitnami" \
-e ALLOW_ANONYMOUS_LOGIN=yes \
-d "$ZOOKEEPER_IMAGE"
```

- start kafka
```
rm -rf /tmp/ape_dts/kafka/kafka_data
mkdir -p /tmp/ape_dts/kafka/kafka_data

docker run --name some-kafka \
-p 9092:9092 \
-p 9093:9093 \
-v "/tmp/ape_dts/kafka/kafka_data:/bitnami/kafka" \
-e KAFKA_CFG_ZOOKEEPER_CONNECT=host.docker.internal:2181 \
-e ALLOW_PLAINTEXT_LISTENER=yes \
-e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CLIENT:PLAINTEXT,EXTERNAL:PLAINTEXT \
-e KAFKA_CFG_LISTENERS=CLIENT://:9092,EXTERNAL://:9093 \
-e KAFKA_CFG_ADVERTISED_LISTENERS=CLIENT://127.0.0.1:9092,EXTERNAL://127.0.0.1:9093 \
-e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=CLIENT \
-d "$KAFKA_IMAGE"
```

- create test topic
```
docker exec -it some-kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9093
```

# Send Snapshot data to Kafka
## Prepare data
```
mysql -h127.0.0.1 -uroot -p123456 -P3307

CREATE DATABASE test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
INSERT INTO test_db.tb_1 VALUES(1,1),(2,2),(3,3),(4,4);
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

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
## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=cdc
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

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

## Make changes in MySQL
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3307

CREATE DATABASE test_db_2;
CREATE TABLE test_db_2.tb_2(id int, value int, primary key(id));
INSERT INTO test_db_2.tb_2 VALUES(1,1);
UPDATE test_db_2.tb_2 SET value=100000 WHERE id=1;
DELETE FROM test_db_2.tb_2;
```

# Run Kafka consumer demo

[python / golang consumer demo](https://github.com/apecloud/ape_dts_consumer_demo)