# English | [中文](README_ZH.md)

# Introduction

- ape-dts is a data migration tool enabling any-to-any data transfers.
- It also provides data subscription and data processing.
- It is lightweight, efficient and standalone, requiring no third-party components or extra storage.
- Designed for cloud-native stateless component scenarios.
- In Rust.

## Key features

- Supports data migration between various databases, both homogeneous and heterogeneous.
- Supports snapshot and cdc tasks with resume from breakpoint.
- Supports checking and revising data.
- Supports filtering and routing at the database, table, and column levels.
- Implements different parallel algorithms for different sources, targets, and task types to improve performance.
- Allows loading user-defined Lua scripts to modify the data.

## Supported task types

|                          | mysql -> mysql | pg -> pg | mongo -> mongo | redis -> redis | mysql -> kafka | pg -> kafka | mysql -> starrocks | mysql -> clickhouse | mysql -> tidb | pg -> starrocks | pg -> clickhouse | mysql -> doris | pg -> doris |
| :----------------------- | :------------- | :------- | :------------- | :------------- | :------------- | :---------- | :----------------- | :------------------ | :------------ | :-------------- | :--------------- | :------------- | :---------- |
| Snapshot                 | &#10004;       | &#10004; | &#10004;       | &#10004;       | &#10004;       | &#10004;    | &#10004;           | &#10004;            | &#10004;      | &#10004;        | &#10004;         | &#10004;       | &#10004;    |
| CDC                      | &#10004;       | &#10004; | &#10004;       | &#10004;       | &#10004;       | &#10004;    | &#10004;           | &#10004;            | &#10004;      | &#10004;        | &#10004;         | &#10004;       | &#10004;    |
| Data check/revise/review | &#10004;       | &#10004; | &#10004;       |                |                |             |                    |                     | &#10004;      |                 |                  |                |             |
| Structure migration      | &#10004;       | &#10004; |                |                |                |             | &#10004;           | &#10004;            | &#10004;      | &#10004;        | &#10004;         | &#10004;       | &#10004;    |

# Advanced

## Crate features

The dt-main crate provides several optional components which can be enabled via `Cargo [features]`:

- `metrics`: Enable Prometheus format task metrics HTTP service interface.
  After enabling this feature, you can customize the metrics service with the following configuration:

  ```
  [metrics]
  # http service host
  http_host=127.0.0.1
  # http service port
  http_port=9090
  # http service worker count
  workers=2
  # prometheus metrics const labels
  labels=your_label1:your_value1,your_label2:your_value2
  ```

- TBD

## Config source and startup arguments

`dt-main` can load task configuration from a local INI file or from Nacos. Both
sources are parsed into the same task configuration structures and support normal
tasks and precheck flows.

Arguments:

- `--config-source`: config source, `local|nacos`, default `local`
- `--config-path`: local INI file path; `--config` and one positional argument remain compatible
- `--nacos-address`: required when the source is `nacos`, for example `http://nacos-host:8848`
- `--nacos-dataid`: required when the source is `nacos`, mapped to the Nacos `dataId`
- `--nacos-group`: optional when the source is `nacos`, default `DEFAULT_GROUP`

Nacos filtering and cache behavior:

- Only task configuration sections are loaded: `global`, `extractor`, `sinker`, `pipeline`, `parallelizer`, `runtime`, `filter`, `router`, `resumer`, `data_marker`, `processor`, `checker`, `metacenter`, `metrics`, `precheck`
- Default cache directory: `.nacos_cache`, override with `NACOS_CACHE_DIR`
- Default cache TTL: `300` seconds, override with `NACOS_CACHE_TTL_SECS`
- Default Nacos request timeout: `10` seconds, override with `NACOS_REQUEST_TIMEOUT_SECS`
- When fetching fails, or the fetched content cannot be parsed as a valid task config, an existing cache is used. Expired cache is still used and a warning is printed
- When fetching fails and no usable cache exists, startup fails

Examples:

```bash
# Local file, compatible with the legacy positional argument
/ape-dts ./configs/task_mysql.ini

# Local file, explicit argument
/ape-dts --config-source local --config-path ./configs/task_mysql.ini

# Load from Nacos
/ape-dts --config-source nacos \
  --nacos-address http://nacos-host:8848 \
  --nacos-dataid task_pg.ini \
  --nacos-group DEFAULT_GROUP
```

# Quick starts

## CLI

`dtscli` is a lightweight local CLI for creating and managing ApeCloud DTS tasks.
It can generate task configs, start `dt-main`, list tasks, stream logs, and stop,
restart, or delete local task records.

![dtscli demo](./docs/img/demo.gif)

For installation and detailed usage, see [dt-cli/README.md](./dt-cli/README.md).

## Tutorial

- [prerequisites](./docs/en/tutorial/prerequisites.md)
- [mysql -> mysql](./docs/en/tutorial/mysql_to_mysql.md)
- [pg -> pg](./docs/en/tutorial/pg_to_pg.md)
- [mongo -> mongo](./docs/en/tutorial/mongo_to_mongo.md)
- [redis -> redis](./docs/en/tutorial/redis_to_redis.md)
- [mysql -> starrocks](./docs/en/tutorial/mysql_to_starrocks.md)
- [mysql -> doris](./docs/en/tutorial/mysql_to_doris.md)
- [mysql -> clickhouse](./docs/en/tutorial/mysql_to_clickhouse.md)
- [mysql -> tidb](./docs/en/tutorial/mysql_to_tidb.md)
- [mysql -> kafka -> consumer](./docs/en/tutorial/mysql_to_kafka_consumer.md)
- [pg -> starrocks](./docs/en/tutorial/pg_to_starrocks.md)
- [pg -> doris](./docs/en/tutorial/pg_to_doris.md)
- [pg -> clickhouse](./docs/en/tutorial/pg_to_clickhouse.md)
- [pg -> kafka -> consumer](./docs/en/tutorial/pg_to_kafka_consumer.md)
- [snapshot + cdc without data loss](./docs/en/tutorial/snapshot_and_cdc_without_data_loss.md)
- [modify data by lua](./docs/en/tutorial/etl_by_lua.md)

## Run tests

Refer to [test docs](./dt-tests/README.md) for details.

# More docs

- Configurations
  - [config details](./docs/en/config.md)
- Structure tasks
  - [migration](./docs/en/structure/migration.md)
  - [check](./docs/en/structure/check.md)
  - [check by Liquibase](./docs/en/structure/check_by_liquibase.md)
- Snapshot tasks
  - [data migration](./docs/en/snapshot/migration.md)
  - [data check](./docs/en/snapshot/check.md)
  - [data revise](./docs/en/snapshot/revise.md)
  - [data review](./docs/en/snapshot/review.md)
  - [resume at breakpoint](./docs/en/snapshot/resume.md)
  - [multiple tables in parallel](./docs/en/snapshot/tb_in_parallel.md)
- CDC tasks
  - [data sync](./docs/en/cdc/sync.md)
  - [heartbeat to source database](./docs/en/cdc/heartbeat.md)
  - [two-way data sync](./docs/en/cdc/two_way.md)
  - [generate sqls from CDC](./docs/en/cdc/to_sql.md)
  - [resume at breakpoint](./docs/en/cdc/resume.md)
- Custom consumers
  - [mysql/pg -> kafka -> consumer](./docs/en/consumer/kafka_consumer.md)
- Data processing
  - [modify data by lua](./docs/en/etl/lua.md)
- Monitor
  - [monitor info](./docs/en/monitor/monitor.md)
  - [position info](./docs/en/monitor/position.md)
- Task templates
  - [mysql -> mysql](./docs/templates/mysql_to_mysql.md)
  - [pg -> pg](./docs/templates/pg_to_pg.md)
  - [mongo -> mongo](./docs/templates/mongo_to_mongo.md)
  - [redis -> redis](./docs/templates/redis_to_redis.md)
  - [mysql/pg -> kafka](./docs/templates/rdb_to_kafka.md)
  - [mysql -> starrocks](./docs/templates/mysql_to_starrocks.md)
  - [mysql -> doris](./docs/templates/mysql_to_doris.md)
  - [mysql -> clickhouse](./docs/templates/mysql_to_clickhouse.md)
  - [pg -> starrocks](./docs/templates/pg_to_starrocks.md)
  - [pg -> doris](./docs/templates/pg_to_doris.md)
  - [pg -> clickhouse](./docs/templates/pg_to_clickhouse.md)

# Benchmark

- MySQL -> MySQL, Snapshot

| Method   | Node Specs | RPS(rows per second) | Source MySQL Load (CPU/Memory) | Target MySQL Load (CPU/Memory) |
| :------- | :--------- | :------------------- | :----------------------------- | :----------------------------- |
| ape_dts  | 1c2g       | 71428                | 8.2% / 5.2%                    | 211% / 5.1%                    |
| ape_dts  | 2c4g       | 99403                | 14.0% / 5.2%                   | 359% / 5.1%                    |
| ape_dts  | 4c8g       | 126582               | 13.8% / 5.2%                   | 552% / 5.1%                    |
| debezium | 4c8g       | 4051                 | 21.5% / 5.2%                   | 51.2% / 5.1%                   |

- MySQL -> MySQL, CDC

| Method   | Node Specs | RPS(rows per second) | Source MySQL Load (CPU/Memory) | Target MySQL Load (CPU/Memory) |
| :------- | :--------- | :------------------- | :----------------------------- | :----------------------------- |
| ape_dts  | 1c2g       | 15002                | 18.8% / 5.2%                   | 467% / 6.5%                    |
| ape_dts  | 2c4g       | 24692                | 18.1% / 5.2%                   | 687% / 6.5%                    |
| ape_dts  | 4c8g       | 26287                | 18.2% / 5.2%                   | 685% / 6.5%                    |
| debezium | 4c8g       | 2951                 | 20.4% / 5.2%                   | 98% / 6.5%                     |

- Image size

| ape_dts:2.0.25-alpha.1 | debezium/connect:2.7 |
| :--------------------- | :------------------- |
| 71.4 MB                | 1.38 GB              |

- more benchmark [details](./docs/en/benchmark.md)

# Contributing

## Structure

![Structure](docs/img/structure.png)

## Modules

- dt-main: program entry
- dt-precheck: pre-check, to minimize interruptions during subsequent data operations by identifying issues early for fast failure
- dt-connector: extractors + sinkers for databases
- dt-pipeline: pipeline to connect extractors and sinkers
- dt-parallelizer: parallel algorithms
- dt-task: create extractors + sinkers + pipelines + parallelizers according to configurations
- dt-common: common utils, basic data structures, metadata management
- dt-tests: integration tests

- related sub module: [mysql binlog connector in rust](https://github.com/apecloud/mysql-binlog-connector-rust)

## Build

- Minimum supported Rust version (MSRV)
  The current minimum supported Rust version (MSRV) is 1.85.0.
- cargo build
- [build images](./docs/en/build_images.md)

## Checklist

- run `cargo clippy --all-targets --all-features --workspace` fix all clippy issues.

## Community

If you have any questions, you can reach out to us through:

- ApeDTS GitHub [Discussions](https://github.com/apecloud/ape-dts/discussions)
- ApeDTS Wechat Account with note **ape-dts**:

  <img src=".\docs\img\wechat-assistant.png" alt="wechat" width="100" height="100" style="margin-top:10px">
