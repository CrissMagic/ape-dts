use std::collections::HashMap;

use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use serde_json::{json, Value};

use super::cloudcanal_converter::CloudCanalConverter;
use crate::{
    config::json_template_type::JsonTemplateType,
    meta::{
        col_value::ColValue,
        ddl_meta::ddl_data::DdlData,
        rdb_meta_manager::{RdbMetaManager, RDB_PRIMARY_KEY},
        row_data::RowData,
        row_type::RowType,
    },
};

#[derive(Clone)]
pub struct JsonConverter {
    pub meta_manager: Option<RdbMetaManager>,
    pub cloudcanal_converter: Option<CloudCanalConverter>,
}

impl JsonConverter {
    pub fn new(meta_manager: Option<RdbMetaManager>) -> Self {
        Self::new_with_template(meta_manager, JsonTemplateType::Standard, None)
    }

    pub fn new_with_template(
        meta_manager: Option<RdbMetaManager>,
        template_type: JsonTemplateType,
        database_name: Option<String>,
    ) -> Self {
        let cloudcanal_converter = match template_type {
            JsonTemplateType::Standard => None,
            JsonTemplateType::CloudCanal => Some(CloudCanalConverter::new(
                meta_manager.clone(),
                database_name,
            )),
        };
        Self {
            meta_manager,
            cloudcanal_converter,
        }
    }

    pub fn refresh_meta(&mut self, data: &[DdlData]) {
        if let Some(meta_manager) = &mut self.meta_manager {
            for ddl_data in data {
                meta_manager.invalidate_cache_by_ddl_data(ddl_data);
            }
        }
        if let Some(converter) = &mut self.cloudcanal_converter {
            converter.refresh_meta(data);
        }
    }

    pub async fn row_data_to_json_key(&mut self, row_data: &RowData) -> Result<String> {
        if let Some(converter) = &mut self.cloudcanal_converter {
            return converter.row_data_to_json_key(row_data).await;
        }
        if let Some(meta_manager) = &mut self.meta_manager {
            // shortcut: preserve historical best-effort metadata and after-only JSON keys; revisit with consumer compatibility requirements.
            if let Ok(tb_meta) = meta_manager
                .get_tb_meta(&row_data.db, &row_data.schema, &row_data.tb)
                .await
            {
                if let Some(primary_key) = tb_meta.key_map.get(RDB_PRIMARY_KEY) {
                    let values: Vec<Value> = primary_key
                        .iter()
                        .filter_map(|col| row_data.after.as_ref()?.get(col))
                        .map(col_value_to_json_value)
                        .collect();
                    return Ok(serde_json::to_string(&values)?);
                }
            }
        }
        Ok(format!("{}_{}", row_data.schema, row_data.tb))
    }

    pub async fn row_data_to_json_value(&mut self, row_data: &RowData) -> Result<String> {
        if let Some(converter) = &mut self.cloudcanal_converter {
            return converter.row_data_to_json_value(row_data).await;
        }
        let mut message = json!({
            "operation": match row_data.row_type {
                RowType::Insert => "insert",
                RowType::Update => "update",
                RowType::Delete => "delete",
            },
            "schema": row_data.schema,
            "tb": row_data.tb,
            "before": null,
            "after": null,
        });
        if let Some(before) = &row_data.before {
            message["before"] = col_values_to_json_value(before);
        }
        if let Some(after) = &row_data.after {
            message["after"] = col_values_to_json_value(after);
        }
        Ok(serde_json::to_string(&message)?)
    }

    pub async fn ddl_data_to_json_value(&mut self, ddl_data: DdlData) -> Result<String> {
        if let Some(converter) = &mut self.cloudcanal_converter {
            return converter.ddl_data_to_json_value(ddl_data).await;
        }
        Ok(serde_json::to_string(&json!({
            "ddl": true,
            "db_type": ddl_data.db_type.to_string(),
            "ddl_type": ddl_data.ddl_type.to_string(),
            "schema": ddl_data.default_schema,
            "query": ddl_data.query,
        }))?)
    }
}

fn col_values_to_json_value(values: &HashMap<String, ColValue>) -> Value {
    Value::Object(
        values
            .iter()
            .map(|(col, value)| (col.clone(), col_value_to_json_value(value)))
            .collect(),
    )
}

fn col_value_to_json_value(value: &ColValue) -> Value {
    match value {
        ColValue::None | ColValue::UnchangedToast => Value::Null,
        ColValue::Bool(value) => json!(value),
        ColValue::Tiny(value) => json!(value),
        ColValue::UnsignedTiny(value) => json!(value),
        ColValue::Short(value) => json!(value),
        ColValue::UnsignedShort(value) => json!(value),
        ColValue::Long(value) => json!(value),
        ColValue::UnsignedLong(value) => json!(value),
        ColValue::LongLong(value) => json!(value),
        ColValue::UnsignedLongLong(value) => json!(value),
        ColValue::Float(value) => serde_json::Number::from_f64(*value as f64)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(value.to_string())),
        ColValue::Double(value) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(value.to_string())),
        ColValue::Decimal(value)
        | ColValue::String(value)
        | ColValue::Date(value)
        | ColValue::Time(value)
        | ColValue::DateTime(value)
        | ColValue::Timestamp(value)
        | ColValue::Set2(value)
        | ColValue::Enum2(value)
        | ColValue::Json2(value) => Value::String(value.clone()),
        ColValue::Json(value) => {
            Value::String(String::from_utf8(value.clone()).unwrap_or_default())
        }
        ColValue::Json3(value) => Value::String(value.to_string()),
        ColValue::Blob(value) => Value::String(general_purpose::STANDARD.encode(value)),
        ColValue::RawString(value) => Value::String(String::from_utf8_lossy(value).to_string()),
        ColValue::MongoDoc(value) => Value::String(value.to_string()),
        ColValue::MongoRawDoc(value) => {
            Value::String(general_purpose::STANDARD.encode(value.as_bytes()))
        }
        ColValue::Enum(value) => Value::String(value.to_string()),
        ColValue::Set(value) => Value::String(value.to_string()),
        ColValue::Year(value) => json!(value),
        ColValue::Bit(value) => Value::String(value.to_string()),
        ColValue::Spatial { srid, wkt } => Value::String(format!("{srid}|{wkt}")),
    }
}

#[cfg(test)]
mod tests {
    use mongodb::bson::{doc, raw::RawDocumentBuf};
    use sqlx::postgres::PgPoolOptions;

    use super::*;
    use crate::{
        config::{
            config_enums::DbType, message_format::MessageFormat, sinker_config::SinkerConfig,
            task_config::TaskConfig,
        },
        meta::{
            ddl_meta::ddl_type::DdlType,
            pg::{
                pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta, type_registry::TypeRegistry,
            },
            rdb_tb_meta::RdbTbMeta,
        },
    };

    fn configured_converter(options: &str, metadata: Option<RdbMetaManager>) -> JsonConverter {
        let config = TaskConfig::new_from_str(&format!(
            "[extractor]\ndb_type=pg\nextract_type=cdc\nurl=postgres://localhost/data_demo\nslot_name=sample\n\n[sinker]\ndb_type=kafka\nsink_type=write\nurl=localhost:9092\n{options}\n\n[router]\ntopic_map=*.*:sample\n\n[parallelizer]\nparallel_type=serial\n"
        ))
        .unwrap();
        let SinkerConfig::Kafka {
            message_format,
            json_template,
            ..
        } = config.sinker
        else {
            panic!("expected Kafka sinker config");
        };
        assert_ne!(message_format, MessageFormat::Avro);
        JsonConverter::new_with_template(
            metadata,
            message_format.json_template(json_template),
            Some("fallback_db".to_string()),
        )
    }

    fn sample_row() -> RowData {
        RowData::new(
            "data_demo".to_string(),
            "public".to_string(),
            "cdc_sample".to_string(),
            0,
            RowType::Insert,
            None,
            Some(HashMap::from([
                ("tenant".to_string(), ColValue::Long(3)),
                ("id".to_string(), ColValue::LongLong(7)),
                ("name".to_string(), ColValue::String("after".to_string())),
            ])),
        )
    }

    fn cached_pg_metadata() -> RdbMetaManager {
        let pool = PgPoolOptions::new()
            .min_connections(0)
            .connect_lazy("postgres://localhost/data_demo")
            .unwrap();
        let mut manager = PgMetaManager {
            conn_pool: pool.clone(),
            type_registry: TypeRegistry::new(pool),
            name_to_tb_meta: HashMap::new(),
            oid_to_tb_meta: HashMap::new(),
        };
        manager
            .update_tb_meta_by_oid(
                1,
                PgTbMeta {
                    oid: 1,
                    basic: RdbTbMeta {
                        db: "data_demo".to_string(),
                        schema: "public".to_string(),
                        tb: "cdc_sample".to_string(),
                        cols: ["tenant", "id", "name", "updated_at", "attributes", "salary"]
                            .map(String::from)
                            .to_vec(),
                        key_map: HashMap::from([(
                            RDB_PRIMARY_KEY.to_string(),
                            vec!["tenant".to_string(), "id".to_string()],
                        )]),
                        col_origin_type_map: HashMap::from([
                            ("tenant".to_string(), "integer".to_string()),
                            ("id".to_string(), "bigint".to_string()),
                            ("name".to_string(), "text".to_string()),
                            (
                                "updated_at".to_string(),
                                "timestamp with time zone".to_string(),
                            ),
                            ("attributes".to_string(), "jsonb".to_string()),
                            ("salary".to_string(), "numeric".to_string()),
                        ]),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .unwrap();
        RdbMetaManager::from_pg(manager)
    }

    #[tokio::test]
    async fn cloudcanal_update_and_delete_preserve_before_and_data() {
        let mut converter =
            configured_converter("message_format=json\njson_template=cloudcanal", None);
        let mut row = sample_row();
        row.row_type = RowType::Update;
        row.before = Some(HashMap::from([
            ("id".to_string(), ColValue::LongLong(7)),
            ("name".to_string(), ColValue::String("before".to_string())),
        ]));
        let message: Value =
            serde_json::from_str(&converter.row_data_to_json_value(&row).await.unwrap()).unwrap();
        assert_eq!(message["action"], "UPDATE");
        assert_eq!(message["before"][0]["name"], "before");
        assert_eq!(message["data"][0]["name"], "after");
        assert_eq!(message["ddl"], false);

        row.row_type = RowType::Delete;
        row.after = None;
        let message: Value =
            serde_json::from_str(&converter.row_data_to_json_value(&row).await.unwrap()).unwrap();
        assert_eq!(message["action"], "DELETE");
        assert_eq!(message["before"][0]["id"], 7);
        assert!(message.get("data").is_none());
    }

    #[tokio::test]
    async fn standard_json_preserves_legacy_row_and_ddl_output() {
        let mut converter = configured_converter("message_format=json", None);
        let mut row = sample_row();
        row.after
            .as_mut()
            .unwrap()
            .insert("payload".to_string(), ColValue::Json3(json!({"ok": true})));
        let insert: Value =
            serde_json::from_str(&converter.row_data_to_json_value(&row).await.unwrap()).unwrap();
        assert_eq!(insert["operation"], "insert");
        assert_eq!(insert["after"]["name"], "after");
        assert_eq!(insert["after"]["payload"], r#"{"ok":true}"#);
        assert!(insert["before"].is_null());
        row.row_type = RowType::Update;
        row.before = Some(HashMap::from([(
            "name".to_string(),
            ColValue::String("before".to_string()),
        )]));
        let update: Value =
            serde_json::from_str(&converter.row_data_to_json_value(&row).await.unwrap()).unwrap();
        assert_eq!(update["operation"], "update");
        assert_eq!(update["before"]["name"], "before");
        assert_eq!(update["after"]["name"], "after");
        row.row_type = RowType::Delete;
        row.after = None;
        let delete: Value =
            serde_json::from_str(&converter.row_data_to_json_value(&row).await.unwrap()).unwrap();
        assert_eq!(delete["operation"], "delete");
        assert!(delete["after"].is_null());
        let ddl: Value = serde_json::from_str(
            &converter
                .ddl_data_to_json_value(DdlData {
                    default_db: "data_demo".to_string(),
                    default_schema: "public".to_string(),
                    query: "CREATE TABLE cdc_sample (id bigint)".to_string(),
                    db_type: DbType::Pg,
                    ddl_type: DdlType::CreateTable,
                    ..Default::default()
                })
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(ddl["ddl"], true);
        assert_eq!(ddl["db_type"], "pg");
        assert_eq!(ddl["query"], "CREATE TABLE cdc_sample (id bigint)");
    }

    #[tokio::test]
    async fn cloudcanal_aliases_select_the_same_template() {
        for options in [
            "message_format=cloudcanal",
            "message_format=json_template:cloudcanal\njson_template=standard",
        ] {
            let mut converter = configured_converter(options, None);
            let message: Value = serde_json::from_str(
                &converter
                    .row_data_to_json_value(&sample_row())
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(message["action"], "INSERT");
            assert_eq!(message["data"][0]["id"], 7);
        }
    }

    #[tokio::test]
    async fn cloudcanal_uses_row_and_ddl_database_before_configured_fallback() {
        let mut converter =
            configured_converter("message_format=json\njson_template=cloudcanal", None);
        let mut row = sample_row();
        for (database, expected) in [("data_demo", "data_demo"), ("", "fallback_db")] {
            row.db = database.to_string();
            let message: Value =
                serde_json::from_str(&converter.row_data_to_json_value(&row).await.unwrap())
                    .unwrap();
            assert_eq!(message["db"], expected);
            let ddl: Value = serde_json::from_str(
                &converter
                    .ddl_data_to_json_value(DdlData {
                        default_db: database.to_string(),
                        default_schema: "public".to_string(),
                        query: "ALTER TABLE cdc_sample ADD COLUMN note text".to_string(),
                        db_type: DbType::Pg,
                        ddl_type: DdlType::AlterTable,
                        ..Default::default()
                    })
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(ddl["action"], "DDL");
            assert_eq!(ddl["db"], expected);
            assert_eq!(ddl["schema"], "public");
            assert_eq!(ddl["sql"], "ALTER TABLE cdc_sample ADD COLUMN note text");
            assert_eq!(ddl["data"], json!([]));
        }
    }

    #[tokio::test]
    async fn cached_pg_primary_key_drives_json_keys_and_cloudcanal_field_metadata() {
        for (options, cloudcanal) in [
            ("message_format=json", false),
            ("message_format=json\njson_template=cloudcanal", true),
        ] {
            let mut converter = configured_converter(options, Some(cached_pg_metadata()));
            let mut row = sample_row();
            assert_eq!(converter.row_data_to_json_key(&row).await.unwrap(), "[3,7]");
            if cloudcanal {
                let message: Value =
                    serde_json::from_str(&converter.row_data_to_json_value(&row).await.unwrap())
                        .unwrap();
                assert_eq!(message["pks"], json!(["tenant", "id"]));
                assert_eq!(message["dbValType"]["id"], "bigint");
                assert_eq!(
                    message["jdbcType"],
                    json!({
                        "tenant": 4, "id": -5, "name": 12, "updated_at": 93, "attributes": 1111,
                        "salary": 12,
                    })
                );
            }
            row.row_type = RowType::Delete;
            row.before = row.after.take();
            assert_eq!(converter.row_data_to_json_key(&row).await.unwrap(), "[]");
        }
    }

    #[tokio::test]
    async fn unavailable_metadata_preserves_historical_best_effort_json_output() {
        let mut converter = configured_converter(
            "message_format=json\njson_template=cloudcanal",
            Some(RdbMetaManager::default()),
        );
        let row = sample_row();
        assert_eq!(
            converter.row_data_to_json_key(&row).await.unwrap(),
            "public_cdc_sample"
        );
        let message: Value =
            serde_json::from_str(&converter.row_data_to_json_value(&row).await.unwrap()).unwrap();
        assert_eq!(message["pks"], json!([]));
        assert!(message.get("dbValType").is_none());
        assert_eq!(message["data"][0]["id"], 7);
    }

    #[tokio::test]
    async fn json_templates_support_new_column_values_and_keep_non_finite_number_semantics() {
        let raw_document = RawDocumentBuf::from_document(&doc! {"id": 7}).unwrap();
        for (options, non_finite, cloudcanal) in [
            ("message_format=json", json!("NaN"), false),
            (
                "message_format=json\njson_template=cloudcanal",
                json!(0),
                true,
            ),
        ] {
            let mut converter = configured_converter(options, None);
            let mut row = sample_row();
            row.after = Some(HashMap::from([
                (
                    "shape".to_string(),
                    ColValue::Spatial {
                        srid: 4326,
                        wkt: "POINT (1 2)".to_string(),
                    },
                ),
                (
                    "document".to_string(),
                    ColValue::MongoRawDoc(raw_document.clone()),
                ),
                ("non_finite".to_string(), ColValue::Double(f64::NAN)),
                (
                    "decimal".to_string(),
                    ColValue::Decimal("1234567890.123456789".to_string()),
                ),
                (
                    "time".to_string(),
                    ColValue::Timestamp("2026-10-13 08:00:00".to_string()),
                ),
            ]));
            let message: Value =
                serde_json::from_str(&converter.row_data_to_json_value(&row).await.unwrap())
                    .unwrap();
            let values = if cloudcanal {
                &message["data"][0]
            } else {
                &message["after"]
            };
            assert_eq!(values["shape"], "4326|POINT (1 2)");
            assert_eq!(values["document"], "DQAAABBpZAAHAAAAAA==");
            assert_eq!(values["non_finite"], non_finite);
            assert_eq!(values["decimal"], "1234567890.123456789");
            assert_eq!(values["time"], "2026-10-13 08:00:00");
        }
    }
}
