use std::collections::HashMap;

use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use serde_json::{json, Value};

use crate::meta::{
    col_value::ColValue,
    ddl_meta::ddl_data::DdlData,
    rdb_meta_manager::{RdbMetaManager, RDB_PRIMARY_KEY},
    row_data::RowData,
    row_type::RowType,
};

#[derive(Clone)]
pub struct CloudCanalConverter {
    pub meta_manager: Option<RdbMetaManager>,
    pub database_name: Option<String>,
}

impl CloudCanalConverter {
    pub fn new(meta_manager: Option<RdbMetaManager>, database_name: Option<String>) -> Self {
        Self {
            meta_manager,
            database_name,
        }
    }

    pub fn refresh_meta(&mut self, data: &[DdlData]) {
        if let Some(meta_manager) = &mut self.meta_manager {
            for ddl_data in data {
                meta_manager.invalidate_cache_by_ddl_data(ddl_data);
            }
        }
    }

    pub async fn row_data_to_json_key(&mut self, row_data: &RowData) -> Result<String> {
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
        let database_name = if !row_data.db.is_empty() {
            row_data.db.clone()
        } else {
            self.database_name
                .clone()
                .unwrap_or_else(|| row_data.schema.clone())
        };
        let mut message = json!({
            "action": match row_data.row_type {
                RowType::Insert => "INSERT",
                RowType::Update => "UPDATE",
                RowType::Delete => "DELETE",
            },
            "bid": 0,
            "db": database_name,
            "schema": row_data.schema,
            "table": row_data.tb,
            "ddl": false,
            "entryType": "ROWDATA",
            "execTs": chrono::Utc::now().timestamp_millis(),
            "sendTs": chrono::Utc::now().timestamp_millis(),
            "sql": null,
            "pks": [],
        });
        if let Some(before) = &row_data.before {
            message["before"] = json!([col_values_to_json_value(before)]);
        }
        if let Some(after) = &row_data.after {
            message["data"] = json!([col_values_to_json_value(after)]);
        }

        if let Some(meta_manager) = &mut self.meta_manager {
            let is_mysql = meta_manager.mysql_meta_manager.is_some();
            let is_pg = meta_manager.pg_meta_manager.is_some();
            if let Ok(tb_meta) = meta_manager
                .get_tb_meta(&row_data.db, &row_data.schema, &row_data.tb)
                .await
            {
                if let Some(primary_key) = tb_meta.key_map.get(RDB_PRIMARY_KEY) {
                    message["pks"] = json!(primary_key);
                }
                let mut db_val_type = serde_json::Map::new();
                let mut jdbc_type = serde_json::Map::new();
                for col in &tb_meta.cols {
                    if let Some(origin_type) = tb_meta.col_origin_type_map.get(col) {
                        if is_mysql || is_pg {
                            db_val_type.insert(col.clone(), Value::String(origin_type.clone()));
                            // shortcut: retain the historical common-type JDBC mapping; extend when consumers require additional types.
                            let code = match origin_type.to_ascii_lowercase().as_str() {
                                value if value.contains("bigint") => -5,
                                value if is_mysql && value.contains("int") => 4,
                                value if is_pg && value.contains("integer") => 4,
                                value if value.contains("varchar") || value.contains("text") => 12,
                                value
                                    if value.contains("timestamp")
                                        || (is_mysql && value.contains("datetime")) =>
                                {
                                    93
                                }
                                value if value.contains("json") => 1111,
                                _ => 12,
                            };
                            jdbc_type.insert(col.clone(), json!(code));
                        }
                    }
                }
                message["dbValType"] = Value::Object(db_val_type);
                message["jdbcType"] = Value::Object(jdbc_type);
            }
        }
        Ok(serde_json::to_string(&message)?)
    }

    pub async fn ddl_data_to_json_value(&mut self, ddl_data: DdlData) -> Result<String> {
        let database_name = if !ddl_data.default_db.is_empty() {
            ddl_data.default_db.clone()
        } else {
            self.database_name
                .clone()
                .unwrap_or_else(|| ddl_data.default_schema.clone())
        };
        Ok(serde_json::to_string(&json!({
            "action": "DDL",
            "bid": 0,
            "db": database_name,
            "schema": ddl_data.default_schema,
            "table": "",
            "ddl": true,
            "entryType": "DDL",
            "execTs": chrono::Utc::now().timestamp_millis(),
            "sendTs": chrono::Utc::now().timestamp_millis(),
            "sql": ddl_data.query,
            "pks": [],
            "before": [],
            "data": [],
            "dbValType": {},
            "jdbcType": {},
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
        // shortcut: preserve historical CloudCanal non-finite float-as-zero output; change only after agreeing consumer semantics.
        ColValue::Float(value) => serde_json::Number::from_f64(*value as f64)
            .map(Value::Number)
            .unwrap_or_else(|| json!(0)),
        ColValue::Double(value) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or_else(|| json!(0)),
        ColValue::Decimal(value)
        | ColValue::String(value)
        | ColValue::Date(value)
        | ColValue::Time(value)
        | ColValue::DateTime(value)
        | ColValue::Timestamp(value)
        | ColValue::Set2(value)
        | ColValue::Enum2(value) => Value::String(value.clone()),
        ColValue::Blob(value) => Value::String(general_purpose::STANDARD.encode(value)),
        ColValue::Json(value) => {
            let text = String::from_utf8_lossy(value);
            serde_json::from_str(&text).unwrap_or_else(|_| Value::String(text.to_string()))
        }
        ColValue::Json2(value) => {
            serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.clone()))
        }
        ColValue::Json3(value) => value.clone(),
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
