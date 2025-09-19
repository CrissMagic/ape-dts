use std::collections::HashMap;

use anyhow::Result;
use serde_json::{json, Value};

use crate::{
    config::config_enums::DbType,
    meta::{
        col_value::ColValue,
        ddl_meta::{ddl_data::DdlData, ddl_type::DdlType},
        rdb_meta_manager::RdbMetaManager,
        row_data::RowData,
        row_type::RowType,
    },
};

#[derive(Clone)]
pub struct JsonConverter {
    pub meta_manager: Option<RdbMetaManager>,
}

impl JsonConverter {
    pub fn new(meta_manager: Option<RdbMetaManager>) -> Self {
        JsonConverter { meta_manager }
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
            if let Ok(tb_meta) = meta_manager.get_tb_meta(&row_data.schema, &row_data.tb).await {
                if let Some(primary_key) = tb_meta.key_map.get("primary") {
                    let mut key_values = Vec::new();
                    for pk_col in primary_key {
                        if let Some(col_value) = row_data.after.as_ref().and_then(|after| after.get(pk_col)) {
                            key_values.push(col_value_to_json_value(col_value));
                        }
                    }
                    return Ok(serde_json::to_string(&key_values)?);
                }
            }
        }
        Ok(format!("{}_{}", row_data.schema, row_data.tb))
    }

    pub async fn row_data_to_json_value(&mut self, row_data: RowData) -> Result<String> {
        let mut json_obj = json!({
            "operation": row_data.row_type.to_string(),
            "schema": row_data.schema,
            "tb": row_data.tb,
        });

        if let Some(before) = &row_data.before {
            json_obj["before"] = col_values_to_json_value(before);
        }

        if let Some(after) = &row_data.after {
            json_obj["after"] = col_values_to_json_value(after);
        }

        Ok(serde_json::to_string(&json_obj)?)
    }

    pub async fn ddl_data_to_json_value(&mut self, ddl_data: DdlData) -> Result<String> {
        let json_obj = json!({
            "ddl": true,
            "db_type": ddl_data.db_type.to_string(),
            "ddl_type": ddl_data.ddl_type.to_string(),
            "schema": ddl_data.default_schema,
            "query": ddl_data.query,
        });

        Ok(serde_json::to_string(&json_obj)?)
    }
}

fn col_values_to_json_value(col_values: &HashMap<String, ColValue>) -> Value {
    let mut json_map = serde_json::Map::new();
    for (key, value) in col_values {
        json_map.insert(key.clone(), col_value_to_json_value(value));
    }
    Value::Object(json_map)
}

fn col_value_to_json_value(value: &ColValue) -> Value {
    match value {
        ColValue::None => Value::Null,
        ColValue::Bool(v) => Value::Bool(*v),
        ColValue::Tiny(v) => Value::Number((*v).into()),
        ColValue::UnsignedTiny(v) => Value::Number((*v).into()),
        ColValue::Short(v) => Value::Number((*v).into()),
        ColValue::UnsignedShort(v) => Value::Number((*v).into()),
        ColValue::Long(v) => Value::Number((*v).into()),
        ColValue::UnsignedLong(v) => Value::Number((*v).into()),
        ColValue::LongLong(v) => Value::Number((*v).into()),
        ColValue::UnsignedLongLong(v) => Value::Number((*v).into()),
        ColValue::Float(v) => {
            if let Some(num) = serde_json::Number::from_f64(*v as f64) {
                Value::Number(num)
            } else {
                Value::String(v.to_string())
            }
        }
        ColValue::Double(v) => {
            if let Some(num) = serde_json::Number::from_f64(*v) {
                Value::Number(num)
            } else {
                Value::String(v.to_string())
            }
        }
        ColValue::Decimal(v) => Value::String(v.clone()),
        ColValue::Time(v) => Value::String(v.clone()),
        ColValue::Date(v) => Value::String(v.clone()),
        ColValue::DateTime(v) => Value::String(v.clone()),
        ColValue::Timestamp(v) => Value::String(v.clone()),
        ColValue::Year(v) => Value::Number((*v).into()),
        ColValue::String(v) => Value::String(v.clone()),
        ColValue::RawString(v) => {
            // Convert bytes to base64 string for JSON compatibility
            Value::String(base64::encode(v))
        }
        ColValue::Blob(v) => {
            // Convert bytes to base64 string for JSON compatibility
            Value::String(base64::encode(v))
        }
        ColValue::Bit(v) => Value::Number((*v).into()),
        ColValue::Set(v) => Value::Number((*v).into()),
        ColValue::Set2(v) => Value::String(v.clone()),
        ColValue::Enum(v) => Value::Number((*v).into()),
        ColValue::Enum2(v) => Value::String(v.clone()),
        ColValue::Json(v) => {
            // Try to parse as JSON, fallback to base64 string
            if let Ok(json_val) = serde_json::from_slice::<Value>(v) {
                json_val
            } else {
                Value::String(base64::encode(v))
            }
        }
        ColValue::Json2(v) => {
            // Try to parse as JSON, fallback to string
            if let Ok(json_val) = serde_json::from_str::<Value>(v) {
                json_val
            } else {
                Value::String(v.clone())
            }
        }
        ColValue::Json3(v) => v.clone(),
        ColValue::MongoDoc(_) => Value::Null, // MongoDB documents not supported in JSON format
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_row_data_to_json() {
        let mut json_converter = JsonConverter::new(None);
        
        let mut after = HashMap::new();
        after.insert("id".to_string(), ColValue::Long(123));
        after.insert("name".to_string(), ColValue::String("test".to_string()));
        after.insert("active".to_string(), ColValue::Bool(true));

        let row_data = RowData {
            schema: "test_schema".to_string(),
            tb: "test_table".to_string(),
            row_type: RowType::Insert,
            before: None,
            after: Some(after),
            data_size: 100,
        };

        let result = json_converter.row_data_to_json_value(row_data).await;
        assert!(result.is_ok());
        
        let json_str = result.unwrap();
        let parsed: Value = serde_json::from_str(&json_str).unwrap();
        
        assert_eq!(parsed["operation"], "insert");
        assert_eq!(parsed["schema"], "test_schema");
        assert_eq!(parsed["tb"], "test_table");
        assert!(parsed["after"].is_object());
    }

    #[tokio::test]
    async fn test_ddl_data_to_json() {
        let mut json_converter = JsonConverter::new(None);
        
        let ddl_data = DdlData {
            default_schema: "test_schema".to_string(),
            query: "CREATE TABLE test (id INT)".to_string(),
            ddl_type: DdlType::CreateTable,
            db_type: DbType::Mysql,
            statement: Default::default(),
        };

        let result = json_converter.ddl_data_to_json_value(ddl_data).await;
        assert!(result.is_ok());
        
        let json_str = result.unwrap();
        let parsed: Value = serde_json::from_str(&json_str).unwrap();
        
        assert_eq!(parsed["ddl"], true);
        assert_eq!(parsed["db_type"], "mysql");
        assert_eq!(parsed["schema"], "test_schema");
        assert_eq!(parsed["query"], "CREATE TABLE test (id INT)");
    }
}