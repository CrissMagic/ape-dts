use std::collections::HashMap;

use anyhow::Result;
use serde_json::{json, Value};

use crate::{
    meta::{
        col_value::ColValue,
        ddl_meta::ddl_data::DdlData,
        rdb_meta_manager::RdbMetaManager,
        row_data::RowData,
        row_type::RowType,
    },
};

/// CloudCanal 格式的 JSON 转换器
/// 生成符合 CloudCanal 消费端系统要求的消息格式
#[derive(Clone)]
pub struct CloudCanalConverter {
    pub meta_manager: Option<RdbMetaManager>,
}

impl CloudCanalConverter {
    pub fn new(meta_manager: Option<RdbMetaManager>) -> Self {
        CloudCanalConverter { meta_manager }
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
        // 获取操作类型，映射到 CloudCanal 的 action 字段
        let action = match row_data.row_type {
            RowType::Insert => "INSERT",
            RowType::Update => "UPDATE", 
            RowType::Delete => "DELETE",
        };

        let database_name = if let Some(meta_manager) = &self.meta_manager {
            if meta_manager.mysql_meta_manager.is_some() {
                row_data.schema.clone()
            } else if meta_manager.pg_meta_manager.is_some() {
                // TODO: 从 PostgreSQL 连接配置中获取真实的数据库名称
                row_data.schema.clone()
            } else {
                row_data.schema.clone()
            }
        } else {
            row_data.schema.clone()
        };

        let mut json_obj = json!({
            "action": action,
            "bid": 0,
            "db": database_name,
            "schema": row_data.schema,
            "table": row_data.tb,
            "ddl": false,
            "entryType": "ROWDATA",
            "execTs": chrono::Utc::now().timestamp_millis(),
            "sendTs": chrono::Utc::now().timestamp_millis(),
            "sql": null,
            "pks": []
        });

        // 添加 before 数据（用于 UPDATE 和 DELETE 操作）
        if let Some(before) = &row_data.before {
            json_obj["before"] = json!([col_values_to_json_value(before)]);
        }

        // 添加 data 数据（用于 INSERT 和 UPDATE 操作）
        if let Some(after) = &row_data.after {
            json_obj["data"] = json!([col_values_to_json_value(after)]);
        }

        // 获取表的元数据信息，添加字段类型信息
        if let Some(meta_manager) = &mut self.meta_manager {
            // 在获取表元数据之前确定数据库类型
            let is_mysql = meta_manager.mysql_meta_manager.is_some();
            let is_pg = meta_manager.pg_meta_manager.is_some();
            
            if let Ok(tb_meta) = meta_manager.get_tb_meta(&row_data.schema, &row_data.tb).await {
                
                // 添加主键信息
                if let Some(primary_key) = tb_meta.key_map.get("primary") {
                    json_obj["pks"] = json!(primary_key);
                }

                // 添加字段类型信息
                let mut db_val_type = serde_json::Map::new();
                let mut jdbc_type = serde_json::Map::new();
                
                for col_name in &tb_meta.cols {
                    if let Some(col_origin_type) = tb_meta.col_origin_type_map.get(col_name) {
                        if is_mysql {
                            db_val_type.insert(col_name.clone(), Value::String(col_origin_type.clone()));
                            // MySQL JDBC 类型映射（简化版本）
                            let jdbc_type_code = match col_origin_type.to_lowercase().as_str() {
                                s if s.contains("bigint") => -5,
                                s if s.contains("int") => 4,
                                s if s.contains("varchar") || s.contains("text") => 12,
                                s if s.contains("timestamp") || s.contains("datetime") => 93,
                                s if s.contains("json") => 1111,
                                _ => 12, // 默认为 VARCHAR
                            };
                            jdbc_type.insert(col_name.clone(), Value::Number(jdbc_type_code.into()));
                        } else if is_pg {
                            db_val_type.insert(col_name.clone(), Value::String(col_origin_type.clone()));
                            // PostgreSQL JDBC 类型映射（简化版本）
                            let jdbc_type_code = match col_origin_type.to_lowercase().as_str() {
                                s if s.contains("bigint") => -5,
                                s if s.contains("integer") => 4,
                                s if s.contains("varchar") || s.contains("text") => 12,
                                s if s.contains("timestamp") => 93,
                                s if s.contains("json") => 1111,
                                _ => 12, // 默认为 VARCHAR
                            };
                            jdbc_type.insert(col_name.clone(), Value::Number(jdbc_type_code.into()));
                        }
                    }
                }
                
                json_obj["dbValType"] = Value::Object(db_val_type);
                json_obj["jdbcType"] = Value::Object(jdbc_type);
            }
        }

        Ok(serde_json::to_string(&json_obj)?)
    }

    pub async fn ddl_data_to_json_value(&mut self, ddl_data: DdlData) -> Result<String> {
        let json_obj = json!({
            "action": "DDL",
            "bid": 0,
            "db": ddl_data.default_schema,
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
            "jdbcType": {}
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
        ColValue::Float(v) => Value::Number(serde_json::Number::from_f64(*v as f64).unwrap_or_else(|| serde_json::Number::from(0))),
        ColValue::Double(v) => Value::Number(serde_json::Number::from_f64(*v).unwrap_or_else(|| serde_json::Number::from(0))),
        ColValue::Decimal(v) => Value::String(v.clone()),
        ColValue::String(v) => Value::String(v.clone()),
        ColValue::Blob(v) => Value::String(base64::encode(v)),
        ColValue::Date(v) => Value::String(v.clone()),
        ColValue::Time(v) => Value::String(v.clone()),
        ColValue::DateTime(v) => Value::String(v.clone()),
        ColValue::Timestamp(v) => Value::String(v.clone()),
        ColValue::Json(v) => {
            // Convert Vec<u8> to String first
            let json_str = String::from_utf8_lossy(v);
            serde_json::from_str(&json_str).unwrap_or_else(|_| Value::String(json_str.to_string()))
        },
        ColValue::Json2(v) => {
            serde_json::from_str(v).unwrap_or_else(|_| Value::String(v.clone()))
        },
        ColValue::Json3(v) => v.clone(),
        ColValue::RawString(v) => Value::String(String::from_utf8_lossy(v).to_string()),
        ColValue::Set2(v) => Value::String(v.clone()),
        ColValue::Enum2(v) => Value::String(v.clone()),
        ColValue::MongoDoc(v) => Value::String(v.to_string()),
        ColValue::Enum(v) => Value::String(v.to_string()),
        ColValue::Set(v) => Value::String(v.to_string()),
        ColValue::Year(v) => Value::Number((*v).into()),
        ColValue::Bit(v) => Value::String(v.to_string()),
    }
}