use super::Encoder;
use crate::flowgger::config::Config;
use crate::flowgger::record::{Record, SDValue};
use serde_json::map::Map;
use serde_json::value::Value;

#[derive(Clone)]
pub struct GelfEncoder {
    extra: Vec<(String, String)>,
}

impl GelfEncoder {
    pub fn new(config: &Config) -> GelfEncoder {
        let extra = match config.lookup("output.gelf_extra") {
            None => Vec::new(),
            Some(extra) => extra
                .as_table()
                .expect("output.gelf_extra must be a list of key/value pairs")
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.to_owned(),
                        v.as_str()
                            .expect("output.gelf_extra values must be strings")
                            .to_owned(),
                    )
                }).collect(),
        };
        GelfEncoder { extra }
    }
}

impl Encoder for GelfEncoder {
    fn encode(&self, record: Record) -> Result<Vec<u8>, &'static str> {
        let mut map = Map::new();
            map.insert("version".to_owned(), Value::String("1.1".to_owned()));
            map.insert(
                "host".to_owned(),
                Value::String(if record.hostname.is_empty() {
                    "unknown".to_owned()
                } else {
                    record.hostname.clone().to_owned()
                }));
            map.insert(
                "short_message".to_owned(),
                Value::String(record.msg.unwrap_or_else(|| "-".to_owned()))
            );
            map.insert("timestamp".to_owned(), record.ts.into());
        if let Some(severity) = record.severity {
            map.insert("level".to_owned(), u64::from(severity).into());
        }
        if let Some(full_msg) = record.full_msg {
            map.insert("full_message".to_owned(), Value::String(full_msg));
        }
        if let Some(appname) = record.appname {
            map.insert("application_name".to_owned(), Value::String(appname));
        }
        if let Some(procid) = record.procid {
            map.insert("process_id".to_owned(), Value::String(procid));
        }
        for (name, value) in self.extra.iter().cloned() {
            map.insert(name, Value::String(value));
        }
        if let Some(sd) = record.sd {
            if let Some(sd_id) = sd.sd_id {
                map.insert("sd_id".to_owned(), Value::String(sd_id));
            }
            for (name, value) in sd.pairs {
                let value = match value {
                    SDValue::String(value) => Value::String(value),
                    SDValue::Bool(value) => Value::Bool(value),
                    SDValue::F64(value) => value.into(),
                    SDValue::I64(value) => value.into(),
                    SDValue::U64(value) => value.into(),
                    SDValue::Null => Value::Null,
                };
                map.insert(name, value);
            }
        }
        let json = serde_json::to_vec(&map).or(Err("Unable to serialize to JSON"))?;
        Ok(json)
    }
}
