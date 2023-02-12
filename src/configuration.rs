use config::{Config, FileFormat, File};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Settings {
    pub database: String,
    excluded_schema_list: Vec<String>,
    pub excluded_schemas: String,
    pub log_dir: String,
    pub vacuum: bool,
    pub reindex: bool,
    pub refresh_matviews: bool,
}

impl Settings {
    pub fn builder() -> Self {
        let mut settings = Config::builder()
            .set_default("excluded_schemas", "").expect("Failed to set default")
            .add_source(File::new("config.yml", FileFormat::Yaml))
            .build()
            .expect("Failed to read config")
            .try_deserialize::<Settings>()
            .expect("Failed to deserialize settings");
        
        settings.excluded_schemas = settings.excluded_schema_list
            .iter()
            .map(|s| format!("'{s}'"))
            .collect::<Vec<String>>()
            .join(",");

        settings
    }
}