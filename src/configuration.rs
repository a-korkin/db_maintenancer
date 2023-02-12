use config::{Config, FileFormat, File};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    pub database: String,
    pub excluded_schemas: Vec<String>,
    pub log_dir: String,
}

pub fn get_config() -> Settings {
    Config::builder()
        .add_source(File::new("config.yml", FileFormat::Yaml))
        .build()
        .expect("Failed to read config")
        .try_deserialize::<Settings>()
        .expect("Failed to deserialize settings")
}