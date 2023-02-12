mod configuration;
mod db;

use configuration::get_config;
use db::{vacuum_tables, reindex_indexes, refresh_matviews};
use sqlx::{postgres::PgPoolOptions};
use tracing_subscriber;

#[tokio::main]
async fn main() {
    let settings = get_config();
    let file_appender = tracing_appender::rolling::daily(settings.log_dir, "info");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .init();

    let excluded_schemas = settings.excluded_schemas
        .iter()
        .map(|s| format!("'{s}'"))
        .collect::<Vec<String>>()
        .join(",");

    let pool = PgPoolOptions::new()
        .connect(&settings.database)
        .await
        .expect("Failed to connect to Postgres");

    vacuum_tables(&pool, &excluded_schemas).await.expect("Failed to VACUUM tables");
    reindex_indexes(&pool, &excluded_schemas).await.expect("Failed to REINDEX indexes");
    refresh_matviews(&pool).await.expect("Failed to REFRESH matviews");
}
