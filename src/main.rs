mod configuration;
mod db;

use configuration::Settings;
use db::{vacuum_tables, reindex_indexes, refresh_matviews};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    let settings = Settings::builder();
    let file_appender = tracing_appender::rolling::daily(&settings.log_dir, "info");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .init();

    let pool = PgPoolOptions::new()
        .connect(&settings.database)
        .await
        .expect("Failed to connect to Postgres");

    if settings.vacuum {
        vacuum_tables(&pool, &settings.excluded_schemas)
            .await
            .expect("Failed to VACUUM tables");
    }
    if settings.reindex {
        reindex_indexes(&pool, &settings.excluded_schemas)
            .await
            .expect("Failed to REINDEX indexes");
    }
    if settings.refresh_matviews {
        refresh_matviews(&pool)
            .await
            .expect("Failed to REFRESH matviews");
    }
}
