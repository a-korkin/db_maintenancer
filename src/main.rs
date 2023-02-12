use config::{Config, File, FileFormat};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool};
use tracing::error;
use tracing::instrument;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    
    let settings = Config::builder()
        .add_source(File::new("config.yml", FileFormat::Yaml))
        .build()
        .expect("Failed to read config")
        .try_deserialize::<Settings>()
        .expect("Failed to deserialize config");

    let excluded_schemas = settings.excluded_schemas
        .iter()
        .map(|s| format!("'{s}'"))
        .collect::<Vec<String>>()
        .join(",");

    let pool = PgPoolOptions::new()
        .connect(&settings.database)
        .await
        .expect("Failed to connect to Postgres");

    let tables = get_tables(&pool, &excluded_schemas)
        .await
        .expect("Failed to get list of tables");
    vacuum_tables(&pool, &tables).await.expect("Failed to vacuum");

    let indexes = get_indexes(&pool, &excluded_schemas).await.expect("Failed to get indexes");
    reindex_indexes(&pool, &indexes).await.expect("Failed to reindex");

    let matviews = get_matviews(&pool).await.expect("Failed to get matviews");
    refresh_matviews(&pool, &matviews).await.expect("Failed to refresh matviews");
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    pub database: String,
    pub excluded_schemas: Vec<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct Table(String);

#[derive(Debug, sqlx::FromRow)]
pub struct Index(String);

#[derive(Debug, sqlx::FromRow)]
pub struct MatView(String);

#[instrument(skip(pool, excluded_schemas))]
async fn get_tables(pool: &PgPool, excluded_schemas: &str) -> Result<Vec<Table>, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT ('"' || table_schema || '"."' || table_name || '"')
        FROM information_schema.tables
        WHERE table_schema NOT IN ({})        
            AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name;
        "#, excluded_schemas);

    let tables = sqlx::query_as::<_, Table>(&sql)
        .fetch_all(pool)
        .await;

    tables
}

async fn vacuum_tables(pool: &PgPool, tables: &Vec<Table>) -> Result<(), sqlx::Error> {
    for table in tables {
        let _ = sqlx::query(&format!("VACUUM ANALYZE {};", table.0))
            .execute(pool)
            .await
            .map_err(|e| error!("{e}"));
    }

    Ok(())
}

#[instrument(skip(pool, excluded_schemas))]
async fn get_indexes(pool: &PgPool, excluded_schemas: &str) -> Result<Vec<Index>, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT '"' || schemaname || '"."' || indexname || '"'
        FROM pg_indexes
        WHERE schemaname NOT IN ({})
        ORDER BY schemaname, indexname;
        "#, excluded_schemas);

    sqlx::query_as::<_, Index>(&sql)
        .fetch_all(pool)
        .await
}

async fn reindex_indexes(pool: &PgPool, indexes: &Vec<Index>) -> Result<(), sqlx::Error> {
    for idx in indexes {
        let _ = sqlx::query(&format!("REINDEX INDEX {};", idx.0))
            .execute(pool)
            .await
            .map_err(|e| error!("{e}"));
    }

    Ok(())
}

#[instrument(skip(pool))]
async fn get_matviews(pool: &PgPool) -> Result<Vec<MatView>, sqlx::Error> {
    sqlx::query_as::<_, MatView>(
        r#"
        SELECT '"' || schemaname || '"."' || matviewname || '"'
        FROM pg_matviews
        ORDER BY schemaname, matviewname;
        "#)
        .fetch_all(pool)
        .await
}

async fn refresh_matviews(pool: &PgPool, matviews: &Vec<MatView>) -> Result<(), sqlx::Error> {
    for matview in matviews {
        let _ = sqlx::query(&format!("REFRESH MATERIALIZED VIEW {};", matview.0))
            .execute(pool)
            .await
            .map_err(|e| error!("{e}"));
    }
    
    Ok(())
}