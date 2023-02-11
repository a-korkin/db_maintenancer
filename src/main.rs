use sqlx::{postgres::PgPoolOptions, PgPool};

#[tokio::main]
async fn main() {
    let pool = PgPoolOptions::new()
        .connect("postgres://postgres:password@localhost:5432/test")
        .await
        .expect("Failed to connect to Postgres");

    let tables = get_tables(&pool)
        .await
        .expect("Failed to get list of tables")
        .iter()
        .map(|t| t.name.to_owned())
        .collect::<Vec<String>>();

    vacuum_tables(&pool, &tables).await.expect("Failed to vacuum");

    let indexes = get_indexes(&pool).await.expect("Failed to get indexes");
    reindex_indexes(&pool, &indexes).await.expect("Failed to reindex");
}

#[derive(Debug, sqlx::FromRow)]
pub struct Table {
    pub name: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct Index(String);

async fn get_tables(pool: &PgPool) -> Result<Vec<Table>, sqlx::Error> {
    let tables = sqlx::query_as::<_, Table>(
        r#"
        SELECT ('"' || s.table_schema || '"."' || s.table_name || '"') AS name
        FROM information_schema.tables AS s
        WHERE s.table_schema NOT IN ('information_schema', 'pg_catalog', 'sys')        
            AND s.table_type = 'BASE TABLE'
        ORDER BY s.table_schema, s.table_name;
        "#)
        .fetch_all(pool)
        .await;

    tables
}

async fn vacuum_tables(pool: &PgPool, tables: &Vec<String>) -> Result<(), sqlx::Error> {
    for table in tables {
        println!("VACUUM table {}", table);

        let _ = sqlx::query(&format!("VACUUM ANALYZE {};", table))
            .execute(pool)
            .await;
    }

    Ok(())
}

async fn get_indexes(pool: &PgPool) -> Result<Vec<Index>, sqlx::Error> {
    sqlx::query_as::<_, Index>(
        r#"
        SELECT '"' || schemaname || '."' || indexname || '"'
        FROM pg_indexes
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'sys')
        "#)
        .fetch_all(pool)
        .await
}

async fn reindex_indexes(pool: &PgPool, indexes: &Vec<Index>) -> Result<(), sqlx::Error> {
    for idx in indexes {
        println!("REINDEX INDEX {}", idx.0);

        let _ = sqlx::query(&format!("REINDEX INDEX {}", idx.0))
            .execute(pool)
            .await;
    }

    Ok(())
}