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

    vacuum_tables(&pool, &tables).await.expect("Failed to VACUUM");
}

#[derive(Debug, sqlx::FromRow)]
pub struct Table {
    pub name: String,
}

async fn get_tables(pool: &PgPool) -> Result<Vec<Table>, sqlx::Error> {
    let tables = sqlx::query_as::<_, Table>(
        r#"
        SELECT (s.table_schema || '.' || s.table_name) AS name
        FROM information_schema.tables AS s
        WHERE s.table_schema NOT IN ('information_schema', 'pg_catalog', 'fias', 'sys')        
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