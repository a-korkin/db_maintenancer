use sqlx::PgPool;
use tracing::error;

#[derive(Debug, sqlx::FromRow)]
struct Table(String);

pub async fn vacuum_tables(pool: &PgPool, excluded_schemas: &str) -> Result<(), sqlx::Error> {
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
        .await
        .expect("Failed to get tables");

    for table in tables {
        let _ = sqlx::query(&format!("VACUUM ANALYZE {};", table.0))
            .execute(pool)
            .await
            .map_err(|e| error!("{e}"));
    }

    Ok(())
}

#[derive(Debug, sqlx::FromRow)]
struct Index(String);

pub async fn reindex_indexes(pool: &PgPool, excluded_schemas: &str) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        SELECT '"' || schemaname || '"."' || indexname || '"'
        FROM pg_indexes
        WHERE schemaname NOT IN ({})
        ORDER BY schemaname, indexname;
        "#, excluded_schemas);

    let indexes = sqlx::query_as::<_, Index>(&sql)
        .fetch_all(pool)
        .await
        .expect("Failed to get indexes");
        
    for idx in indexes {
        let _ = sqlx::query(&format!("REINDEX INDEX {};", idx.0))
            .execute(pool)
            .await
            .map_err(|e| error!("{e}"));
    }

    Ok(())
}

#[derive(Debug, sqlx::FromRow)]
struct MatView(String);

pub async fn refresh_matviews(pool: &PgPool) -> Result<(), sqlx::Error> {
    let matviews = sqlx::query_as::<_, MatView>(
        r#"
        SELECT '"' || schemaname || '"."' || matviewname || '"'
        FROM pg_matviews
        ORDER BY schemaname, matviewname;
        "#)
        .fetch_all(pool)
        .await
        .expect("Failed to get matviews");

    for matview in matviews {
        let _ = sqlx::query(&format!("REFRESH MATERIALIZED VIEW {};", matview.0))
            .execute(pool)
            .await
            .map_err(|e| error!("{e}"));
    }
    
    Ok(())
}