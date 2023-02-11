use sqlx::{postgres::PgPoolOptions, types::Uuid};

#[tokio::main]
async fn main() {
    let pool = PgPoolOptions::new()
        .connect("postgres://postgres:password@localhost:5432/test")
        .await
        .expect("Failed to connect to Postgres");

    let persons = sqlx::query_as::<_, Person>(
        "SELECT id, last_name, first_name FROM public.person;"
    )
    .fetch_all(&pool)
    .await
    .expect("Failed to execute query");

    for person in persons {
        println!("{:#?}", person);
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct Person {
    pub id: Uuid,
    pub last_name: String,
    pub first_name: String,
}
