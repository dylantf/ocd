use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::{postgres::PgPoolOptions, QueryBuilder};

#[derive(Debug, Deserialize)]
struct CsvRow {
    #[serde(rename(deserialize = "ID"))]
    outcrop_id: i64,
    #[serde(rename(deserialize = "Inserted At"), with = "timestamp_fmt")]
    inserted_at: DateTime<Utc>,
}

mod timestamp_fmt {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &str = "%Y-%m-%d %H:%M:%S";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let dt = NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)?;
        Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
    }
}

fn read_csv_file(filepath: &str) -> Vec<CsvRow> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b';')
        .from_path(filepath)
        .unwrap();

    reader
        .deserialize()
        .map(|row| {
            let record: CsvRow = row.unwrap();
            record
        })
        .collect::<Vec<CsvRow>>()
}

#[derive(sqlx::FromRow, Debug)]
struct ExistingLog {
    outcrop_id: Option<i64>,
}

async fn fetch_outcrops_with_logs(pool: &sqlx::PgPool) -> Result<Vec<i64>, sqlx::Error> {
    let cur_logs = sqlx::query_as::<_, ExistingLog>(
        r#"
        select outcrop_id from audit_logs
        where entity = 'outcrop'
        and action = 'created'
        and outcrop_id is not null"#,
    )
    .fetch_all(pool)
    .await?;

    let outcrop_ids = cur_logs
        .iter()
        .filter_map(|record| record.outcrop_id)
        .collect();

    Ok(outcrop_ids)
}

struct RecordToInsert {
    entity: String,
    action: String,
    outcrop_id: Option<i64>,
    user_id: i64,
    inserted_at: DateTime<Utc>,
}

impl From<&CsvRow> for RecordToInsert {
    fn from(value: &CsvRow) -> Self {
        Self {
            entity: String::from("outcrop"),
            action: String::from("created"),
            outcrop_id: Some(value.outcrop_id),
            user_id: 11,
            inserted_at: value.inserted_at,
        }
    }
}

async fn insert_records(
    records: Vec<RecordToInsert>,
    pool: &sqlx::PgPool,
) -> Result<(u64, u64), sqlx::Error> {
    let total_records = records.len() as u64;
    let mut qb = QueryBuilder::new(
        "insert into audit_logs (entity, action, outcrop_id, user_id, inserted_at)",
    );

    qb.push_values(records, |mut b, record| {
        b.push_bind(record.entity)
            .push_bind(record.action)
            .push_bind(record.outcrop_id)
            .push_bind(record.user_id)
            .push_bind(record.inserted_at);
    });

    let query = qb.build();

    let results = query.execute(pool).await?;
    let success = results.rows_affected();
    let failed = total_records - success;

    Ok((success, failed))
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let records = read_csv_file("dates.csv");
    println!("Read {} records available from CSV file.", records.len());

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect("postgres://postgres:postgres@localhost:5432/safari_api")
        .await?;

    let cur_logs = fetch_outcrops_with_logs(&pool).await?;
    println!("{} outcrops currently have audit logs.", cur_logs.len());

    let to_insert: Vec<RecordToInsert> = records
        .iter()
        .filter_map(|row| {
            if !cur_logs.contains(&row.outcrop_id) {
                Some(row.into())
            } else {
                None
            }
        })
        .collect();
    println!("{} new audit logs to be inserted.", to_insert.len());

    if !to_insert.is_empty() {
        let (success, failed) = insert_records(to_insert, &pool).await?;
        println!("Created {} new audit logs, with {} failed", success, failed);
    } else {
        println!("Nothing to do!");
    }

    Ok(())
}
