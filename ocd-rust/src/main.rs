use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;

#[derive(Debug, Deserialize)]
struct CsvRow {
    #[serde(rename(deserialize = "ID"))]
    outcrop_id: u64,
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

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let records = read_csv_file("dates.csv");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect("postgres://postgres:postgres@localhost:5432/safari_api")
        .await?;

    let cur_logs = sqlx::query_as::<_, ExistingLog>(
        r#"
        select outcrop_id from audit_logs
        where entity = 'outcrop'
        and action = 'created'
        and outcrop_id is not null"#,
    )
    .fetch_all(&pool)
    .await?;

    for log in cur_logs {
        println!("{:?}", log);
    }

    // for line in records {
    //     println!("{:?} - {:?}", line.outcrop_id, line.inserted_at)
    // }

    Ok(())
}
