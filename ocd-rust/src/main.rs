use chrono::{DateTime, Utc};
use serde::Deserialize;

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

fn main() {
    let records = read_csv_file("dates.csv");

    for line in records {
        println!("{:?} - {:?}", line.outcrop_id, line.inserted_at)
    }
}
