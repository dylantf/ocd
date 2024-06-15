use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

fn main() {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b';')
        .from_path("Outcrop_creationDates_.InsertedAt.csv")
        .unwrap();

    let records = reader
        .records()
        .map(|record| {
            let result = record.unwrap();
            let outcrop_id = result.get(0).unwrap().parse::<u64>();
            let outcrop_id = outcrop_id.unwrap();

            let inserted_at =
                DateTime::parse_from_str(result.get(1).unwrap(), "%Y-%m-%d %H:%M:%S").unwrap();

            (outcrop_id, inserted_at)
        })
        .collect::<Vec<_>>();

    for line in records {
        println!("{:?}", line)
    }
}
