use std::io::Cursor;

use arrow::ipc::writer::StreamWriter;
use arrow::util::pretty::pretty_format_batches;
use polars::prelude::{self as pl, IntoLazy, SerReader};
use spark_connect::{col, functions as f, lit, Column, SparkSession};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let remote = std::env::var("SPARK_REMOTE").unwrap_or_else(|_| "sc://localhost:15002".into());
    let spark = SparkSession::builder().remote(&remote).get_or_create()?;
    println!("Spark {} at {remote}", spark.version()?);

    // Runs on Spark: 5 million rows reduced to 7 groups.
    let e = |c: Column| c.expression().clone();
    let summary = spark
        .range(5_000_000)?
        .with_column("bucket", col("id") % lit(7))
        .group_by([col("bucket")])
        .agg(vec![
            e(f::count(col("id")).alias("n")),
            e(f::sum(col("id")).alias("sum_id")),
            e(f::max(col("id")).alias("max_id")),
        ])
        .sort(vec![e(col("bucket"))]);

    // Collect as arrow-rs RecordBatches.
    let batches = summary.collect_record_batches()?;
    println!("{}", pretty_format_batches(&batches)?);

    // Hand the batches to Polars through an Arrow IPC stream.
    let schema = batches.first().ok_or("empty result")?.schema();
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &schema)?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    let df = pl::IpcStreamReader::new(Cursor::new(ipc)).finish()?;

    // Runs locally in Polars: join a small lookup table, derive a column, sort.
    let days = polars::df!(
        "bucket" => [0i64, 1, 2, 3, 4, 5, 6],
        "day" => ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
    )?;
    let out = df
        .lazy()
        .join(
            days.lazy(),
            [pl::col("bucket")],
            [pl::col("bucket")],
            pl::JoinArgs::new(pl::JoinType::Left),
        )
        .with_columns([(pl::col("n").cast(pl::DataType::Float64) * pl::lit(100.0)
            / pl::col("n").sum())
        .alias("pct")])
        .sort(
            ["pct", "day"],
            pl::SortMultipleOptions::default().with_order_descending_multi([true, false]),
        )
        .collect()?;
    println!("{out}");
    Ok(())
}
