// Calls DataFrame::zip, whose `Zip` relation was added to Spark Connect in 4.3.0 (SPARK-57247).
use spark_connect::{col, lit, SparkSession};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark = SparkSession::builder().remote(&std::env::var("SPARK_REMOTE")?).get_or_create()?;
    println!("server {}", spark.version()?);
    let base = spark.range(3)?;
    let left = base.select([col("id")]);
    let right = base.select([(col("id") * lit(10)).alias("x")]);
    match left.zip(&right).and_then(|z| z.collect_record_batches()) {
        Ok(b) => println!("zip ok:\n{}", arrow::util::pretty::pretty_format_batches(&b)?),
        Err(e) => println!("zip error: {e}"),
    }
    Ok(())
}
