
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = PROJECT_ROOT / "data" / "bank.csv"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

def main():
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Dataset not found: {DATA_FILE}")

    spark = (
        SparkSession.builder
        .appName("BankingCapstoneEDA")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # The original Bank Marketing dataset is semicolon-delimited.
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("sep", ",")
            .csv(str(DATA_FILE))
        )

        print("\n=== DATASET SCHEMA ===")
        df.printSchema()

        print("\n=== DATASET SIZE ===")
        print(f"Rows: {df.count()}")
        print(f"Columns: {len(df.columns)}")

        print("\n=== MISSING VALUES ===")
        missing_exprs = [
            F.sum(
                F.when(F.col(c).isNull(), 1).otherwise(0)
            ).alias(c)
            for c in df.columns
        ]
        df.select(*missing_exprs).show(truncate=False)

        print("\n=== SUBSCRIPTION TARGET DISTRIBUTION ===")
        df.groupBy("y").count().orderBy(F.desc("count")).show()

        print("\n=== SUBSCRIPTION RATE BY JOB ===")
        job_summary = (
            df.groupBy("job")
            .agg(
                F.count("*").alias("customers"),
                F.sum(
                    F.when(F.col("y") == "yes", 1).otherwise(0)
                ).alias("subscriptions")
            )
            .withColumn(
                "subscription_rate_pct",
                F.round(
                    F.col("subscriptions") / F.col("customers") * 100,
                    2
                )
            )
            .orderBy(F.desc("subscription_rate_pct"))
        )
        job_summary.show(truncate=False)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        job_summary.coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv(str(OUTPUT_DIR / "job_by_target"))

        print(f"\nJob summary saved under: {OUTPUT_DIR / 'job_by_target'}")
        print("\nSpark EDA completed successfully.")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
