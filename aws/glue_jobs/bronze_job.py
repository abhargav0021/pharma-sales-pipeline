import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, input_file_name

# ── Init ──────────────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ── Read raw CSV from S3 ──────────────────────────────
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3://pharma-sales-pipeline/raw/salesdaily.csv")

# ── Inspect schema (visible in Glue logs) ────────────
df_raw.printSchema()
print(f"Row count: {df_raw.count()}")

# ── Add metadata columns ──────────────────────────────
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", input_file_name())

# ── Write Parquet to Bronze layer ─────────────────────
df_bronze.write.format("parquet") \
    .mode("overwrite") \
    .save("s3://pharma-sales-pipeline/bronze/")

print("✅ Bronze layer written successfully.")
job.commit()