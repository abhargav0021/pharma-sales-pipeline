import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, round
from pyspark.sql.types import DoubleType

# ── Init ──────────────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ── Read Bronze Parquet ───────────────────────────────
df_bronze = spark.read.format("parquet") \
    .load("s3://pharma-sales-pipeline/bronze/")

print("Bronze schema:")
df_bronze.printSchema()
print(f"Bronze row count: {df_bronze.count()}")

# ── Drug columns (all except datum and metadata) ──────
drug_cols = ["M01AB", "M01AE", "N02BA", "N02BE", "N05B", "N05C", "R03", "R06"]

# ── Clean & Transform ─────────────────────────────────
df_silver = df_bronze \
    .withColumn("sale_date", to_date(col("datum"), "M/d/yyyy")) \
    .drop("datum", "ingestion_timestamp", "source_file")

# Cast all drug columns to DoubleType and round to 2 decimals
for drug in drug_cols:
    df_silver = df_silver \
        .withColumn(drug, round(col(drug).cast(DoubleType()), 2))

# ── Data Quality Checks ───────────────────────────────
# Drop rows where sale_date is null
df_silver = df_silver.filter(col("sale_date").isNotNull())

# Drop duplicates
df_silver = df_silver.dropDuplicates()

# Drop rows where all drug columns are null or zero
from pyspark.sql.functions import greatest
df_silver = df_silver.filter(
    greatest(*[col(c) for c in drug_cols]) > 0
)

print("Silver schema:")
df_silver.printSchema()
print(f"Silver row count after cleaning: {df_silver.count()}")
df_silver.show(5)

# ── Write Parquet to Silver layer ─────────────────────
df_silver.write.format("parquet") \
    .mode("overwrite") \
    .save("s3://pharma-sales-pipeline/silver/")

print("✅ Silver layer written successfully.")
job.commit()