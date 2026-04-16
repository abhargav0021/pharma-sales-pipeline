import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum, avg, round, count

# ── Init ──────────────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ── Read Silver Parquet ───────────────────────────────
df_silver = spark.read.format("parquet") \
    .load("s3://pharma-sales-pipeline/silver/")

print("Silver schema:")
df_silver.printSchema()

drug_cols = ["M01AB", "M01AE", "N02BA", "N02BE", "N05B", "N05C", "R03", "R06"]

# ── Gold Table 1: Monthly sales per drug ──────────────
from pyspark.sql.functions import lit
from functools import reduce
from pyspark.sql import DataFrame

# Unpivot (melt) drug columns into rows for easier aggregation
dfs = []
for drug in drug_cols:
    dfs.append(
        df_silver.select(
            col("sale_date"),
            col("Year"),
            col("Month"),
            col("`Weekday Name`").alias("weekday_name"),
            col("Hour"),
            lit(drug).alias("drug_name"),
            col(drug).alias("quantity")
        )
    )

df_long = reduce(DataFrame.unionAll, dfs)

# Monthly sales by drug
df_monthly = df_long \
    .groupBy("Year", "Month", "drug_name") \
    .agg(
        round(sum("quantity"), 2).alias("total_units_sold"),
        round(avg("quantity"), 2).alias("avg_daily_units"),
        count("quantity").alias("trading_days")
    ) \
    .orderBy("Year", "Month", "drug_name")

df_monthly.show(5)
df_monthly.write.format("parquet") \
    .mode("overwrite") \
    .save("s3://pharma-sales-pipeline/gold/monthly_drug_sales/")

print("✅ Gold Table 1 written: monthly_drug_sales")

# ── Gold Table 2: Top drugs overall ───────────────────
df_top_drugs = df_long \
    .groupBy("drug_name") \
    .agg(
        round(sum("quantity"), 2).alias("total_units_sold"),
        round(avg("quantity"), 2).alias("avg_daily_units")
    ) \
    .orderBy("total_units_sold", ascending=False)

df_top_drugs.show()
df_top_drugs.write.format("parquet") \
    .mode("overwrite") \
    .save("s3://pharma-sales-pipeline/gold/top_drugs/")

print("✅ Gold Table 2 written: top_drugs")

# ── Gold Table 3: Sales by weekday ────────────────────
df_weekday = df_long \
    .groupBy("weekday_name", "drug_name") \
    .agg(
        round(avg("quantity"), 2).alias("avg_units_sold")
    ) \
    .orderBy("drug_name", "avg_units_sold", ascending=False)

df_weekday.show()
df_weekday.write.format("parquet") \
    .mode("overwrite") \
    .save("s3://pharma-sales-pipeline/gold/weekday_sales/")

print("✅ Gold Table 3 written: weekday_sales")

print("✅ All Gold tables written successfully.")
job.commit()