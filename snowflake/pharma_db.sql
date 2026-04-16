-- Create database and schema
CREATE DATABASE pharma_db;
CREATE SCHEMA pharma_db.gold;

-- Use them
USE DATABASE pharma_db;
USE SCHEMA gold;

-- Table 1: Monthly drug sales
CREATE TABLE monthly_drug_sales (
    year              INT,
    month             INT,
    drug_name         VARCHAR,
    total_units_sold  FLOAT,
    avg_daily_units   FLOAT,
    trading_days      INT
);

-- Table 2: Top drugs overall
CREATE TABLE top_drugs (
    drug_name         VARCHAR,
    total_units_sold  FLOAT,
    avg_daily_units   FLOAT
);

-- Table 3: Weekday sales trends
CREATE TABLE weekday_sales (
    weekday_name      VARCHAR,
    drug_name         VARCHAR,
    avg_units_sold    FLOAT
);

SHOW TABLES IN SCHEMA pharma_db.gold;

-- Create S3 integration
CREATE STORAGE INTEGRATION s3_pharma_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://pharma-sales-pipeline/gold/')
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::045230654036:role/GluePharmaRole';

    DESC INTEGRATION s3_pharma_integration;

    -- File format for Parquet
CREATE FILE FORMAT parquet_format
    TYPE = PARQUET
    SNAPPY_COMPRESSION = TRUE;

-- Stage for monthly drug sales
CREATE STAGE stage_monthly_drug_sales
    URL = 's3://pharma-sales-pipeline/gold/monthly_drug_sales/'
    STORAGE_INTEGRATION = s3_pharma_integration
    FILE_FORMAT = parquet_format;

-- Stage for top drugs
CREATE STAGE stage_top_drugs
    URL = 's3://pharma-sales-pipeline/gold/top_drugs/'
    STORAGE_INTEGRATION = s3_pharma_integration
    FILE_FORMAT = parquet_format;

-- Stage for weekday sales
CREATE STAGE stage_weekday_sales
    URL = 's3://pharma-sales-pipeline/gold/weekday_sales/'
    STORAGE_INTEGRATION = s3_pharma_integration
    FILE_FORMAT = parquet_format;

LIST @stage_monthly_drug_sales;
LIST @stage_top_drugs;
LIST @stage_weekday_sales;

COPY INTO monthly_drug_sales
FROM (
    SELECT
        $1:Year::INT,
        $1:Month::INT,
        $1:drug_name::VARCHAR,
        $1:total_units_sold::FLOAT,
        $1:avg_daily_units::FLOAT,
        $1:trading_days::INT
    FROM @stage_monthly_drug_sales
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = CONTINUE;

SELECT COUNT(*) FROM monthly_drug_sales;
SELECT * FROM monthly_drug_sales LIMIT 5;

COPY INTO top_drugs
FROM (
    SELECT
        $1:drug_name::VARCHAR,
        $1:total_units_sold::FLOAT,
        $1:avg_daily_units::FLOAT
    FROM @stage_top_drugs
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = CONTINUE;

SELECT COUNT(*) FROM top_drugs;
SELECT * FROM top_drugs LIMIT 5;

COPY INTO weekday_sales
FROM (
    SELECT
        $1:weekday_name::VARCHAR,
        $1:drug_name::VARCHAR,
        $1:avg_units_sold::FLOAT
    FROM @stage_weekday_sales
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = CONTINUE;

SELECT COUNT(*) FROM weekday_sales;
SELECT * FROM weekday_sales LIMIT 5;