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