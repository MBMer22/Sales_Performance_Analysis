-- Set the Snowflake warehouse for processing queries
USE WAREHOUSE COMPUTE_WH;

-- Create a new database for the sales performance analysis
CREATE OR REPLACE DATABASE SalesPerformanceDB;
USE DATABASE SalesPerformanceDB;

-- Create a schema to organize objects related to the Sales Dashboard
CREATE OR REPLACE SCHEMA SalesDashboardSchema;
USE SCHEMA SalesDashboardSchema;

-- Create a stage for file uploads and specify the CSV format
CREATE OR REPLACE STAGE saledb_stage
FILE_FORMAT = (TYPE = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- List files in the stage to ensure successful upload
LIST @saledb_stage;

-- Create staging tables to temporarily store raw data before processing
CREATE OR REPLACE TABLE Staging_Products (
    Product_ID INT,
    Product_Name STRING,
    Category STRING,
    Sub_Category STRING,
    Price FLOAT
);

CREATE OR REPLACE TABLE Staging_Customers (
    Customer_ID INT,
    First_Name STRING,
    Last_Name STRING,
    Email STRING,
    Phone STRING,
    City STRING,
    State STRING,
    Country STRING
);

CREATE OR REPLACE TABLE Staging_Sales (
    Sale_ID INT,
    Customer_ID INT,
    Product_ID INT,
    Sale_Date DATE,
    Quantity_Sold INT,
    Unit_Price FLOAT,
    Total_Sale FLOAT
);

-- Create dimensional and fact tables for the star schema
CREATE OR REPLACE TABLE Fact_Sales (
    Sale_ID INT,
    Customer_ID INT,
    Product_ID INT,
    Sale_Date DATE,
    Quantity_Sold INT,
    Unit_Price FLOAT,
    Total_Sale FLOAT
);

CREATE OR REPLACE TABLE Dim_Products (
    Product_ID INT,
    Product_Name STRING,
    Category STRING,
    Sub_Category STRING,
    Price FLOAT
);

CREATE OR REPLACE TABLE Dim_Customers (
    Customer_ID INT,
    First_Name STRING,
    Last_Name STRING,
    Email STRING,
    Phone STRING,
    City STRING,
    State STRING,
    Country STRING
);

-- Load data into staging tables from the stage
COPY INTO STAGING_SALES
FROM @saledb_stage/sales.csv
FILE_FORMAT = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO STAGING_PRODUCTS
FROM @saledb_stage/products.csv
FILE_FORMAT = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO STAGING_CUSTOMERS
FROM @saledb_stage/customers.csv
FILE_FORMAT = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Inspect the structure of the staging tables
DESCRIBE TABLE STAGING_SALES;
DESCRIBE TABLE STAGING_PRODUCTS;
DESCRIBE TABLE STAGING_CUSTOMERS;

-- Perform quality checks to ensure data integrity
SELECT COUNT(*) AS row_count FROM staging_customers;
SELECT COUNT(*) AS row_count FROM staging_products;
SELECT COUNT(*) AS row_count FROM staging_sales;

-- Check for missing or null values in staging tables
SELECT * 
FROM staging_customers 
WHERE CUSTOMER_ID IS NULL OR FIRST_NAME IS NULL OR LAST_NAME IS NULL;

SELECT * 
FROM staging_products 
WHERE PRODUCT_ID IS NULL OR PRODUCT_NAME IS NULL;

SELECT * 
FROM staging_sales 
WHERE SALE_ID is NULL OR PRODUCT_ID IS NULL OR CUSTOMER_ID IS NULL;

-- Identify duplicate entries in staging tables
SELECT PRODUCT_ID, COUNT(*)
FROM staging_products
GROUP BY PRODUCT_ID
HAVING COUNT(*) > 1;

SELECT CUSTOMER_ID, COUNT(*)
FROM STAGING_CUSTOMERS
GROUP BY CUSTOMER_ID
HAVING COUNT(*) > 1;

SELECT SALE_ID, COUNT(*)
FROM STAGING_SALES
GROUP BY SALE_ID
HAVING COUNT(*) > 1;

-- Data cleaning: Replace null values and remove invalid rows
CREATE OR REPLACE TABLE Staging_Products_Cleaned AS
SELECT
    Product_ID,
    Product_Name,
    COALESCE(Category, 'Unknown') AS Category,
    COALESCE(SUB_CATEGORY, 'Miscellaneous') AS SUB_CATEGORY,
    Price
FROM STAGING_PRODUCTS;

CREATE OR REPLACE TABLE Staging_Sales_Cleaned AS
SELECT *
FROM STAGING_SALES
WHERE CUSTOMER_ID IS NOT NULL AND PRODUCT_ID IS NOT NULL;

-- Load cleaned data into dimensional and fact tables
INSERT INTO DIM_PRODUCTS (Product_ID, Product_Name, Category, Sub_Category, Price)
SELECT Product_ID, Product_Name, Category, Sub_Category, Price
FROM STAGING_PRODUCTS_CLEANED;

INSERT INTO DIM_CUSTOMERS (Customer_ID, Customer_Name, Country)
SELECT
    Customer_ID,
    CONCAT(First_Name, ' ', Last_Name) AS Customer_Name,
    Country
FROM STAGING_CUSTOMERS;

INSERT INTO FACT_SALES (Customer_ID, Product_ID, Quantity_Sold, Sale_Date, Sale_ID, Total_Sale, Unit_Price)
SELECT
    Customer_ID,
    Product_ID,
    Quantity_Sold,
    Sale_Date,
    Sale_ID,
    Total_Sale,
    Unit_Price
FROM STAGING_SALES_CLEANED;

CREATE OR REPLACE TABLE DIM_CUSTOMERS (
    Customer_ID INT PRIMARY KEY,
    Customer_Name STRING,
    Country STRING
);

-- Verify dimensional tables
SELECT * FROM DIM_CUSTOMERS;
SELECT * FROM DIM_PRODUCTS;

-- Perform analysis queries
-- 1. Top 10 products by revenue
SELECT
    dp.Product_Name,
    SUM(fs.TOTAL_SALE) AS Total_Revenue
FROM FACT_SALES fs
JOIN DIM_PRODUCTS dp ON fs.product_id = dp.product_id
GROUP BY dp.product_name
ORDER BY Total_Revenue DESC
LIMIT 10;

-- 2. Monthly sales trends
SELECT
    DATE_TRUNC('month', fs.Sale_Date) AS Sales_Month,
    SUM(fs.Total_Sale) AS Total_Revenue
FROM FACT_SALES fs
GROUP BY Sales_Month
ORDER BY Sales_Month;

-- 3. Top 5 customers by total purchases
SELECT
    dc.Customer_Name,
    SUM(fs.Quantity_Sold) AS Total_Products_Purchased,
    SUM(fs.Total_Sale) AS Total_Revenue
FROM FACT_SALES fs
JOIN DIM_CUSTOMERS dc ON fs.customer_id = dc.customer_id
GROUP BY dc.customer_name
ORDER BY Total_Products_Purchased DESC
LIMIT 5;

-- Create views for dashboard reporting
CREATE OR REPLACE VIEW vw_top_products_sales AS
SELECT
    dp.Product_Name,
    SUM(fs.TOTAL_SALE) AS Total_Revenue
FROM FACT_SALES fs
JOIN DIM_PRODUCTS dp ON fs.product_id = dp.product_id
GROUP BY dp.product_name
ORDER BY Total_Revenue DESC
LIMIT 10;

CREATE OR REPLACE VIEW vw_sales_trends AS
SELECT
    DATE_TRUNC('month', fs.Sale_Date) AS Sales_Month,
    SUM(fs.Total_Sale) AS Total_Revenue
FROM FACT_SALES fs
GROUP BY Sales_Month
ORDER BY Sales_Month;

CREATE OR REPLACE VIEW vw_top_customers AS
SELECT
    dc.Customer_Name,
    SUM(fs.Quantity_Sold) AS Total_Products_Purchased,
    SUM(fs.Total_Sale) AS Total_Revenue
FROM FACT_SALES fs
JOIN DIM_CUSTOMERS dc ON fs.customer_id = dc.customer_id
GROUP BY dc.customer_name
ORDER BY Total_Products_Purchased DESC
LIMIT 5;