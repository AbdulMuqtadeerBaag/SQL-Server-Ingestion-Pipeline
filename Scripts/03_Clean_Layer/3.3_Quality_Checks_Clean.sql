/*
==================================================================================
                  DQL Script: Clean Layer Quality Checks
==================================================================================

Project     : SQL Server Ingestion Pipeline
Script      : 3.3_Quality_Checks_Clean.sql
Author      : Abdul Muqtadeer Baag

Purpose:
	- Perform data quality checks on the 'clean' layer to ensure accurancy, consistency, and correctness.
	- This script validates:
		 - Null or Duplicate primary keys.
		 - Unwanted Spaces in string Columns.
		 - Invalid data values.
		 - Data types or format inconsistencies.
		 - Logical consistency between related fields.

Usage Notes:
	- Run these checks after loading the 'clean' layer.
	- Investigate and fix any issues reported by the queries.

===================================================================================
*/

-- Change the data context:
	USE IngestionDB;
	GO

-- ================================================================================
--					1. Checking 'clean.crm_cust_info'
-- ================================================================================

-- Check for NULLs or Duplicates in Primary Key:
-- Expectation: No results:
SELECT
	Cust_Id,
	COUNT(*) AS Duplicate_Count
FROM clean.crm_cust_info
GROUP BY Cust_Id
HAVING COUNT(*)>1 OR Cust_Id IS NULL;
GO

-- Check for Unwanted Spaces:
-- Expectation: No results:
SELECT
	Cust_Key
FROM clean.crm_cust_info
WHERE Cust_Key != TRIM(Cust_Key);
GO

-- Data Standradization and Consistency:
SELECT
	DISTINCT Cust_Marital_Status
FROM clean.crm_cust_info;
GO

-- ================================================================================
--					2. Checking 'clean.crm_prod_info'
-- ================================================================================

-- Check for NULLs or Duplicates in Primary Key:
-- Expectation: No results:
SELECT
	Prod_Id,
	COUNT(*) AS Duplicate_Count
FROM clean.crm_prod_info
GROUP BY Prod_Id
HAVING COUNT(*)>1 OR Prod_Id IS NULL;
GO

-- Check for Unwanted Spaces:
-- Expectation: No results:
SELECT
	Prod_Name
FROM clean.crm_prod_info
WHERE Prod_Name != TRIM(Prod_Name);
GO

-- Check for NULLs or Negative Values in Cost:
SELECT
	Prod_Cost
FROM clean.crm_prod_info
WHERE Prod_Cost < 0 OR Prod_Cost IS NULL;
GO

-- Data Standradization and Consistency:
SELECT
	DISTINCT Prod_Line
FROM clean.crm_prod_info;
GO

-- Check for Invalid Date Orders (Start Date > End Date):
-- Expectation: No results:
SELECT
	Prod_Id,
	Catg_Id,
	Prod_Key,
	Prod_Name,
	Prod_Cost,
	Prod_Line,
	Prod_Start_Date,
	Prod_End_Date,
	dwh_Create_Date	
FROM clean.crm_prod_info
WHERE Prod_Start_Date > Prod_End_Date;
GO

-- ================================================================================
--					3. Checking 'clean.crm_sales_details'
-- ================================================================================

-- Check for Invalid Date:
-- Expectation: No Invalid Date:
SELECT
	NULLIF(Sls_Due_Date,0) AS Sls_Due_Date
FROM raw.crm_sales_details
WHERE Sls_Due_Date <= 0
	OR LEN(Sls_Due_Date) != 8
	OR Sls_Due_Date > 20500101
	OR Sls_Due_Date < 19000101;
GO

-- Check for Invalid Date Order (Order Date > Shipping/Due Dates):
-- Expectation: No Results:
SELECT
	Sls_Ord_Num,
	Sls_Prod_Key,
	Sls_Cust_Id,
	Sls_Order_Date,
	Sls_Ship_Date,
	Sls_Due_Date,
	Sls_Sales,
	Sls_Quantity,
	Sls_Price,
	dwh_Create_Date	
FROM clean.crm_sales_details
WHERE Sls_Order_Date > Sls_Ship_Date
	OR Sls_Order_Date > Sls_Due_Date;
GO

-- Check Data Consistency: Sales = Quantity * Price:
-- Expectation: No Results:
SELECT
	Sls_Sales,
	Sls_Quantity,
	Sls_Price
FROM clean.crm_sales_details
WHERE Sls_Sales != Sls_Quantity * Sls_Price
	OR Sls_Sales IS NULL
	OR Sls_Quantity IS NULL
	OR Sls_Price IS NULL
	OR Sls_Sales <= 0
	OR Sls_Quantity <= 0
	OR Sls_Price <= 0
ORDER BY Sls_Sales, Sls_Quantity, Sls_Price;
GO

-- ================================================================================
--					4. Checking 'clean.erp_loc_mst'
-- ================================================================================

-- Data Standardization & Consistency
SELECT DISTINCT 
    Country 
FROM clean.erp_loc_mst
ORDER BY Country;

-- ================================================================================
--					5. Checking 'clean.erp_cust_mst'
-- ================================================================================

-- Identify Out-of-Range Dates:
-- Expectation: Birth dates between 1924-01-01 and Today:
SELECT 
	DISTINCT DoB
FROM clean.erp_cust_mst
WHERE DoB < '1924-01-01' 
   OR DoB > GETDATE();
GO

-- Data Standardization and Consistency:
SELECT 
	DISTINCT Gender 
FROM clean.erp_cust_mst;
GO

-- ================================================================================
--					6. Checking 'clean.erp_prod_catg'
-- ================================================================================

-- Check for Unwanted Spaces:
-- Expectation: No Results:
SELECT 
	Id,
	Catg,
	Sub_Catg,
	Maintenance,
	dwh_Create_Date	
FROM clean.erp_prod_catg
WHERE Catg != TRIM(Catg) 
   OR Sub_Catg != TRIM(Sub_Catg) 
   OR Maintenance != TRIM(Maintenance);

-- Data Standardization & Consistency
SELECT
	DISTINCT Maintenance 
FROM clean.erp_prod_catg;
GO
