/*
===================================================================
              DDL Scripts: Create Final View
===================================================================

Project: SQL Server Ingestion Pipeline
Scripts: 4.1_Create_Final_Views.sql
Author: Abdul Muqtadeer Baag

Purpose:
	- Create dimension and fact views for the 'final' layer.
	- The 'final' layer providers business-ready, analytics-focused datasets.
	- Built from the refined 'clean' layer.

Usage:
	- These views can be queried directly by BI tools and dashboards.

Notes:
	- Views combine, enrich, and standardize data into star-schema structures.
	- 'Final' layer is optimized for reporting and analytics only.

====================================================================
*/

-- Use the Database:
	USE IngestionDB;
	GO

-- ====================================================================
--				  CREATE DIMENSION: final.dim_customers
-- ====================================================================

-- i. Drop View if it already exists:
IF OBJECT_ID ('final.dim_customers','V') IS NOT NULL
	DROP VIEW final.dim_customers;
GO

-- Create the 1_View 'final.dim_customers':
CREATE VIEW final.dim_customers
AS
	SELECT
		ROW_NUMBER() OVER (ORDER BY ci.Cust_Id) AS Customer_Key, -- Surrogate Key.
		ci.Cust_Id								AS Customer_Id,
		ci.Cust_Key								AS Customer_Number,
		ci.Cust_Firstname						AS First_Name,
		ci.Cust_Lastname						AS Last_Name,
		lm.Country								AS Country,
		ci.Cust_Marital_Status					AS Marital_Status,
		CASE
			WHEN ci.Cust_Gndr != 'n/a' THEN ci.Cust_Gndr	-- CRM is the primary source for gender.
			ELSE COALESCE(cm.Gender,'n/a')					-- Fallback to ERP data.
		END										AS Gender,
		cm.DoB									AS Birth_Date,
		ci.Cust_Create_Date						AS Create_Date
	FROM clean.crm_cust_info ci
	LEFT JOIN clean.erp_cust_mst cm
		ON ci.Cust_Key = cm.C_Id
	LEFT JOIN clean.erp_loc_mst lm 
		ON ci.Cust_Key = lm.C_Id;
GO

-- ====================================================================
--				 CREATE DIMENSION: final.dim_products
-- ====================================================================

-- ii. Drop View if it already exists:
IF OBJECT_ID ('final.dim_products','V') IS NOT NULL
	DROP VIEW final.dim_products;
GO

-- Create the 2_View 'final.dim_products':
CREATE VIEW final.dim_products
AS
	SELECT
		ROW_NUMBER() OVER (ORDER BY pr.Prod_Start_Date, pr.Prod_Key) AS Product_Key,		-- Surrogate key.
		pr.Prod_Id			AS Product_Id,
		pr.Prod_Key			AS Product_Number,
		pr.Prod_Name		AS Product_Name,
		pr.Catg_Id			AS Category_Id,
		pc.Catg				AS Category,
		pc.Sub_Catg			AS Sub_Category,
		pc.Maintenance		AS Maintenance,
		pr.Prod_Cost		AS Cost,
		pr.Prod_Line		AS Product_Line,
		pr.Prod_Start_Date	AS Start_Date
	FROM clean.crm_prod_info pr
	LEFT JOIN clean.erp_prod_catg pc
		ON pr.Catg_Id = pc.Id
	WHERE pr.Prod_End_Date IS NULL;		-- Filter out all historical data.
GO

-- ====================================================================
--				CREATE DIMENSION: final.fact_products
-- ====================================================================

-- iii. Drop View if it already exists:
IF OBJECT_ID ('final.fact_sales','V') IS NOT NULL
	DROP VIEW final.fact_sales;
GO

-- Create the 3_View 'final.fact_sales':
CREATE VIEW final.fact_sales
AS
	SELECT
		sd.Sls_Ord_Num		AS Order_Number,
		dp.Product_Number	AS Product_Key,
		dc.Customer_Key		AS Customer_Key,
		sd.Sls_Order_Date	AS Order_Date,
		sd.Sls_Ship_Date	AS Ship_Date,
		sd.Sls_Due_Date		AS Due_Date,
		sd.Sls_Sales		AS Sales_Amount,
		sd.Sls_Quantity		AS Quantity,
		sd.Sls_Price		AS Price
	FROM clean.crm_sales_details sd
	LEFT JOIN final.dim_products dp
		ON sd.Sls_Prod_Key = dp.Product_Number
	LEFT JOIN final.dim_customers dc
		ON sd.Sls_Cust_Id = dc.Customer_Id;
GO
