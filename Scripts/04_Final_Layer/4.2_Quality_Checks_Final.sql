/*
==================================================================================
                  DQL Script: Final Layer Quality Checks
==================================================================================

Project     : SQL Server Ingestion Pipeline
Script      : 4.2_Quality_Checks_Final.sql
Author      : Abdul Muqtadeer Baag

Purpose:
	- Validate the integrity and consistency of the 'final' layer.
	- This scripts checks for:
		  - Unqiueness of surrogate keys in dimension views.
		  - Referential integrity between fact and dimension views.
		  - Missing or invaild relationships in the star-schema.

Usage Notes:
	- Run these checks after creating 'final' dimensions and fact views.
	- Investigate and fix any issues found by these queries.

===================================================================================
*/

-- Change the data context:
	USE IngestionDB;
	GO

-- ================================================================================
--					1. Checking 'final.dim_customers'
-- ================================================================================

-- Check for uniqueness of customer key in final.dim_customers:
-- Expectation: No results:
SELECT
	Customer_Key,
	COUNT(*) AS Duplicate_Count
FROM final.dim_customers
GROUP BY Customer_Key
HAVING COUNT(*)>1;
GO

-- ================================================================================
--					2. Checking 'final.dim_products'
-- ================================================================================

-- Check for uniqueness of product key in final.dim_products:
-- Expectation: No results:
SELECT
	Product_Key,
	COUNT(*) AS Duplicate_Count
FROM final.dim_products
GROUP BY Product_Key
HAVING COUNT(*)>1;
GO

-- ================================================================================
--					3. Checking 'final.fact_sales'
-- ================================================================================

-- Check the date model connectivity between dimensionS and fact:
SELECT
	*
FROM final.fact_sales S
LEFT JOIN final.dim_customers C
	ON C.Customer_Key = S.Customer_Key
LEFT JOIN final.dim_products P
	ON P.Product_Number = S.Product_Key
WHERE P.Product_Number IS NULL OR C.Customer_Key IS NULL;
GO
