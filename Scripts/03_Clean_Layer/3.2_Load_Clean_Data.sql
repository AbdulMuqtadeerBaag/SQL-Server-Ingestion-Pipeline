/*
==================================================================================
                   Stored Procedure: Load Clean Data (Raw -> Clean)
==================================================================================

Project     : SQL Server Ingestion Pipeline
Script      : 3.2_Load_Clean_Data.sql
Author      : Abdul Muqtadeer Baag

Purpose:
	- Perform ETL process to load data into the 'clean' schema.
	- Actions Performed:
		 - Truncates 'clean' tables before loading.
		 - Inserts cleaned and transformed data from 'raw' into 'clean'.
		 - Applies standardization and formatting rules.

Parameters:
	None.
	  This stored procedure does not accept any parameters or return any values.

===================================================================================
*/
-- Use database:
	USE IngestionDB;
	GO
	-- Call Stored Procedure:
	EXEC clean.load_clean_data;
	GO

-- Create the Procedure:
CREATE OR ALTER PROC clean.load_clean_data
AS
BEGIN
	DECLARE @start_time		  DATETIME,
			@end_time  		  DATETIME,
			@batch_start_time DATETIME,
			@batch_end_time	  DATETIME;

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=====================================';
		PRINT '          LOADING CLEAN LAYER';
		PRINT '=====================================';

		PRINT '-------------------------------------';
		PRINT '         Loading CRM Tables';
		PRINT '-------------------------------------';

--1. Loading clean.crm_cust_info:
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: clean.crm_cust_info';
		TRUNCATE TABLE clean.crm_cust_info;
		PRINT '>> Inserting Data Into: clean.crm_cust_info';
		INSERT INTO clean.crm_cust_info (
			Cust_Id, 
			Cust_Key, 
			Cust_Firstname, 
			Cust_Lastname, 
			Cust_Marital_Status, 
			Cust_Gndr, 
			Cust_Create_Date
		)
		SELECT	
			Cust_Id, 
			Cust_Key, 
			TRIM(Cust_Firstname) AS Cust_Firstname,
			TRIM(Cust_Lastname) AS Cust_Lastname, 
			CASE
				WHEN Cust_Marital_Status = 'S' THEN 'Single'
				WHEN Cust_Marital_Status = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS Cust_Marital_Status,		-- Normalize Marital Status values to readable format. 
			CASE
				WHEN Cust_Gndr = 'F' THEN 'Female'
				WHEN Cust_Gndr = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS Cust_Gndr,	-- Normalize Gender values to readable format. 
			Cust_Create_Date
		FROM (
			SELECT	*,
					ROW_NUMBER()
							OVER (PARTITION BY Cust_Id ORDER BY Cust_Create_Date DESC) AS flag_last
			FROM raw.crm_cust_info
			WHERE Cust_Id IS NOT NULL
			 ) t
		WHERE flag_last = 1;  -- Select the most recent record per customer.
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' Second';
		PRINT '---------------------------';

--2. Loading clean.crm_prod_info:
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: clean.crm_prod_info';
		TRUNCATE TABLE clean.crm_prod_info;
		PRINT '>> Inserting Data Into: clean.crm_prod_info';
		INSERT INTO clean.crm_prod_info (
			Prod_Id,
			Catg_Id,
			Prod_Key, 
			Prod_Name, 
			Prod_Cost, 
			Prod_Line, 
			Prod_Start_Date, 
			Prod_End_Date
		)
		SELECT	
			Prod_Id,
			REPLACE(SUBSTRING(Prod_Key,1,5),'-','_') AS Catg_Id,	-- Extract Category ID.
			SUBSTRING(Prod_Key,7,LEN(Prod_Key)) AS Prod_Key,		-- Extract Product Key.
			Prod_Name, 
			ISNULL(Prod_Cost,0) AS Prod_Cost, 
			CASE
				WHEN UPPER(TRIM(Prod_Line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(Prod_Line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(Prod_Line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(Prod_Line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS Prod_Line,	-- Map product line codesto descriptive values.
			CAST(Prod_Start_Date AS DATE) AS Prod_Start_Date,
			CAST(
				LEAD(Prod_Start_Date) OVER (PARTITION BY Prod_Key ORDER BY Prod_Start_Date ASC)-1 AS DATE)
			AS Prod_End_Date	-- Calculate End date as one day before the next start date.
		FROM raw.crm_prod_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' Second';
		PRINT '---------------------------';

--3. Loading clean.crm_sales_details:
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: clean.crm_sales_details';
		TRUNCATE TABLE clean.crm_sales_details;
		PRINT '>> Inserting Data Into: clean.crm_sales_details';
		INSERT INTO clean.crm_sales_details (
			Sls_Ord_Num,
			Sls_Prod_Key, 
			Sls_Cust_Id, 
			Sls_Order_Date, 
			Sls_Ship_Date, 
			Sls_Due_Date, 
			Sls_Sales, 
			Sls_Quantity, 
			Sls_Price
		)
		SELECT
			Sls_Ord_Num,
			Sls_Prod_Key, 
			Sls_Cust_Id,
			CASE
				WHEN Sls_Order_Date = 0 OR LEN(Sls_Order_Date) != 8 THEN NULL
				ELSE CAST(CAST(Sls_Order_Date AS VARCHAR) AS DATE)
			END AS Sls_Order_Date,
			CASE
				WHEN Sls_Ship_Date = 0 OR LEN(Sls_Ship_Date) != 8 THEN NULL
				ELSE CAST(CAST(Sls_Ship_Date AS VARCHAR) AS DATE)
			END AS Sls_Ship_Date, 
			CASE
				WHEN Sls_Due_Date = 0 OR LEN(Sls_Due_Date) != 8 THEN NULL
				ELSE CAST(CAST(Sls_Due_Date AS VARCHAR) AS DATE)
			END AS Sls_Due_Date, 
			CASE
				WHEN Sls_Sales IS NULL OR Sls_Sales <= 0 OR Sls_Sales != Sls_Quantity * ABS(Sls_Price)
					THEN Sls_Quantity * ABS(Sls_Price)
				ELSE Sls_Sales
			END AS Sls_Sales,	-- Recalculate sales if original value is missing or incorrect.
			Sls_Quantity, 
			CASE
				WHEN Sls_Price IS NULL OR Sls_Price <= 0 THEN Sls_Sales / NULLIF(Sls_Quantity,0)
				ELSE Sls_Price
			END AS Sls_Price	-- Derive price if original value is invaild.
		FROM raw.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' Second';
		PRINT '---------------------------';

		PRINT '-------------------------------------';
		PRINT '         Loading ERP Tables';
		PRINT '-------------------------------------';

--4. Loading clean.erp_loc_mst:
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: clean.erp_loc_mst';
		TRUNCATE TABLE clean.erp_loc_mst;
		PRINT '>> Inserting Data Into: clean.erp_loc_mst';
		INSERT INTO clean.erp_loc_mst (
			C_Id,
			Country
		)
		SELECT
			REPLACE(C_Id,'-',''),
			CASE
				WHEN TRIM(Country) IN ('US','USA') THEN 'United States'
				WHEN TRIM(Country) = 'DE' THEN 'Canada'
				WHEN TRIM(Country) = ''OR Country = NULL THEN 'n/a'
				ELSE Country
			END AS Country	-- Normalize and Handle missing or blank country codes.
		FROM raw.erp_loc_mst;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' Second';
		PRINT '---------------------------';

--5. Loading clean.erp_cust_mst:
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: clean.erp_cust_mst';
		TRUNCATE TABLE clean.erp_cust_mst;
		PRINT '>> Inserting Data Into: clean.erp_cust_mst';
		INSERT INTO clean.erp_cust_mst (
			C_Id, 
			DoB, 
			Gender
		)
		SELECT
			CASE
				WHEN C_Id LIKE 'Nas%' THEN SUBSTRING(C_Id,3,LEN(C_Id)) -- Remove 'NAS' prefix if present.
				ELSE C_Id
			END AS C_Id,
			CASE
				WHEN DoB > GETDATE() THEN NULL
				ELSE DoB
			END AS DoB,	-- Set future birthdates to NULL.
			CASE
				WHEN UPPER(TRIM(Gender)) IN ('F','Female') THEN 'Female'
				WHEN UPPER(TRIM(Gender)) IN ('M','Male') THEN 'Male'
				ELSE 'n/a'
			END AS Gender	-- Normalize gender values and handle unknown cases.
		FROM raw.erp_cust_mst;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' Second';
		PRINT '---------------------------';

--6. Loading clean.erp_prod_catg:
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: clean.erp_prod_catg';
		TRUNCATE TABLE clean.erp_prod_catg;
		PRINT '>> Inserting Data Into: clean.erp_prod_catg';
		INSERT INTO clean.erp_prod_catg (
			Id, 
			Catg,
			Sub_Catg, 
			Maintenance
		)
		SELECT
			Id, 
			Catg,
			Sub_Catg, 
			Maintenance
		FROM raw.erp_prod_catg;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' Second';
		PRINT '---------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '=====================================';
		PRINT '     LOADING CLEAN LAYER IS COMPLELED!';
		PRINT '>> Total Load Duration: '+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS VARCHAR)+' Second';
		PRINT '=====================================';
	END TRY

	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING CLEAN LAYER:'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message Number: ' + CAST (ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Message State: ' + CAST (ERROR_STATE() AS VARCHAR);
		PRINT '=========================================='
	END CATCH
END;
GO
