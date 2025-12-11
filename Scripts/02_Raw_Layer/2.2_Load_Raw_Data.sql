/*
==================================================================================
                   Stored Procedure: Load Raw Data (Source -> Raw)
==================================================================================

Project     : SQL Server Ingestion Pipeline
Script      : 2.2_Load_Raw_Data.sql
Author      : Abdul Muqtadeer Baag

Purpose:
   Load CSV/source files into the 'raw' schema (store as-is, landing zone).

Notes:
   - This procedure Truncates and Reloads raw tables.
   - Uses the 'BULK INSERT' Loads data from CSV files into raw tables.
   - Do not apply transformations here; keep raw data unchanged.

==================================================================================
*/

-- Run from the project database:
   USE IngestionDB;
   GO
   -- Call Stored Procedure:
   EXEC raw.load_raw_data;
   GO

-- Create the Stored Procedure:
CREATE OR ALTER PROC raw.load_raw_data
AS
BEGIN
	DECLARE @start_time		  DATETIME,
			@end_time  		  DATETIME,
			@batch_start_time DATETIME,
			@batch_end_time	  DATETIME;

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=====================================';
		PRINT '          LOADING RAW LAYER';
		PRINT '=====================================';

		PRINT '-------------------------------------';
		PRINT '         Loading CRM Tables';
		PRINT '-------------------------------------';

--1. Loading raw.crm_cust_info:
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: raw.crm_cust_info';
		TRUNCATE TABLE raw.crm_cust_info;
		PRINT '>> Inserting Data Into: raw.crm_cust_info';
		BULK INSERT raw.crm_cust_info
		FROM 'C:\SQL Server\Project\Datasets\Source_CRM\Cust_Info.csv'
		WITH (
				FIRSTROW = 2,			-- Skip header start from row 2.
				FIELDTERMINATOR = ',',  -- Columns separated by comma.
				ROWTERMINATOR = '\n',   -- Each row ends on new line.
				TABLOCK					-- Locks the table so other users can`t write during load.
			 );
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' Second';
		PRINT '---------------------------';

--2. Loading raw.crm_prod_info:
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: raw.crm_prod_info';
		TRUNCATE TABLE raw.crm_prod_info;
		PRINT '>> Inserting Data Into: raw.crm_prod_info';
		BULK INSERT raw.crm_prod_info
		FROM 'C:\SQL Server\Project\Datasets\Source_CRM\Prod_Info.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
			 );
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' Second';
		PRINT '---------------------------';

--3. Loading raw.crm_sales_details:
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: raw.crm_sales_details';
		TRUNCATE TABLE raw.crm_sales_details;
		PRINT '>> Inserting Data Into: raw.crm_sales_details';
		BULK INSERT raw.crm_sales_details
		FROM 'C:\SQL Server\Project\Datasets\Source_CRM\Sales_Details.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
			 );
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' Second';
		PRINT '---------------------------';

		PRINT '-------------------------------------';
		PRINT '         Loading ERP Tables';
		PRINT '-------------------------------------';

--4. Loading raw.erp_loc_mst:
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: raw.erp_loc_mst';
		TRUNCATE TABLE raw.erp_loc_mst;
		PRINT '>> Inserting Data Into: raw.erp_loc_mst';
		BULK INSERT raw.erp_loc_mst
		FROM 'C:\SQL Server\Project\Datasets\Source_ERP\Loc_Mst.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK	
			 );
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' Second';
		PRINT '---------------------------';

--5. Loading raw.erp_cust_mst:
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: raw.erp_cust_mst';
		TRUNCATE TABLE raw.erp_cust_mst;
		PRINT '>> Inserting Data Into: raw.erp_cust_mst';
		BULK INSERT raw.erp_cust_mst
		FROM 'C:\SQL Server\Project\Datasets\Source_ERP\Cust_Mst.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
			 );
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' Second';
		PRINT '---------------------------';

--6. Loading clean.erp_prod_catg:
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: raw.erp_prod_catg';
		TRUNCATE TABLE raw.erp_prod_catg;
		PRINT '>> Inserting Data Into: raw.erp_prod_catg';
		BULK INSERT raw.erp_prod_catg
		FROM 'C:\SQL Server\Project\Datasets\Source_ERP\Prod_Catg.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
			 );
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' Second';
		PRINT '---------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '=====================================';
		PRINT '     LOADING RAW LAYER IS COMPLELED!';
		PRINT '>> Total Load Duration: '+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS VARCHAR)+' Second';
		PRINT '=====================================';
	END TRY

	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING RAW LAYER:'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message Number: ' + CAST (ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Message State: ' + CAST (ERROR_STATE() AS VARCHAR);
		PRINT '=========================================='
	END CATCH
END;
GO
