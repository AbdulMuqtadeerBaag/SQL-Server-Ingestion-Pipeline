/*
===================================================================
              DDL Scripts: Create Clean Tables
===================================================================

Project: SQL Server Ingestion Pipeline
Scripts: 3.1_Create_Clean_Tables.sql
Author: Abdul Muqtadeer Baag

Purpose:
	- Create tables in the 'clean' Schema.
	- Clean layer stores validated, cleaned, and enriched data derived from the 'raw' layer.

Notes:
	- Use this script to rebuild 'clean' tables when schema changes occur.
	- Apply transformations, type corrections, and lookups in the 'clean' layer.

====================================================================
*/

-- Use the Database:
	USE IngestionDB;
	GO

----------------------------------------------------------------------
-- CRM: Customer Relationship Management.
-- i. Drop Table if it already exists:
IF OBJECT_ID ('clean.crm_cust_info','U') IS NOT NULL
	DROP TABLE clean.crm_cust_info;
GO

-- Create the CRM table_1 'Cust_Info':
	CREATE TABLE clean.crm_cust_info
	(
		Cust_Id					INT,
		Cust_Key				VARCHAR(30),
		Cust_Firstname			VARCHAR(50),
		Cust_Lastname			VARCHAR(50),
		Cust_Marital_Status		VARCHAR(20),
		Cust_Gndr				VARCHAR(15),
		Cust_Create_Date		DATE,
		dwh_Create_Date			DATETIME2 DEFAULT GETDATE()
	);
	GO

-- See the table:
SELECT * FROM clean.crm_cust_info;
GO

----------------------------------------------------------------------
-- ii. Drop Table if it already exists:
IF OBJECT_ID('clean.crm_prod_info', 'U') IS NOT NULL
    DROP TABLE clean.crm_prod_info;
GO

-- Create the CRM table_2 'Prod_Info':
	CREATE TABLE clean.crm_prod_info
	(
		Prod_Id				INT,
		Catg_Id				VARCHAR(20),
		Prod_Key			VARCHAR(30),
		Prod_Name			VARCHAR(50),
		Prod_Cost			INT,
		Prod_Line			VARCHAR(20),
		Prod_Start_Date		DATE,
		Prod_End_Date		DATE,
		dwh_Create_Date		DATETIME2 DEFAULT GETDATE()
	);
	GO

-- See the table:
SELECT * FROM clean.crm_prod_info;
GO

----------------------------------------------------------------------
-- iii. Drop Table if it already exists:
IF OBJECT_ID('clean.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE clean.crm_sales_details;
GO

-- Create the CRM table_3 'Sales_Details':
	CREATE TABLE clean.crm_sales_details
	(
		Sls_Ord_Num			VARCHAR(20),
		Sls_Prod_Key		VARCHAR(30),
		Sls_Cust_Id			INT,
		Sls_Order_Date		DATE,
		Sls_Ship_Date		DATE,
		Sls_Due_Date		DATE,
		Sls_Sales			INT,
		Sls_Quantity		INT,
		Sls_Price			INT,
		dwh_Create_Date		DATETIME2 DEFAULT GETDATE()
	);
	GO

-- See the table:
SELECT * FROM clean.crm_sales_details;
GO

----------------------------------------------------------------------
--ERP: Enterprise Resource Planning.
-- i. Drop Table if it already exists:
IF OBJECT_ID('clean.erp_loc_mst', 'U') IS NOT NULL
    DROP TABLE clean.erp_loc_mst;
GO

-- Create the ERP table_1 'Loc_Mst':
	CREATE TABLE clean.erp_loc_mst
	(
		C_Id				VARCHAR(30),
		Country				VARCHAR(20),
		dwh_Create_Date		DATETIME2 DEFAULT GETDATE()
	);
	GO

-- See the table:
SELECT * FROM clean.erp_loc_mst;
GO

----------------------------------------------------------------------
-- ii. Drop Table if it already exists:
IF OBJECT_ID('clean.erp_cust_mst', 'U') IS NOT NULL
    DROP TABLE clean.erp_cust_mst;
GO

-- Create the ERP table_2 'Cust_Mst':
	CREATE TABLE clean.erp_cust_mst
	(
		C_Id				VARCHAR(30),
		DoB					DATE,
		Gender				VARCHAR(15),
		dwh_Create_Date		DATETIME2 DEFAULT GETDATE()
	);
	GO

-- See the table:
SELECT * FROM clean.erp_cust_mst;
GO

----------------------------------------------------------------------------
-- iii. Drop Table if it already exists:
IF OBJECT_ID('clean.erp_prod_catg', 'U') IS NOT NULL
    DROP TABLE clean.erp_prod_catg;
GO

-- Create the ERP table_3 'Prod_Catg':
	CREATE TABLE clean.erp_prod_catg
	(
		Id					VARCHAR(30),
		Catg				VARCHAR(30),
		Sub_Catg			VARCHAR(30),
		Maintenance			VARCHAR(10),
		dwh_Create_Date		DATETIME2 DEFAULT GETDATE()
	);
	GO

-- See the table:
SELECT * FROM clean.erp_prod_catg;
GO
