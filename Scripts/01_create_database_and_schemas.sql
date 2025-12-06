/*
======================================================================
                  Create Database and Schemas
======================================================================
 Project: SQL Server Data Ingestion Pipeline
 Script: 01_create_database_and_schemas.sql
 Author: Abdul Muqtadeer Baag
 Created On: 2025-12-07

 Overview:
   This script initializes the DataWarehouse environment used in the
   SQL Server Ingestion Pipeline. It ensures a clean, consistent
   setup for end-to-end data processing.

 What This Script Does:
   - Checks if the DataWarehouse database already exists.
   - If it exists, it is safely dropped and recreated.
   - Creates the pipeline schema layers:
         1) raw      – stores unprocessed source data.
         2) clean    – stores validated and transformed data.
         3) final    – stores curated, analytics-ready data.


 ⚠ Warning:
     Running this script will DROP the existing DataWarehouse database.
     All current data will be permanently deleted.  
     Make sure you have backups before executing.
*/

-- Use the Master Database:
USE master;
GO

-- Drop IngestionDB if it already exists:
IF DB_ID('IngestionDB') IS NOT NULL
BEGIN
    ALTER DATABASE IngestionDB 
    SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    
    DROP DATABASE IngestionDB;
END;
GO

-- Create the Database:
Create Database IngestionDB;
GO

-- Use the Database:
Use IngestionDB;
GO

-- Create Schema_1:
Create Schema Raw;
Go

-- Create Schema_2:
Create Schema Clean;
GO

-- Create Schema_3:
Create Schema Final;
Go
