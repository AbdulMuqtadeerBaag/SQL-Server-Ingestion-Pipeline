/*
=========================================================================
               DDL Scripts: Create Database and Schemas
=========================================================================
 Project: SQL Server Data Ingestion Pipeline
 Script: 01_create_database_and_schemas.sql
 Author: Abdul Muqtadeer Baag
 Created On: 07/12/2025

 Overview:
   This script initializes the IngestionDB environment used in the
   SQL Server Ingestion Pipeline. It ensures a clean, consistent
   setup for end-to-end data processing.

 What This Script Does:
   - Checks if the DataWarehouse database already exists.
   - If it exists, it is safely dropped and recreated.
   - Creates the pipeline schema layers:
         1) raw      – stores unprocessed source data.
         2) clean    – stores validated and transformed data.
         3) final    – stores curated, analytics-ready data.

⚠Warning:
     Running this script will DROP the existing DataWarehouse database.
     All current data will be permanently deleted.  
     Make sure you have backups before executing.

==========================================================================
*/

-- Use the Master Database:
  USE master;
  GO

-- Drop IngestionDB if it already exists:
  IF DB_ID('IngestionDB') IS NOT NULL
    BEGIN
        ALTER DATABASE [IngestionDB]
        SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
        
        DROP DATABASE [IngestionDB];
        PRINT('IngestionDB Dropped Successfully!');
    END;
  ELSE
    BEGIN
        PRINT('IngestionDB doesn`t exist. Creating a new Database...');
    END;
  GO

-- Create the Database:
  CREATE DATABASE [IngestionDB];
  GO

-- Use the Database:
  USE [IngestionDB];
  GO

-- Check Current Database:
  SELECT DB_NAME() AS Database_Name;
  GO
    
-- Create Schema_1:
  CREATE SCHEMA [raw]
    AUTHORIZATION dbo;
  Go

-- Create Schema_2:
   CREATE SCHEMA [clean]
    AUTHORIZATION dbo;
  Go

-- Create Schema_3:
   CREATE SCHEMA [final]
    AUTHORIZATION dbo;
  Go
