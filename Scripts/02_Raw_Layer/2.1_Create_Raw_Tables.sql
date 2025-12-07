/*
===================================================================
              DDL Scripts: Create Raw Tables
===================================================================
Project: SQL Server Ingestion Pipeline
Scripts: 2.1_Create_Raw_Tables.sql
Author: Abdul Muqtadeer Baag
Created On: 07/12/2025

Purpose:
    Create raw tables to store incoming CSV data (Exact Copy).

Warning!
    This table holds raw unmodified source data.
    Do not apply any transformations here.

-------------------------------------------------------------------
*/

-- Use the Database:
  USE IngestionDB;
  GO
