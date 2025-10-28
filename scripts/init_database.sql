/*
--------------------------------------------------------------------------------
Project : Data Warehouse creation script
Purpose : Create (drop if exists) and initialize the DataWarehouse database
Author  : Aakash Vishwakarma
Date    : 2025-10-28
Env     : Microsoft SQL Server (T-SQL). Tested for SSMS / sqlcmd compatibility.
Notes   : 
  - Script sets database to SINGLE_USER before dropping to ensure no open sessions.
  - Uses GO batch separator for SSMS/sqlcmd.
  - Creates bronze, silver, gold schemas for data layering.
Revision History:
  2025-10-28  v1.0  Initial creation and formatting with header comments

WARNING:
  Running this script will drop the entire 'DataWarehouse' database if it exists.
  All data in the database will be permanently deleted. Proceed with caution
  and ensure you have proper backups before running this script.
--------------------------------------------------------------------------------
*/

-- Use the master database to manage databases
USE master;
GO

-- Drop and recreate the 'DataWarehouse' database if it already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- Force single user mode and rollback open transactions, then drop
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the new database
USE DataWarehouse;
GO

-- Create schemas for data-layer separation
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
