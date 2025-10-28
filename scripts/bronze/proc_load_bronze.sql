/*
======================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
======================================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the `BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:

  None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC bronze. load_bronze;
=======================================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    BEGIN TRY
        SET NOCOUNT ON;

        DECLARE 
            @start_time DATETIME,
            @end_time DATETIME,
            @duration NVARCHAR(50),
            @batch_start_time DATETIME,
            @batch_end_time DATETIME,
            @batch_duration NVARCHAR(50);

        SET @batch_start_time = GETDATE();
        PRINT '===============================================================';
        PRINT 'STARTING BRONZE LAYER DATA LOAD PROCESS';
        PRINT '===============================================================';
        PRINT CHAR(13);

        --------------------------------------------------------------------------------
        -- CRM Source Data Loads
        --------------------------------------------------------------------------------
        PRINT '---------------------------------------------------------------';
        PRINT 'LOADING CRM SOURCE TABLES';
        PRINT '---------------------------------------------------------------';

        -- Load Customer Info
        PRINT 'Loading bronze.crm_cust_info ...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\Aakash Vishwakarma\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        SET @duration = CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10));
        PRINT 'Loaded bronze.crm_cust_info in ' + @duration + ' seconds.';
        PRINT CHAR(13);

        -- Load Product Info
        PRINT 'Loading bronze.crm_prd_info ...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\Aakash Vishwakarma\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        SET @duration = CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10));
        PRINT 'Loaded bronze.crm_prd_info in ' + @duration + ' seconds.';
        PRINT CHAR(13);

        -- Load Sales Details
        PRINT 'Loading bronze.crm_sales_details ...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\Aakash Vishwakarma\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        SET @duration = CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10));
        PRINT 'Loaded bronze.crm_sales_details in ' + @duration + ' seconds.';
        PRINT CHAR(13);

        --------------------------------------------------------------------------------
        -- ERP Source Data Loads
        --------------------------------------------------------------------------------
        PRINT '---------------------------------------------------------------';
        PRINT 'LOADING ERP SOURCE TABLES';
        PRINT '---------------------------------------------------------------';

        -- ERP Customer
        PRINT 'Loading bronze.erp_cust_az12 ...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\Aakash Vishwakarma\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        SET @duration = CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10));
        PRINT 'Loaded bronze.erp_cust_az12 in ' + @duration + ' seconds.';
        PRINT CHAR(13);

        -- ERP Location
        PRINT 'Loading bronze.erp_loc_a101 ...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\Aakash Vishwakarma\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        SET @duration = CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10));
        PRINT 'Loaded bronze.erp_loc_a101 in ' + @duration + ' seconds.';
        PRINT CHAR(13);

        -- ERP Product Category
        PRINT 'Loading bronze.erp_px_cat_g1v2 ...';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\Aakash Vishwakarma\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        SET @duration = CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10));
        PRINT 'Loaded bronze.erp_px_cat_g1v2 in ' + @duration + ' seconds.';
        PRINT CHAR(13);

        --------------------------------------------------------------------------------
        -- Summary Report
        --------------------------------------------------------------------------------

        PRINT '---------------------------------------------------------------';
        PRINT 'All Bronze Tables Loaded Successfully';
        PRINT '===============================================================';
        PRINT CHAR(13);

        PRINT '===============================================================';
        PRINT 'BRONZE LAYER DATA LOAD SUMMARY.';
        ----------------------------------------------------------------------------------
        -- Calculating Bronze Batch loading duration
        ----------------------------------------------------------------------------------
        SET @batch_end_time = GETDATE();
        SET @batch_duration = CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(10));
        PRINT '     - Total Load Duration : ' + @batch_duration + ' seconds.';
        PRINT '===============================================================';


        -- To check Tables Data and added Data count
        SELECT 
            'bronze.crm_cust_info'      AS TableName, COUNT(*) AS RecordCount FROM bronze.crm_cust_info
        UNION ALL
        SELECT 'bronze.crm_prd_info',   COUNT(*) FROM bronze.crm_prd_info
        UNION ALL
        SELECT 'bronze.crm_sales_details', COUNT(*) FROM bronze.crm_sales_details
        UNION ALL
        SELECT 'bronze.erp_cust_az12',  COUNT(*) FROM bronze.erp_cust_az12
        UNION ALL
        SELECT 'bronze.erp_loc_a101',   COUNT(*) FROM bronze.erp_loc_a101
        UNION ALL
        SELECT 'bronze.erp_px_cat_g1v2', COUNT(*) FROM bronze.erp_px_cat_g1v2
        ORDER BY TableName;



    END TRY

    BEGIN CATCH
        PRINT 'An error occurred during Bronze layer load.';
        PRINT 'Error Message   : ' + ERROR_MESSAGE();
        PRINT 'Error Number    : ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error Line      : ' + CAST(ERROR_LINE() AS NVARCHAR(10));
        PRINT 'Error Procedure : ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
    END CATCH
END;
GO

--------------------------------------------------------------------------------
-- Execute the Procedure to Load Data and Verify Counts
--------------------------------------------------------------------------------
EXEC bronze.load_bronze;
GO
