-- ===============================================
-- Procedure: bronze.load_bronze
-- Purpose: This procedure loads the raw CSV data from 
-- CRM and ERP systems into the Bronze layer of the 
-- data warehouse. It performs a truncate and bulk load 
-- for each source file into respective staging tables.
-- ===============================================
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '=======================================';
        PRINT '        LOADING BRONZE LAYER';
        PRINT '=======================================';

        -- Begin CRM Tables Load
        PRINT '---------------------------------------';
        PRINT '        LOADING CRM TABLES';
        PRINT '---------------------------------------';

        -- Load CRM Customer Info
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info.'
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> INSERTING DATA INTO: bronze.crm_cust_info.'
        BULK INSERT bronze.crm_cust_info
        FROM 'E:\Data Analysis Road Map\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH
        (
            FIRSTROW = 2,              -- Skips header row
            FIELDTERMINATOR = ',',     -- CSV delimiter
            TABLOCK                    -- Optimizes bulk insert
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' +  CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' Seconds';

        -- Load CRM Product Info
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info.'
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> INSERTING DATA INTO: bronze.crm_prd_info'
        BULK INSERT bronze.crm_prd_info
        FROM 'E:\Data Analysis Road Map\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' +  CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' Seconds';

        -- Load CRM Sales Details
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details'
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> INSERTING DATA INTO: bronze.crm_sales_details'
        BULK INSERT bronze.crm_sales_details
        FROM 'E:\Data Analysis Road Map\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' +  CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' Seconds';

        -- Begin ERP Tables Load
        PRINT '---------------------------------------';
        PRINT '         LOADING ERP TABLES';
        PRINT '---------------------------------------';

        -- Load ERP Customer Data (AZ12)
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12'
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> INSERTING DATA INTO: bronze.erp_cust_az12'
        BULK INSERT bronze.erp_cust_az12
        FROM 'E:\Data Analysis Road Map\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' +  CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' Seconds';

        -- Load ERP Location Data (LOC_A101)
        SET @start_time = GETDATE()
        PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101'
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> INSERTING DATA INTO: bronze.erp_loc_a101'
        BULK INSERT bronze.erp_loc_a101
        FROM 'E:\Data Analysis Road Map\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE()
        PRINT '>> Load Duration: ' +  CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' Seconds';

        -- Load ERP Product Category Data (PX_CAT_G1V2)
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2'
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> INSERTING DATA INTO: bronze.erp_px_cat_g1v2'
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'E:\Data Analysis Road Map\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        SET @batch_end_time = GETDATE();
        PRINT '>> Load Duration: ' +  CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' Seconds';

        PRINT ' ';
        PRINT '============================================';
        PRINT '       LOADING BRONZE LAYER COMPLETED       ';
        PRINT '>> Batch Load Duration: ' +  CAST(DATEDIFF(second, @batch_start_time,@batch_end_time) AS NVARCHAR) + ' Seconds';
        PRINT '============================================';

    END TRY

    BEGIN CATCH
        -- Logs error details for debugging
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message ' + ERROR_MESSAGE();
        PRINT 'Error Number ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Error Line ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT 'Error Procedure ' + ERROR_PROCEDURE();
        PRINT '==========================================';
    END CATCH
END
