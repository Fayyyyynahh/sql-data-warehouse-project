/*

Usage Example:
      EXEC bronze_schema.load_bronze;
========================================================================
*/

  
CREATE OR ALTER PROCEDURE bronze_schema.load_bronze AS 
BEGIN
  DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
  BEGIN TRY
    SET @batch_start_time = GETDATE();
    PRINT '========================';
    PRINT 'Loading Bronze Layer';
    PRINT '========================';
  
    PRINT '------------------------';
    PRINT 'Loading CRM Tables';
    PRINT '------------------------';
    
    SET @start_time = GETDATE();
    PRINT '>>Truncating Table: bronze_schema.crm_cust_info';
    TRUNCATE TABLE bronze_schema.crm_cust_info;

    PRINT '>> Inserting Data Into: bronze_schema.crm_cust_info';
    BULK INSERT bronze_schema.crm_cust_info
    FROM 'C: \sql\dwh_project\datasets\source_crm\cust_info.csv'
    WITH(
      FIRSTROW = 2,
      FIELDTERMINATOR = ','
      TABLOCK
      );
    SET @end_time = GETDATE();
    PRINT'>> Load Duration:' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT'>> -------------------------------------------------';


    SET @start_time = GETDATE();
    PRINT '>>Truncating Table: bronze_schema.crm_prd_info';
    TRUNCATE TABLE bronze_schema.crm_prd_info;

    PRINT '>> Inserting Data Into: bronze_schema.crm_prd_info';
    BULK INSERT bronze_schema.crm_prd_info
    FROM 'C: \sql\dwh_project\datasets\source_crm\prd_info.csv'
    WITH(
      FIRSTROW = 2,
      FIELDTERMINATOR = ','
      TABLOCK
      );
    SET @end_time = GETDATE();
    PRINT'>> Load Duration:' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT'>> -------------------------------------------------';


    SET @start_time = GETDATE();
    PRINT '>>Truncating Table: bronze_schema.crm_sales_details';
    TRUNCATE TABLE bronze_schema.crm_sales_details;
    
    PRINT '>> Inserting Data Into: bronze_schema.crm_sales_details';
    BULK INSERT bronze_schema.crm_sales_details
    FROM 'C: \sql\dwh_project\datasets\source_crm\sales_details.csv'
    WITH(
      FIRSTROW = 2,
      FIELDTERMINATOR = ','
      TABLOCK
      );
    SET @end_time = GETDATE();
    PRINT'>> Load Duration:' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT'>> -------------------------------------------------';

    PRINT '------------------------';
    PRINT 'Loading ERP Tables';
    PRINT '------------------------';

    SET @start_time = GETDATE();
    PRINT '>>Truncating Table: bronze_schema.erp_loc_a101';
    TRUNCATE TABLE bronze_schema.erp_loc_a101;

    PRINT '>> Inserting Data Into: bronze_schema.erp_loc_a101';
    BULK INSERT bronze_schema.erp_loc_a101
    FROM 'C: \sql\dwh_project\datasets\source_erp\LOC_A101.csv'
    WITH(
      FIRSTROW = 2,
      FIELDTERMINATOR = ','
      TABLOCK
      );
    SET @end_time = GETDATE();
    PRINT'>> Load Duration:' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT'>> -------------------------------------------------';

    SET @start_time = GETDATE();
    PRINT '>>Truncating Table: bronze_schema.erp_cust_az12';
    TRUNCATE TABLE bronze_schema.erp_cust_az12;

    PRINT '>> Inserting Data Into: bronze_schema.erp_cust_az12';
    BULK INSERT bronze_schema.erp_cust_az12
    FROM 'C: \sql\dwh_project\datasets\source_erp\CUST_AZ12.csv'
    WITH(
      FIRSTROW = 2,
      FIELDTERMINATOR = ','
      TABLOCK
      );
    SET @end_time = GETDATE();
    PRINT'>> Load Duration:' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT'>> -------------------------------------------------';

    SET @start_time = GETDATE();
    PRINT '>>Truncating Table: bronze_schema.erp_px_cat_glv2'; 
    TRUNCATE TABLE bronze_schema.erp_px_cat_glv2;

    PRINT '>> Inserting Data Into: bronze_schema.erp_px_cat_glv2';
    BULK INSERT bronze_schema.erp_px_cat_glv2
    FROM 'C: \sql\dwh_project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH(
      FIRSTROW = 2,
      FIELDTERMINATOR = ','
      TABLOCK
      );
    SET @end_time = GETDATE();
    PRINT'>> Load Duration:' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT'>> -------------------------------------------------';
    SET @batch_end_time = GETDATE();
    PRINT'=======================================';
    PRINT 'Loading Bronze Layer is Completed';
    PRINT '  - Total Load Duration: ' + CAST (DATEDIFF (SECOND,@batch_start_time, @batch_end_time) AS NARCHAR) + ' seconds';
    PRINT '=======================================';
  END TRY
  BEGIN CATCH
        PRINT'==================================================='
        PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER' 
        PRINT'Error Message' + ERROR_MESSAGE ();
        PRINT'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT'===================================================='
    END CATCH
END







