/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Create bronze_schema Tables

Script Purpose:
This script creates tables in the 'bronze schema, dropping existing tables if they already exist.
Run this script to re-define the DDL structure of bronze' Tables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/


IF OBJECT_ID('bronze_schema.crm_cust_info','U') IS NOT NULL
  DROP TABLE bronze_schema.crm_cust_info;

GO

CREATE TABLE bronze_schema.crm_cust_info(
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_material_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);
GO
  
IF OBJECT_ID('bronze_schema.crm_prd_info','U') IS NOT NULL
  DROP TABLE  bronze_schema.crm_prd_info;
GO
  
CREATE TABLE bronze_schema.crm_prd_info(
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(10),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);
GO
  
IF OBJECT_ID('ronze_schema.crm_sales_details','U') IS NOT NULL
  DROP TABLE bronze_schema.crm_sales_details;
GO
  
CREATE TABLE bronze_schema.crm_sales_details(
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);
GO

IF OBJECT_ID('bronze_schema.erp_loc_a101','U') IS NOT NULL
  DROP TABLE bronze_schema.erp_loc_a101;
GO
  
CREATE TABLE bronze_schema.erp_loc_a101(
    cid VARCHAR(50),
    cntry VARCHAR(50)
);
GO
  
IF OBJECT_ID('bronze_schema.erp_cust_az12','U') IS NOT NULL
  DROP TABLE bronze_schema.erp_cust_az12;
GO
  
CREATE TABLE bronze_schema.erp_cust_az12(
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50)
);
GO

IF OBJECT_ID('bronze_schema.erp_px_cat_glv2','U') IS NOT NULL
  DROP TABLE bronze_schema.erp_px_cat_glv2;
GO
  
CREATE TABLE bronze_schema.erp_px_cat_glv2(
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);


CREATE OR ALTER PROCEDURE bronze_schema.load_bronze AS 
BEGIN
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
   
    PRINT '------------------------';
    PRINT 'Loading ERP Tables';
    PRINT '------------------------';

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







