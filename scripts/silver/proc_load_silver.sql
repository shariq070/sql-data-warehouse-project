/*
    PURPOSE:
    This ETL script transforms and loads raw CRM and ERP data from the bronze staging layer 
    into the silver layer. It applies data cleaning, enrichment, and normalization rules to ensure 
    consistency, deduplication, and integrity before loading. 
*/

-- Procedure to load cleaned and standardized data from the bronze staging layer into the silver cleansed layer.
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    -- Load CRM Customer Info
    TRUNCATE TABLE silver.crm_cust_info;
    
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        TRIM(cst_key),
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        -- Normalize marital status codes
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        -- Normalize gender codes
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flaging
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flaging = 1; -- Keep only the most recent entry per customer

    -- Load CRM Product Info
    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Derive category ID
        SUBSTRING(prd_key, 7, LEN(prd_key)),
        TRIM(prd_nm),
        COALESCE(prd_cost, 0),
        -- Normalize product line codes
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        CAST(prd_start_dt AS DATE),
        -- Set end date to day before next start date (if exists)
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY prd_id ORDER BY prd_start_dt DESC) AS flaging
        FROM bronze.crm_prd_info
        WHERE prd_id IS NOT NULL
    ) t
    WHERE flaging = 1;

    -- Load CRM Sales Details
    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        -- Clean and cast date fields (assuming YYYYMMDD format)
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END,
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END,
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END,
        -- Recalculate sales amount if invalid
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
             THEN sls_quantity * ABS(sls_price)
             ELSE sls_sales
        END,
        sls_quantity,
        -- Recalculate price if missing or invalid
        CASE WHEN sls_price IS NULL OR sls_price <= 0
             THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    -- Load ERP Customer Demographics
    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        -- Remove NAS prefix from customer ID
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
        -- Remove invalid future birthdates
        CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
        -- Normalize gender values
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    -- Load ERP Location Data
    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', ''),
        -- Normalize country values
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
            WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    -- Load ERP Product Categories (no transformation needed)
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 (
        ID,
        cat,
        subcat,
        maintenence
    )
    SELECT
        ID,
        cat,
        subcat,
        maintenence
    FROM bronze.erp_px_cat_g1v2;
END
