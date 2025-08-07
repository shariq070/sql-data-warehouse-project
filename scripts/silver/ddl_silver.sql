/* 
    PURPOSE:
    This script initializes staging (bronze layer) tables for CRM and ERP datasets 
    by dropping existing tables (if any) and recreating them with the appropriate schema. 
    These tables are typically used in a data warehouse ETL pipeline to store raw ingested data.
*/

-- Drop and recreate CRM Customer Info table
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR (50),
	cst_firstname NVARCHAR (50),
	cst_lastname NVARCHAR (50),
	cst_marital_status NVARCHAR (50),
	cst_gndr NVARCHAR (50),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Drop and recreate CRM Product Info table
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
	prd_id			INT,
	cat_id			NVARCHAR (50),
	prd_key			NVARCHAR (50),
	prd_nm			NVARCHAR (50),
	prd_cost		INT,
	prd_line		NVARCHAR (50),
	prd_start_dt	DATE,
	prd_end_dt		DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Drop and recreate CRM Sales Details table
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR (50),
	sls_prd_key NVARCHAR (50),
	sls_cust_id INT,
	sls_order_dt DATE,       -- Typically stored as YYYYMMDD integer
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Drop and recreate ERP Customer Demographics table
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen VARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Drop and recreate ERP Customer Location table
IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Drop and recreate ERP Product Category table
IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
	ID			NVARCHAR(50),
	cat			NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenence NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
