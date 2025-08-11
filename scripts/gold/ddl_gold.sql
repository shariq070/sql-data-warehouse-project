USE DataWarehouse
GO
CREATE or ALTER VIEW gold.dim_customers AS
select
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	el.cntry as country,
	ci.cst_marital_status as marital_status,
	CASE	WHEN ci.cst_gndr != 'n/a' THEN  ci.cst_gndr
			ELSE COALESCE (ca.gen,'n/a')
	END gender,
	ca.bdate as birth_date,
	ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 el
on ci.cst_key = el.cid;

GO

CREATE VIEW gold.dim_products AS
select 
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenence,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.ID
where pn.prd_end_dt is null -- filter out historical dat;
