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
on ci.cst_key = el.cid

select prd_id, count(*)
from 
(select 
	pi.prd_id,
	pi.cat_id,
	pi.prd_key,
	pi.prd_nm,
	pi.prd_cost,
	pi.prd_line,
	pi.prd_start_dt,
	pi.prd_end_dt,
	pc.cat,
	pc.subcat,
	pc.maintenence
from silver.crm_prd_info pi
left join silver.erp_px_cat_g1v2 pc
on pi.cat_id = pc.ID
)t 
group by prd_id
having count(*)>0