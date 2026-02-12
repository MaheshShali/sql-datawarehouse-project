/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM data for gender is master data 
		 ELSE ISNULL(ca.gen,'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_marital_status AS marital_status,
	la.cntry AS country,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY po.prd_start_dt ,po.prd_id) AS product_key,
	po.prd_id AS product_id,
	po.prd_key AS product_number,
	po.prd_nm AS product_name,
	po.cat_id AS category_id,
	COALESCE(px.cat,'n/a') AS product_category,
	COALESCE(px.subcat,'n/a') AS product_subcategory,
	COALESCE(px.maintenance,'n/a') AS maintenance,
	po.prd_line AS product_line,
	po.prd_cost AS product_cost,
	po.prd_start_dt AS product_start_date
FROM silver.crm_prd_info po
LEFT JOIN silver.erp_px_cat_g1v2 px
ON po.cat_id = px.id
WHERE po.prd_end_dt IS NULL;
GO 

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
   DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.facts_sales AS 
SELECT 
	 si.sls_ord_num  AS order_number,
    gpx.product_key  AS product_key,
    gci.customer_key AS customer_key,
    si.sls_order_dt AS order_date,
    si.sls_ship_dt  AS shipping_date,
    si.sls_due_dt   AS due_date,
    si.sls_sales    AS sales_amount,
    si.sls_quantity AS quantity,
    si.sls_price    AS price
FROM silver.crm_sales_details si
LEFT JOIN gold.dim_products gpx
ON si.sls_prd_key = gpx.product_number
LEFT JOIN gold.dim_customers gci
ON si.sls_cust_id = gci.customer_id

GO
