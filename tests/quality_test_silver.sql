/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

/* Data Cleaning
Removing Duplicates*/

SELECT 
	cst_id,
	count(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1

SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag
FROM bronze.crm_cust_info

--REMOVE WHITESPACES 
SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status)

-- Data Standardization 
SELECT Distinct cst_gndr
FROM bronze.crm_cust_info

SELECT Distinct cst_marital_status
FROM bronze.crm_cust_info


--Validating Dates
SELECT
prd_key,
prd_start_dt,
LEAD(prd_start_dt,1) OVER(PARTITION BY prd_key order by prd_start_dt)
FROM bronze.crm_prd_info

-- VALIDATING DATEs
SELECT 
NULLIF(sls_order_dt,0)
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) > 8 OR sls_order_dt > 20500101 or sls_order_dt < 19000101

-- INT TO DATE

SELECT 
Format(CAST(Cast(sls_ship_dt AS nvarchar) AS Date),'dd/mm/yyyy')
FROM bronze.crm_sales_details

-- DATA ENRICHMENT FOR PRICE,Quantity, Sales 
SELECT 
	sls_price,
	sls_quantity,
	sls_sales,
	CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales 
	END as New_sales,
	CASE WHEN sls_price <= 0 OR sls_price IS NULL  THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
		ELSE sls_price 
	END as New_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_price * sls_quantity  or sls_price <= 0 OR sls_price IS NULL OR sls_quantity <= 0 or 
sls_quantity is NULL or sls_sales <=0 or sls_sales is NULL
ORDER by sls_price,sls_sales,sls_quantity


---HANDLE NULL / EMPTY DATA / DATA Normalization  

SELECT  
	gen,
	CASE UPPER(ISNULL(TRIM(gen),'')) 
		WHEN 'M' THEN 'Male'
		WHEN 'F' THEN 'Female'
		WHEN '' THEN 'n/a'
		ELSE gen
	END
FROM bronze.erp_cust_az12
WHERE gen = 'M' or gen = 'F' OR gen is null or gen = ''


----Data Standarization 

SELECT DISTINCT 
	cntry,
	CASE WHEN UPPER(TRIM(cntry)) in ('USA','US','UNITED STATES') THEN 'United States'
	WHEN UPPER(TRIM(cntry)) in ('DE','GERMANY') THEN 'Germany'
	WHEN UPPER(TRIM(cntry)) = '' OR UPPER(TRIM(cntry)) IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
	END as cntry
FROM bronze.erp_loc_a101


