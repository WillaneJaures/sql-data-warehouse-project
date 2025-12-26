/*
==============================================================================
Quality Checks
==============================================================================
Script purpose:
    This script performs various quality checks for data consitency, accuracy,
    and standardization across the 'silver' schemas. It include checks for:
    - Null or duplicate primary
    - Unwanted space in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related field

Usage notes: 
    -Run these checks after data loading silver layer.
    -Investigate and resolve any discrepancies found during the checks
================================================================================
*/


--Data Quality Check
--Check For Nulls or Duplicates in Primary Key
-- Expectation: No result

SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT (*) > 1 or prd_id IS NULL


--Check for unwanted spaces
-- Expectation: No results
SELECT 
prd_nm
from silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Data Standardization & Consitency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Nulls or Negative Numbers
--Expectation: No results
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost < 1 OR prd_cost IS NULL



--Check for Invalid Date Orders
SELECT 
NULLIF (sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0






SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) as prd_cost,
CASE UPPER(TRIM(prd_line)) 
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'R' THEN 'Touring'
	ELSE 'n/a'
END AS prod_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) prd_end_dt
FROM bronze.crm_prd_info



SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price 
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0


SELECT 
cid,
bdate,
gen
FROM bronze.erp_cust_az12


SELECT * FROM silver.erp_px_cat_g1v2










	

