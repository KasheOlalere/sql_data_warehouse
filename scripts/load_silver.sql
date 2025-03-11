-- LOAD INTO SILVER LAYER

-- silver.crm_cust_info
INSERT INTO silver.crm_cust_info
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
		WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
		ELSE COALESCE (cst_marital_status, 'N/A')
		END AS 
	cst_marital_status,
		CASE WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
		WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
		ELSE COALESCE (cst_gndr, 'N/A')
		END AS 
	cst_gndr,
	(cst_create_date::DATE - INTERVAL '1 YEAR')::DATE AS cst_create_date
FROM 
	(SELECT *,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as ranks
	FROM bronze.crm_cust_info
	ORDER BY cst_id)
WHERE ranks = 1;


-------------------------------------------------------------------------------------------------
-- SILVER.CRM_PRD_INFO
-------------------------------------------------------------------------------------------------

-- ALTER TABLE silver.crm_prd_info
-- ADD COLUMN cat_id VARCHAR(50);

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
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
    prd_nm,
    COALESCE(prd_cost::INTEGER, 0)::INTEGER AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'R' THEN 'Road'
        WHEN 'T' THEN 'Touring'
        ELSE COALESCE(prd_line, 'N/A')
    END AS prd_line,
    prd_start_dt::DATE,
    (LEAD(prd_start_dt::DATE) OVER(PARTITION BY prd_key ORDER BY prd_start_dt::DATE) - INTERVAL '1 day')::DATE AS 
	prd_end_dt
FROM bronze.crm_prd_info;


-------------------------------------------------------------------------------------------------
-- SILVER.CRM_SALES_DETAIL
-------------------------------------------------------------------------------------------------

INSERT INTO silver.crm_sales_details 
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::varchar) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::varchar) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::varchar) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price  
	END AS sls_price
FROM bronze.crm_sales_details;


-------------------------------------------------------------------------------------------------
-- SILVER.ERP_CUST_AZ12
-------------------------------------------------------------------------------------------------

INSERT INTO silver.erp_cust_az12 
SELECT
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) 
		ELSE cid
	END AS cid, 
	CASE
		WHEN bdate > CURRENT_DATE THEN NULL
		ELSE bdate
	END AS bdate, 
	CASE
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen 
FROM bronze.erp_cust_az12;


-------------------------------------------------------------------------------------------------
-- SILVER.ERP_LOC_A101
-------------------------------------------------------------------------------------------------

INSERT INTO silver.erp_loc_a101 
SELECT
	REPLACE(cid, '-', '') AS cid, 
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry 
FROM bronze.erp_loc_a101;


-------------------------------------------------------------------------------------------------
-- SILVER.ERP_PX_CAT_G1V2
-------------------------------------------------------------------------------------------------

INSERT INTO silver.erp_px_cat_g1v2 
SELECT
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2;
