import os
import pandas as pd
from sqlalchemy import create_engine, text

user = "postgres"
password = "admin"
host = "localhost"
port = "5432"
database = "warehouse"
schema = "bronze" 

sql_queries = f"""
DROP TABLE IF EXISTS {schema}.crm_cust_info;
CREATE TABLE {schema}.crm_cust_info(
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(5),
    cst_create_date DATE
);

DROP TABLE IF EXISTS {schema}.crm_prd_info;
CREATE TABLE {schema}.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

DROP TABLE IF EXISTS {schema}.crm_sales_details;
CREATE TABLE {schema}.crm_sales_details (
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

DROP TABLE IF EXISTS {schema}.erp_loc_a101;
CREATE TABLE {schema}.erp_loc_a101 (
    cid VARCHAR(50),
    cntry VARCHAR(50)
);

DROP TABLE IF EXISTS {schema}.erp_cust_az12;
CREATE TABLE {schema}.erp_cust_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50)
);

DROP TABLE IF EXISTS {schema}.erp_px_cat_g1v2;
CREATE TABLE {schema}.erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);
"""

try:
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
    with engine.connect() as conn:
        conn.execute(text(sql_queries))
        conn.commit()  

    print("Tables successfully created in schema:", schema)

except Exception as e:
    print("Error:", e)

finally:
    engine.dispose()  
