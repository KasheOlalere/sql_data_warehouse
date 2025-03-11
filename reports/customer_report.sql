CREATE VIEW gold.report_customers AS

-- 1) Base Query: Retrieves core columns from tables  

WITH base_query AS (

SELECT
    f.order_number,
    f.product_key,
    f.order_date,
    f.sales_amount,
    f.quantity,
    c.customer_key,
    c.customer_number,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    DATE_PART('year', AGE(NOW(), c.birthdate)) AS age  -- Fix for DATEDIFF(year, c.birthdate, GETDATE())
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
WHERE f.order_date IS NOT NULL
),

--2) Customer Aggregations: Summarizes key metrics at the customer level  

customer_aggregation AS (

SELECT 
    customer_key,
    customer_number,
    customer_name,
    age,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT product_key) AS total_products,
    MAX(order_date) AS last_order_date,
    DATE_PART('month', AGE(MAX(order_date), MIN(order_date))) AS lifespan -- Fix for DATEDIFF(month)
FROM base_query
GROUP BY 
    customer_key,
    customer_number,
    customer_name,
    age
)

SELECT
    customer_key,
    customer_number,
    customer_name,
    age,

    -- Age Grouping
    CASE 
        WHEN age < 20 THEN 'Under 20'
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        ELSE '50 and above'
    END AS age_group,

    -- Customer Segmentation
    CASE 
        WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,

    last_order_date,
    DATE_PART('month', AGE(NOW(), last_order_date)) AS recency, -- Fix for DATEDIFF(month, last_order_date, GETDATE())
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    lifespan,

    -- Compute Average Order Value (AOV)
    CASE 
        WHEN total_sales = 0 THEN 0
        ELSE total_sales / total_orders
    END AS avg_order_value,

    -- Compute Average Monthly Spend
    CASE 
        WHEN lifespan = 0 THEN total_sales
        ELSE total_sales / lifespan
    END AS avg_monthly_spend

FROM customer_aggregation;
