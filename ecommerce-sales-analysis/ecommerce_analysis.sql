-- Sales trends by year with YoY growth
SELECT
 *,
 LAG(total_sales) OVER (ORDER BY order_created_year) AS previous_year_sales,
  ((total_sales - LAG(total_sales, 1) OVER (ORDER BY order_created_year))
  / NULLIF(LAG(total_sales, 1) OVER (ORDER BY order_created_year), 0)) * 100 AS pct_change
FROM (
  SELECT 
    LEFT(CAST(created_at AS STRING),4) AS order_created_year,
    SUM(sale_price) AS total_sales
  FROM 
    `mineral-circlet-414407.ecommerce_data.order_items`
  WHERE 
    status != 'Cancelled' AND status != 'Returned'
  GROUP BY 1
) sales_by_year
ORDER BY 1;

-- Top 50 selling products by quantity
SELECT 
  prod.name,
  COUNT(ord.product_id) AS quantity_ordered
FROM 
  `mineral-circlet-414407.ecommerce_data.order_items` ord
JOIN `mineral-circlet-414407.ecommerce_data.products` prod
ON prod.id = ord.product_id
WHERE 
  ord.status != 'Cancelled' AND
  ord.status != 'Returned'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 50;

-- Products that sold only one unit
SELECT 
  prod.name,
  COUNT(ord.product_id) AS quantity_ordered
FROM 
  `mineral-circlet-414407.ecommerce_data.order_items` ord
JOIN `mineral-circlet-414407.ecommerce_data.products` prod
ON prod.id = ord.product_id
WHERE 
  ord.status != 'Cancelled' AND 
  ord.status != 'Returned'
GROUP BY 1
HAVING quantity_ordered = 1
ORDER BY 2;

-- Average purchase value
SELECT 
  AVG(purchase_value) AS avg_purchase_value
FROM (
  SELECT 
    order_id,
    SUM(sale_price) AS purchase_value
  FROM 
    `mineral-circlet-414407.ecommerce_data.order_items`
  WHERE 
    status != 'Cancelled' AND 
    status != 'Returned'
  GROUP BY 1);

-- Average number of purchases per customer
SELECT 
  AVG(customer_purchases_table.number_of_purchases) AS avg_num_of_purchases
FROM (
  SELECT
    user_id,
    COUNT(order_id) AS number_of_purchases
  FROM 
    `mineral-circlet-414407.ecommerce_data.order_items`
  WHERE 
    status != 'Cancelled' AND 
    status != 'Returned'
  GROUP BY 1) customer_purchases_table;

-- Average customer lifespan in days (excluding one-time purchases in 2024)
CREATE TEMP TABLE customer_lifespan_table
AS
SELECT 
  user_id, 
  MIN(created_at) AS first_purchase_date,
  MAX(created_at) AS last_purchase_date,
  DATE_DIFF(MAX(created_at), MIN(created_at), DAY) + 1 AS customer_lifespan_days
FROM 
  `mineral-circlet-414407.ecommerce_data.orders`
WHERE 
  status != 'Cancelled' AND 
  status != 'Returned'
GROUP BY 1;

SELECT 
 AVG(customer_lifespan_days) AS avg_customer_lifespan_days
FROM customer_lifespan_table
WHERE
 NOT (last_purchase_date > '2024-01-01' AND
 customer_lifespan_days = 1);

-- Average purchase value by gender
SELECT 
  gender,
  AVG(purchase_value) AS avg_purchase_value
FROM (
  SELECT 
    ord.order_id,
    use.gender,
    SUM(ord.sale_price) AS purchase_value
  FROM 
    `mineral-circlet-414407.ecommerce_data.order_items` ord
  JOIN 
    `mineral-circlet-414407.ecommerce_data.users` use
  ON ord.user_id = use.id
  WHERE 
    ord.status != 'Cancelled' AND 
    ord.status != 'Returned'
  GROUP BY 1,2)
GROUP BY 1;

-- Create a permanent table for visualization
CREATE OR REPLACE TABLE mineral-circlet-414407.ecommerce_data.customer_purchases_table 
AS
SELECT 
  ord.order_id,
  use.gender,
  SUM(ord.sale_price) AS purchase_value
FROM 
  `mineral-circlet-414407.ecommerce_data.order_items` ord
JOIN 
  `mineral-circlet-414407.ecommerce_data.users` use
ON ord.user_id = use.id
WHERE 
  ord.status != 'Cancelled' AND 
  ord.status != 'Returned'
GROUP BY 1,2;

-- Average number of purchases by gender
SELECT 
  gender,
  AVG(customer_purchases_table.number_of_purchases) AS avg_num_of_purchases
FROM (
  SELECT
    ord.user_id,
    use.gender,
    COUNT(ord.order_id) AS number_of_purchases
  FROM 
   `mineral-circlet-414407.ecommerce_data.order_items` ord
JOIN 
  `mineral-circlet-414407.ecommerce_data.users` use
  ON ord.user_id = use.id
  WHERE 
    ord.status != 'Cancelled' AND 
    ord.status != 'Returned'
  GROUP BY 1,2) customer_purchases_table
GROUP BY 1;

-- Average customer lifespan by gender (excluding one-time purchases in 2024)
SELECT 
  gender,
  AVG(customer_lifespan_days) AS avg_customer_lifespan_days
FROM (
  SELECT 
      ord.user_id, 
      use.gender,
      use.age,
      MIN(ord.created_at) AS first_purchase_date,
      MAX(ord.created_at) AS last_purchase_date,
      DATE_DIFF(MAX(ord.created_at), MIN(ord.created_at), DAY) AS customer_lifespan_days
    FROM 
      `mineral-circlet-414407.ecommerce_data.orders` ord
    JOIN
      `mineral-circlet-414407.ecommerce_data.users` use
    ON ord.user_id = use.id
    WHERE 
      ord.status != 'Cancelled' AND 
      ord.status != 'Returned'
    GROUP BY 1,2,3)
WHERE
  NOT (last_purchase_date > '2024-01-01' AND
  customer_lifespan_days = 1)
GROUP BY 1;
