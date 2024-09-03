
-- Context: I analyzed a publicly available dataset on orders placed by customers for an ecommerce company.
-- Data Source: BigQuery's thelook_ecommerce dataset
-- Goal: 1) Identify sales and revenue trends; 2) Provide actionable recommendations for the ecommerce company based on these trends.

-- Key Questions: 
-- 1) What are the current sales trends, and how do they compare to previous periods?
-- 2) Which products are the top sellers, and which are underperforming?
-- 3) What is the average purchase value, and how can it be increased?
-- 4) What is the customer lifetime value (CLV), and how does it vary by segment?

-- Key Findings:
-- 1) Sales initially more than tripled from 2019 to 2020 and has increased by double digits every year at a slower rate (see lines 21 - 38).
-- 2) 5 of the Top 10 products are Men's Jeans (see lines 40 - 55).
-- 3) There are more than a thousand products that sold only one piece (see lines 57 - 71).
-- 4) On average, customers purchase about $86 worth of goods per order (see lines 78 - 91). Since men have a larger average purchase value than women, recommend targeting men to increase average purchase value (see lines 168 - 186).
-- 5) The average customer lifetime value $77 (see lines 73 - 164). Average customer lifetime value for men is $83 while average customer lifetime value for women is $73 (see lines 167 - 253).

-- Note: Analysis was conducted on Sept. 2, 2024. The dataset is continually refreshed and updated and so figures may differ slightly from when analysis was conducted.

-- Shows current sales trends by year and year-over-year percent change.
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
    status != 'Cancelled' AND 
    status != 'Returned'
  GROUP BY 1
  ORDER BY 1) sales_by_year_table
ORDER BY 1;

-- Shows top 50 selling products by number of products sold, excluding cancelled and returned orders.
-- Excluded cancelled and returned orders because ultimately the ecommerce company didn't end up making revenues from those orders.
-- 5 of the Top 10 products are Men's Jeans.
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

-- Shows lowest performing products by number of products sold, excluding cancelled and returned orders.
-- There are more than a thousand products that sold only one piece.
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

-- Customer lifetime value (CLTV)
-- CLTV = Customer Value x Average Customer Lifespan
-- Customer Value = Average Purchase Value x Average Number of Purchases
-- Average Customer Lifespan (ACL) = Sum of Customer Lifespans / Number of Customers

-- Shows average purchase value, excluding cancelled and returned purchases.
-- On average, customers purchase about $86 worth of goods per order.
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

-- Shows average number of purchases, excluding cancelled and returned purchases.
-- On average, each customer makes about two purchases.
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

-- Customer Value = ($86 average purchase value) x (2 average number of purchases) = $172

-- Shows Average Customer Lifespan, excluding cancelled or returned orders.
-- On average, customers are active for 121 days (or 1/3 of a year).
SELECT 
  AVG(customer_lifespan_days) AS avg_customer_lifespan_days
FROM (
  SELECT 
      user_id, 
      MIN(created_at) AS first_purchase_date,
      MAX(created_at) AS last_purchase_date,
      DATE_DIFF(MAX(created_at), MIN(created_at), DAY)+ 1 AS customer_lifespan_days
    FROM 
      `mineral-circlet-414407.ecommerce_data.orders`
    WHERE 
      status != 'Cancelled' AND 
      status != 'Returned'
    GROUP BY 1) customer_lifespan_table;

-- Temp Table
DROP TABLE IF EXISTS mineral-circlet-414407.ecommerce_data.customer_lifespan_table;
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

-- However, the average customer lifespan of 120 days maybe skewed by the fact that 46,052 unique customers out of 65,838 unique customers have made only one purchase
-- Shows distribution of 65,838 unique customers who made only one purchase by year
-- Of the 46,052 unique customers that made only one purchase, 17,887 customers made their one-time purchase on 2024
SELECT 
  LEFT(CAST(first_purchase_date AS STRING), 4) AS year,
  COUNT(user_id) AS quantity_of_one_time_purchase
FROM customer_lifespan_table
WHERE first_purchase_date = last_purchase_date
GROUP BY 1
ORDER BY 1 DESC;

-- Assume unique customers who made a one time purchase on 2024 are still active while unique customers who made a one-time purchase before 2024 are no longer active.
-- Shows average customer lifespan, excluding unique customers who have made only one purchase in 2024 so it doesn't skew the calculation
-- On average, customers are active for 165 days (or almost half of a year).
SELECT 
  AVG(customer_lifespan_days) AS avg_customer_lifespan_days
FROM customer_lifespan_table
WHERE
  NOT (last_purchase_date > '2024-01-01' AND
  customer_lifespan_days = 1);

-- Customer lifetime value = ($172 Customer Value) x (0.45 years) = $77


-- Customer lifetime value (CLTV) varies between men and women segments.
-- Shows men's average purchase value is materially higher than women's ($92 vs. $81).
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

-- Permanent table for data visualizations
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

-- Shows on average, both men and women make about two purchases.
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

-- Shows average customer lifespan for both men and women are quite similar (164 days for men vs. 165 days for women).
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
  customer_lifespan_days = 0)
GROUP BY 1;

-- CLTV for men = ($92 average purchase value) x (2 average number of purchases) x (0.45 years) = $83
-- CLTV for women = ($81 average purchase value) x (2 average number of purchases) x (0.45 years) = $73
-- To increase overall CLTV, recommend e-commerce company target more men