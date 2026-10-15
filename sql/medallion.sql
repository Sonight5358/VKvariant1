CREATE SCHEMA IF NOT EXISTS lakehouse.bronze;
CREATE SCHEMA IF NOT EXISTS lakehouse.silver;
CREATE SCHEMA IF NOT EXISTS lakehouse.gold;
CREATE SCHEMA IF NOT EXISTS lakehouse.demo;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.customer
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT custkey, mktsegment FROM tpch.sf1.customer;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.orders
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, custkey, orderdate, shippriority FROM tpch.sf1.orders;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.lineitem
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, linenumber, extendedprice, discount, shipdate FROM tpch.sf1.lineitem;

CREATE TABLE IF NOT EXISTS lakehouse.silver.order_lines
WITH (format='PARQUET', compression_codec='ZSTD') AS
WITH customer_clean AS (
  SELECT custkey, upper(trim(mktsegment)) AS mktsegment
  FROM lakehouse.bronze.customer
  WHERE custkey > 0 AND mktsegment IS NOT NULL
), orders_clean AS (
  SELECT orderkey, custkey, orderdate, shippriority
  FROM lakehouse.bronze.orders
  WHERE orderkey > 0 AND custkey > 0
    AND orderdate IS NOT NULL AND shippriority IS NOT NULL
), lineitem_clean AS (
  SELECT orderkey, linenumber,
         CAST(extendedprice AS DECIMAL(18,2)) AS extendedprice,
         CAST(discount AS DECIMAL(3,2)) AS discount, shipdate
  FROM lakehouse.bronze.lineitem
  WHERE orderkey > 0 AND linenumber > 0 AND extendedprice >= 0
    AND discount BETWEEN 0 AND 1 AND shipdate IS NOT NULL
)
SELECT l.orderkey, l.linenumber, o.custkey, o.orderdate, o.shippriority,
       l.shipdate, c.mktsegment, l.extendedprice, l.discount,
       l.extendedprice * (DECIMAL '1.00' - l.discount) AS net_revenue,
       date_diff('day', o.orderdate, l.shipdate) AS shipment_lag_days
FROM customer_clean c
JOIN orders_clean o ON c.custkey=o.custkey
JOIN lineitem_clean l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15';

CREATE TABLE IF NOT EXISTS lakehouse.gold.order_revenue
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, sum(net_revenue) AS revenue, orderdate, shippriority
FROM lakehouse.silver.order_lines
GROUP BY orderkey, orderdate, shippriority;

CREATE TABLE IF NOT EXISTS lakehouse.gold.shipping_priority_top10
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, revenue, orderdate, shippriority
FROM lakehouse.gold.order_revenue
ORDER BY revenue DESC
LIMIT 10;
