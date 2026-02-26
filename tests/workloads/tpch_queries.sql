-- TPC-H workload for Crucible end-to-end testing.
-- Each block is parsed by the xk6-sql generic driver; the @name annotation
-- becomes the Prometheus Trend metric label (sql_duration_<name>).
--
-- Tables required per query:
--   Q1, Q6              → lineitem
--   Q12                 → lineitem, orders
--   Q3                  → customer, orders, lineitem
--   Q10                 → customer, orders, lineitem, nation
--   Q14, Q19            → lineitem, part
--   Q5                  → customer, orders, lineitem, supplier, nation, region

-- @name: PricingSummaryReport
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity)                                        AS sum_qty,
    SUM(l_extendedprice)                                   AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount))                AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity)                                        AS avg_qty,
    AVG(l_extendedprice)                                   AS avg_price,
    AVG(l_discount)                                        AS avg_disc,
    COUNT(*)                                               AS count_order
FROM lineitem
WHERE l_shipdate <= DATE_SUB('1998-12-01', INTERVAL 90 DAY)
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

-- @name: ForecastRevenueChange
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= '1994-01-01'
  AND l_shipdate <  DATE_ADD('1994-01-01', INTERVAL 1 YEAR)
  AND l_discount  BETWEEN 0.05 AND 0.07
  AND l_quantity  < 24;

-- @name: ShippingPriority
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM customer
JOIN orders   ON c_custkey  = o_custkey
JOIN lineitem ON l_orderkey = o_orderkey
WHERE c_mktsegment = 'BUILDING'
  AND o_orderdate  < '1995-03-15'
  AND l_shipdate   > '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;

-- @name: LocalSupplierVolume
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer
JOIN orders   ON  c_custkey   = o_custkey
JOIN lineitem ON  l_orderkey  = o_orderkey
JOIN supplier ON  l_suppkey   = s_suppkey AND c_nationkey = s_nationkey
JOIN nation   ON  s_nationkey = n_nationkey
JOIN region   ON  n_regionkey = r_regionkey
WHERE r_name        = 'ASIA'
  AND o_orderdate  >= '1994-01-01'
  AND o_orderdate  <  DATE_ADD('1994-01-01', INTERVAL 1 YEAR)
GROUP BY n_name
ORDER BY revenue DESC;

-- @name: ReturnedItemReporting
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM customer
JOIN orders   ON c_custkey   = o_custkey
JOIN lineitem ON l_orderkey  = o_orderkey
JOIN nation   ON c_nationkey = n_nationkey
WHERE o_orderdate  >= '1993-10-01'
  AND o_orderdate  <  DATE_ADD('1993-10-01', INTERVAL 3 MONTH)
  AND l_returnflag = 'R'
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20;

-- @name: ShippingModesOrderPriority
SELECT
    l_shipmode,
    SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH')      THEN 1 ELSE 0 END) AS high_line_count,
    SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH')  THEN 1 ELSE 0 END) AS low_line_count
FROM orders
JOIN lineitem ON o_orderkey = l_orderkey
WHERE l_shipmode IN ('MAIL', 'SHIP')
  AND l_commitdate  < l_receiptdate
  AND l_shipdate    < l_commitdate
  AND l_receiptdate >= '1994-01-01'
  AND l_receiptdate <  DATE_ADD('1994-01-01', INTERVAL 1 YEAR)
GROUP BY l_shipmode
ORDER BY l_shipmode;

-- @name: PromotionEffect
SELECT
    100.00 * SUM(
        CASE WHEN p_type LIKE 'PROMO%'
             THEN l_extendedprice * (1 - l_discount)
             ELSE 0
        END
    ) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem
JOIN part ON l_partkey = p_partkey
WHERE l_shipdate >= '1995-09-01'
  AND l_shipdate <  DATE_ADD('1995-09-01', INTERVAL 1 MONTH);

-- @name: DiscountedRevenue
SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem
JOIN part ON l_partkey = p_partkey
WHERE (
        p_brand     = 'Brand#12'
    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND l_quantity  BETWEEN 1  AND 11
    AND p_size      BETWEEN 1  AND 5
    AND l_shipmode  IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
) OR (
        p_brand     = 'Brand#23'
    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    AND l_quantity  BETWEEN 10 AND 20
    AND p_size      BETWEEN 1  AND 10
    AND l_shipmode  IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
) OR (
        p_brand     = 'Brand#34'
    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    AND l_quantity  BETWEEN 20 AND 30
    AND p_size      BETWEEN 1  AND 15
    AND l_shipmode  IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
);
