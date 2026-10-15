"""Materialize the medallion; verify values, keys, cleaning, and row counts."""
import json
import statistics
from benchmark import same_q3, q3, table_metrics
from trino_client import ROOT, RESULTS, query, save

for index, statement in enumerate((ROOT / 'sql/medallion.sql').read_text(encoding='utf-8-sig').split(';')):
    if statement.strip():
        record = query(statement, f'pipeline:{index}')
        print(record['label'], record['stats']['elapsedTimeMillis']/1000, 's', flush=True)

checks = {}
checks['bronze_counts'] = query('''SELECT
  (SELECT count(*) FROM lakehouse.bronze.customer),
  (SELECT count(*) FROM lakehouse.bronze.orders),
  (SELECT count(*) FROM lakehouse.bronze.lineitem)''', 'validation:counts')['data'][0]
assert checks['bronze_counts'] == [150000, 1500000, 6001215]
for table, key in [('customer', 'custkey'), ('orders', 'orderkey'), ('lineitem', 'orderkey, linenumber')]:
    duplicates = query(f'SELECT count(*) FROM (SELECT {key} FROM lakehouse.bronze.{table} '
                       f'GROUP BY {key} HAVING count(*) > 1)', f'validation:duplicate:{table}')['data'][0][0]
    checks[f'duplicate_{table}_keys'] = duplicates
    assert duplicates == 0
checks['bad_customer'] = query("SELECT count(*) FROM lakehouse.bronze.customer WHERE custkey IS NULL OR custkey<=0 OR mktsegment IS NULL", 'validation:bad_customer')['data'][0][0]
checks['bad_orders'] = query("SELECT count(*) FROM lakehouse.bronze.orders WHERE orderkey IS NULL OR orderkey<=0 OR custkey IS NULL OR custkey<=0 OR orderdate IS NULL OR shippriority IS NULL", 'validation:bad_orders')['data'][0][0]
checks['bad_lineitem'] = query("SELECT count(*) FROM lakehouse.bronze.lineitem WHERE orderkey IS NULL OR orderkey<=0 OR linenumber IS NULL OR linenumber<=0 OR extendedprice IS NULL OR extendedprice<0 OR discount IS NULL OR discount<0 OR discount>1 OR shipdate IS NULL", 'validation:bad_lineitem')['data'][0][0]
checks['customer_normalization_changes'] = query("SELECT count(*) FROM lakehouse.bronze.customer WHERE mktsegment <> upper(trim(mktsegment))", 'validation:normalization')['data'][0][0]
checks['orders_without_customer'] = query("SELECT count(*) FROM lakehouse.bronze.orders o LEFT JOIN lakehouse.bronze.customer c ON o.custkey=c.custkey WHERE c.custkey IS NULL", 'validation:orders_fk')['data'][0][0]
checks['lineitems_without_order'] = query("SELECT count(*) FROM lakehouse.bronze.lineitem l LEFT JOIN lakehouse.bronze.orders o ON l.orderkey=o.orderkey WHERE o.orderkey IS NULL", 'validation:lineitems_fk')['data'][0][0]
for k in ('bad_customer','bad_orders','bad_lineitem','customer_normalization_changes','orders_without_customer','lineitems_without_order'):
    assert checks[k] == 0, (k, checks[k])
checks['silver_bad_rows'] = query("""SELECT count(*) FROM lakehouse.silver.order_lines
WHERE mktsegment <> 'BUILDING' OR orderdate >= DATE '1995-03-15'
OR shipdate <= DATE '1995-03-15' OR net_revenue IS NULL
OR net_revenue <> extendedprice * (DECIMAL '1.00' - discount)""", 'validation:silver')['data'][0][0]
assert checks['silver_bad_rows'] == 0
baseline = json.loads((RESULTS / 'reference_q3.json').read_text(encoding='utf-8-sig'))
bronze_result = query(q3('lakehouse.bronze'), 'validation:bronze_q3')['data']
gold_result = query('SELECT orderkey, revenue, orderdate, shippriority FROM lakehouse.gold.shipping_priority_top10 ORDER BY revenue DESC', 'validation:gold_q3')['data']
checks['bronze_equals_reference'] = same_q3(bronze_result, baseline)
checks['gold_equals_reference'] = same_q3(gold_result, baseline)
assert checks['bronze_equals_reference'] and checks['gold_equals_reference']
save('gold_top10.json', gold_result)
metrics = []
for schema, table in [('bronze','customer'),('bronze','orders'),('bronze','lineitem'),
                      ('silver','order_lines'),('gold','order_revenue'),('gold','shipping_priority_top10')]:
    item = {'schema':schema,'table':table, **table_metrics(schema,table)}
    item['count_star'] = query(f'SELECT count(*) FROM lakehouse.{schema}.{table}', f'count:{schema}.{table}')['data'][0][0]
    assert item['rows'] == item['count_star']
    item['ddl'] = query(f'SHOW CREATE TABLE lakehouse.{schema}.{table}', f'ddl:{schema}.{table}')['data'][0][0]
    metrics.append(item)
save('layer_metrics.json', metrics)
# Timings of the precomputed Silver aggregation and reading Gold are separate from Bronze Q3.
read_timings = {}
for name, sql in [('silver', 'SELECT orderkey, sum(net_revenue) AS revenue, orderdate, shippriority FROM lakehouse.silver.order_lines GROUP BY orderkey,orderdate,shippriority ORDER BY revenue DESC LIMIT 10'),
                  ('gold', 'SELECT * FROM lakehouse.gold.shipping_priority_top10 ORDER BY revenue DESC')]:
    query(sql, f'warmup:{name}')
    read_timings[name] = [query(sql, f'layer_query:{name}:r{r}')['stats']['elapsedTimeMillis']/1000 for r in range(1,6)]
save('layer_query_timings.json', read_timings)
save('validation.json', checks)
print(json.dumps(checks, indent=2), flush=True)
print('PIPELINE COMPLETE', flush=True)
