"""Capture final schemas and a readable Gold export after schema evolution."""
import csv
import json
from benchmark import q3
from trino_client import ROOT, RESULTS, query, save

layers = json.loads((RESULTS / 'layer_metrics.json').read_text(encoding='utf-8-sig'))
for item in layers:
    item['ddl'] = query(f"SHOW CREATE TABLE lakehouse.{item['schema']}.{item['table']}",
                        f"final_ddl:{item['schema']}.{item['table']}")['data'][0][0]
save('layer_metrics.json', layers)
gold = json.loads((RESULTS / 'gold_top10.json').read_text(encoding='utf-8-sig'))
with (RESULTS / 'gold_top10.csv').open('w',newline='',encoding='utf-8') as handle:
    writer = csv.writer(handle)
    writer.writerow(['orderkey','revenue','orderdate','shippriority'])
    writer.writerows(gold)
(ROOT / 'sql/q3_bronze.sql').write_text(q3('lakehouse.bronze'), encoding='utf-8')
plan = query('EXPLAIN (TYPE DISTRIBUTED) ' + q3('lakehouse.bronze').strip().rstrip(';'), 'explain:bronze_q3')['data'][0][0]
(RESULTS / 'q3_plan.txt').write_text(plan, encoding='utf-8')
print('Final evidence saved')
