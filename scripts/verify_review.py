"""Verify the additional evidence and the standalone Markdown artifact."""
import json
import re
import statistics as st
from decimal import Decimal
from trino_client import ROOT,RESULTS

def read(name):
    return json.loads((RESULTS/name).read_text(encoding='utf-8-sig'))
audit={r['id']:r for r in (json.loads(line) for line in (RESULTS/'queries.jsonl').read_text(encoding='utf-8-sig').splitlines()) if 'error' not in r}
seen=[]
for run in ('main','repeat1'):
    b=read(f'benchmark_{run}.json')
    assert len(b['writes'])==72 and len(b['queries'])==80
    for r in b['writes']+b['queries']:
        a=audit[r['query_id']]
        assert r['engine_s']==a['stats']['elapsedTimeMillis']/1000
        seen.append(r['query_id'])
        if 'physical_input_bytes' in r:
            assert r['physical_input_bytes']==a['stats']['physicalInputBytes']
    for r in b['queries']:
        assert r['correct']
e=read('experiments_review1.json')
assert len(e['materialized_writes'])==72
assert len(e['baseline'])==18
assert len(e['partition_queries'])==10
assert len(e['layer_queries'])==20
assert len(e['scale_queries'])==10
for field in ('materialized_writes','baseline','partition_queries','layer_queries','scale_queries'):
    for r in e[field]:
        a=audit[r['query_id']]
        assert r['elapsed_s']==a['stats']['elapsedTimeMillis']/1000
        assert r['physical_bytes']==a['stats']['physicalInputBytes']
        assert r['processed_rows']==a['stats']['processedRows']
        seen.append(r['query_id'])
assert len(seen)==len(set(seen))
assert e['universal_gold_order_count']==11620 and e['universal_gold_differences']==0
assert e['evolution']['before']==e['evolution']['after']
assert e['evolution']['null_count']==30519
assert 'source_name' not in e['final_ddl']['order_lines']
assert e['full_fact_metrics']['rows']==e['monthly_fact_metrics']['rows']==6001215
assert e['order_lines_full_review1_checksum']==e['order_lines_monthly_review1_checksum']
pruning=read('partition_pruning.json')
assert pruning['total_files']==80 and pruning['eligible_files']==5
assert pruning['eligible_rows']==379415
assert st.median(r['processed_rows'] for r in e['partition_queries'] if 'monthly' in r['table'])==pruning['eligible_rows']
storage=read('review_storage_audit.json')
assert storage['verified_tables']==60 and storage['verified_files']==139
assert all(r['all_s3_sizes_match'] for r in storage['tables'])
md=(ROOT/'отчет.md').read_text(encoding='utf-8-sig')
assert '\u2014' not in md and '\u2013' not in md
assert not re.search(r'\]\((?!https?://|#)[^)]+\)',md)
assert not re.search(r'!\[',md)
assert 'Вояга Даниил Максимович' in md and 'БИВТ-24-СП-3' in md
assert len(re.findall(r'^## [1-7]\.',md,re.M))==7
assert md.count('<details>')==md.count('</details>') and md.count('```')%2==0
for code in re.findall(r'```json\n(.*?)\n```',md,re.S):
    json.loads(code)
for code in re.findall(r'```python\n(.*?)\n```',md,re.S):
    import ast
    ast.parse(code)
print('OK: 216 measured writes, 218 analytical/control queries, unique real query IDs')
print('OK: Gold, schema evolution, pruning, S3 objects, embedded JSON and Python')
print('OK: one independent Markdown file, author/group, no long dashes or adjacent-file links')
