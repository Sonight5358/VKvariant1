"""Validate the evidence that supports the report, without rerunning benchmarks."""
import json
import argparse
import statistics
from decimal import Decimal
from trino_client import ROOT, RESULTS

def read(name):
    return json.loads((RESULTS/name).read_text(encoding='utf-8-sig'))

parser = argparse.ArgumentParser()
parser.add_argument('--run-id',default='main')
args = parser.parse_args()
b = read(f'benchmark_{args.run_id}.json')
assert len(b['writes']) == 8*3*3
assert len(b['queries']) == 8*5*2
assert all(q['correct'] for q in b['queries'])
ids = [r['query_id'] for r in b['writes']+b['queries']]
assert len(set(ids)) == len(ids)
audit = [json.loads(line) for line in (RESULTS/'queries.jsonl').read_text(encoding='utf-8-sig').splitlines()]
by_id = {r['id']:r for r in audit if 'error' not in r}
for sample in b['writes']+b['queries']:
    assert sample['query_id'] in by_id
    assert sample['engine_s'] == by_id[sample['query_id']]['stats']['elapsedTimeMillis']/1000
for summary in b['summary']:
    w = [x for x in b['writes'] if x['format']==summary['format'] and x['codec']==summary['codec']]
    expected = statistics.median([sum(x['bytes'] for x in w if x['repeat']==rep) for rep in (1,2,3)])
    assert summary['bytes_median'] == expected
    expected_write = statistics.median([sum(x['engine_s'] for x in w if x['repeat']==rep) for rep in (1,2,3)])
    assert summary['write_median_s'] == expected_write
    for kind in ('q3','aggregate'):
        q = [x['engine_s'] for x in b['queries'] if x['format']==summary['format'] and x['codec']==summary['codec'] and x['workload']==kind]
        assert summary[kind+'_median_s'] == statistics.median(q)
validation = read('validation.json')
assert validation['gold_equals_reference'] and validation['bronze_equals_reference']
assert all(v == 0 for k,v in validation.items() if k not in ('bronze_counts','gold_equals_reference','bronze_equals_reference'))
storage = read('storage_audit.json')
assert len(storage['tables']) == 30
assert all(t['metadata_bytes']==t['s3_data_bytes'] and t['all_s3_file_sizes_match'] for t in storage['tables'])
assert read('duckdb_demo.json')['q3_matches_trino']
assert read('demos.json')['snapshot']['all_values_restored']
print(f'OK: {len(b["writes"])} writes, {len(b["queries"])} measured queries, {len(storage["tables"])} storage checks')
for md in ROOT.glob('*.md'):
    text = md.read_text(encoding='utf-8')
    assert '\u2014' not in text and '\u2013' not in text, f'Long dash found in {md.name}'
print('OK: no long dashes in Markdown deliverables')
