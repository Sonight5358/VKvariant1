"""Recompute two-session comparisons and empirical variability from actual query IDs."""
import json
import statistics
from benchmark import COMBINATIONS, write_csv
from trino_client import RESULTS, save

records=[json.loads(line) for line in (RESULTS/'queries.jsonl').read_text(encoding='utf-8-sig').splitlines()]
by_id={r['id']:r for r in records if 'error' not in r}
sessions={}
for run in ('main','repeat1'):
    b=json.loads((RESULTS/f'benchmark_{run}.json').read_text(encoding='utf-8-sig'))
    for q in b['queries']:
        s=by_id[q['query_id']]['stats']
        q['physical_input_bytes']=s['physicalInputBytes']
        q['planning_ms']=s['planningTimeMillis']
        q['analysis_ms']=s['analysisTimeMillis']
    for w in b['writes']:
        w['physical_written_bytes']=by_id[w['query_id']]['stats']['physicalWrittenBytes']
    for item in b['summary']:
        for kind in ('q3','aggregate'):
            samples=[q for q in b['queries'] if q['workload']==kind and q['format']==item['format'] and q['codec']==item['codec']]
            item[kind+'_physical_mb']=statistics.median(q['physical_input_bytes'] for q in samples)/1e6
            item[kind+'_planning_ms']=statistics.median(q['planning_ms'] for q in samples)
    save(f'benchmark_{run}.json',b)
    write_csv(f'query_samples_{run}.csv',b['queries'])
    write_csv(f'write_samples_{run}.csv',b['writes'])
    write_csv(f'benchmark_summary_{run}.csv',b['summary'])
    sessions[run]=b

noise=[]
for metric in ('write','q3','aggregate'):
    median_differences=[]
    within_ranges=[]
    for fmt,codec in COMBINATIONS:
        samples=[]
        for b in sessions.values():
            if metric=='write':
                w=[r for r in b['writes'] if r['format']==fmt and r['codec']==codec]
                values=[sum(r['engine_s'] for r in w if r['repeat']==rep) for rep in (1,2,3)]
            else:
                values=[r['engine_s'] for r in b['queries'] if r['format']==fmt and r['codec']==codec and r['workload']==metric]
            samples.append(values)
            within_ranges.append(max(values)-min(values))
        median_differences.append(abs(statistics.median(samples[0])-statistics.median(samples[1])))
    noise.append({'metric':metric,'max_session_median_delta_s':max(median_differences),
                  'max_within_group_range_s':max(within_ranges),
                  'empirical_threshold_s':max(max(median_differences),max(within_ranges),0.001)})
save('review_summary.json',{'noise':noise})
print(json.dumps(noise,indent=2))
