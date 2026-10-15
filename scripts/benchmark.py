"""8 combinations, 3 fresh writes, 5 interleaved warm queries per workload."""
import argparse
import csv
import json
import random
import statistics
from decimal import Decimal
from trino_client import ROOT, RESULTS, query, save

PROJECTIONS = {
    'customer': 'custkey, mktsegment',
    'orders': 'orderkey, custkey, orderdate, shippriority',
    'lineitem': 'orderkey, linenumber, extendedprice, discount, shipdate',
}
COMBINATIONS = [(fmt, codec) for fmt in ('PARQUET', 'ORC')
                for codec in ('NONE', 'SNAPPY', 'GZIP', 'ZSTD')]


def q3(prefix, suffix=''):
    text = (ROOT / 'sql/q3.sql').read_text(encoding='utf-8-sig')
    for table in PROJECTIONS:
        text = text.replace(f'tpch.sf1.{table}', f'{prefix}.{table}{suffix}')
    return text


def same_q3(actual, expected):
    if len(actual) != len(expected):
        return False
    for a, b in zip(actual, expected):
        if [a[0], a[2], a[3]] != [b[0], b[2], b[3]]:
            return False
        if abs(Decimal(str(a[1])) - Decimal(str(b[1]))) > Decimal('0.000001'):
            return False
    return True


def table_metrics(schema, table):
    r = query(f'SELECT count(*), sum(record_count), sum(file_size_in_bytes) '
              f'FROM lakehouse.{schema}."{table}$files"', f'metadata:{schema}.{table}')
    files, rows, size = r['data'][0]
    return {'files': files, 'rows': rows, 'bytes': size}


def write_csv(name, rows):
    with (RESULTS / name).open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', default='main')
    args = parser.parse_args()
    if not args.run_id.replace('_', '').isalnum():
        raise ValueError('run-id must contain letters, digits or underscore')
    run = args.run_id.lower()
    if (RESULTS / f'benchmark_{run}.json').exists():
        raise RuntimeError('Completed run already exists. Use a new --run-id.')
    rng = random.Random(20261013)
    baseline = query(q3('tpch.sf1'), 'reference:Q3')['data']
    save('reference_q3.json', baseline)
    aggregate_sql = 'SELECT count(*), sum(extendedprice * (1 - discount)) FROM {}'
    baseline_agg = query(aggregate_sql.format('tpch.sf1.lineitem'), 'reference:aggregate')['data'][0]
    for schema in ('bronze', 'silver', 'gold', 'demo'):
        query(f'CREATE SCHEMA IF NOT EXISTS lakehouse.{schema}', f'schema:{schema}')
    query(f'CREATE SCHEMA lakehouse.warmup_{run}', 'schema:warmup')
    # Excluded warmup for writer implementations and source generation.
    for fmt, codec in COMBINATIONS:
        name = f'w_{fmt.lower()}_{codec.lower()}'
        query(f"CREATE TABLE lakehouse.warmup_{run}.{name} "
              f"WITH (format='{fmt}', compression_codec='{codec}') AS "
              f"SELECT {PROJECTIONS['lineitem']} FROM tpch.tiny.lineitem", f'warmup:write:{name}')
    writes, queries = [], []
    for rep in range(1, 4):
        configs = list(COMBINATIONS)
        rng.shuffle(configs)
        for fmt, codec in configs:
            schema = f'bench_{run}_{fmt.lower()}_{codec.lower()}'
            query(f'CREATE SCHEMA IF NOT EXISTS lakehouse.{schema}', f'schema:{schema}')
            for table, projection in PROJECTIONS.items():
                name = f'{table}_r{rep}'
                sql = f"CREATE TABLE lakehouse.{schema}.{name} "
                sql += f"WITH (format='{fmt}', compression_codec='{codec}') AS "
                sql += f'SELECT {projection} FROM tpch.sf1.{table}'
                result = query(sql, f'write:{fmt}:{codec}:r{rep}:{table}')
                metrics = table_metrics(schema, name)
                row = {'format': fmt, 'codec': codec, 'repeat': rep, 'table': table,
                       'query_id': result['id'], 'wall_s': result['wall_s'],
                       'engine_s': result['stats']['elapsedTimeMillis'] / 1000,
                       'physical_written_bytes':result['stats']['physicalWrittenBytes'], **metrics}
                expected = {'customer': 150000, 'orders': 1500000, 'lineitem': 6001215}[table]
                assert metrics['rows'] == expected, row
                writes.append(row)
                write_csv(f'write_samples_{run}.csv', writes)
                print(f"WRITE {fmt:7} {codec:6} r{rep} {table:8} "
                      f"{row['engine_s']:.3f}s {metrics['bytes']/1e6:.3f}MB", flush=True)
    # Separate warmup, then interleave configs in each of 5 rounds.
    for kind in ('q3', 'aggregate'):
        for rep in range(0, 6):
            configs = list(COMBINATIONS)
            rng.shuffle(configs)
            for fmt, codec in configs:
                prefix = f'lakehouse.bench_{run}_{fmt.lower()}_{codec.lower()}'
                sql = q3(prefix, '_r3') if kind == 'q3' else aggregate_sql.format(prefix + '.lineitem_r3')
                result = query(sql, f'{kind}:{fmt}:{codec}:r{rep}')
                if kind == 'q3':
                    valid = same_q3(result['data'], baseline)
                else:
                    a = result['data'][0]
                    valid = a[0] == baseline_agg[0] and abs(float(a[1])-float(baseline_agg[1])) < 0.05
                assert valid, (kind, fmt, codec, result['data'])
                if rep:
                    queries.append({'workload': kind, 'format': fmt, 'codec': codec,
                                    'repeat': rep, 'query_id': result['id'], 'wall_s': result['wall_s'],
                                    'engine_s': result['stats']['elapsedTimeMillis']/1000,
                                    'cpu_s': result['stats']['cpuTimeMillis']/1000,
                                    'processed_rows': result['stats']['processedRows'],
                                    'processed_bytes': result['stats']['processedBytes'], 'correct': valid})
                    queries[-1]['physical_input_bytes']=result['stats']['physicalInputBytes']
                    queries[-1]['planning_ms']=result['stats']['planningTimeMillis']
                    write_csv(f'query_samples_{run}.csv', queries)
                print(f"{kind.upper():9} {fmt:7} {codec:6} r{rep} "
                      f"{result['stats']['elapsedTimeMillis']/1000:.3f}s OK", flush=True)
    summary = []
    for fmt, codec in COMBINATIONS:
        w = [r for r in writes if r['format'] == fmt and r['codec'] == codec]
        wt = [sum(x['engine_s'] for x in w if x['repeat'] == rep) for rep in range(1, 4)]
        sizes = [sum(x['bytes'] for x in w if x['repeat'] == rep) for rep in range(1, 4)]
        item = {'format': fmt, 'codec': codec, 'write_median_s': statistics.median(wt),
                'write_min_s': min(wt), 'write_max_s': max(wt),
                'bytes_median': statistics.median(sizes), 'bytes_min': min(sizes), 'bytes_max': max(sizes),
                'files_r3': sum(x['files'] for x in w if x['repeat'] == 3)}
        for kind in ('q3', 'aggregate'):
            samples = [x['engine_s'] for x in queries if x['format'] == fmt and x['codec'] == codec
                       and x['workload'] == kind]
            item.update({f'{kind}_median_s': statistics.median(samples),
                         f'{kind}_min_s': min(samples), f'{kind}_max_s': max(samples),
                         f'{kind}_stdev_s': statistics.stdev(samples)})
        summary.append(item)
    write_csv(f'benchmark_summary_{run}.csv', summary)
    save(f'benchmark_{run}.json', {'run_id': run, 'sf': 1, 'seed': 20261013,
                                  'write_repeats': 3, 'query_repeats': 5,
                                  'writes': writes, 'queries': queries, 'summary': summary})
    print('BENCHMARK COMPLETE', flush=True)


if __name__ == '__main__':
    main()
