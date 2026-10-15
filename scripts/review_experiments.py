"""Additional measurements requested by the report review, run sequentially."""
import argparse
import json
import random
import statistics
from decimal import Decimal
from benchmark import COMBINATIONS, PROJECTIONS, q3, same_q3, table_metrics
from trino_client import ROOT, RESULTS, query, save


def stats(record, **extra):
    s = record['stats']
    return {'query_id':record['id'], 'elapsed_s':s['elapsedTimeMillis']/1000,
            'wall_s':record['wall_s'], 'physical_bytes':s['physicalInputBytes'],
            'processed_rows':s['processedRows'], 'planning_ms':s['planningTimeMillis'],
            'analysis_ms':s['analysisTimeMillis'], 'cpu_s':s['cpuTimeMillis']/1000,
            'peak_memory_bytes':s['peakMemoryBytes'], 'spilled_bytes':s['spilledBytes'], **extra}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--run-id',default='review1')
    args=parser.parse_args()
    if not args.run_id.replace('_','').isalnum():
        raise ValueError('Invalid run-id')
    run=args.run_id.lower()
    output_name=f'experiments_{run}.json'
    if (RESULTS/output_name).exists():
        raise RuntimeError('Completed experiment already exists. Use a new run-id.')
    rng=random.Random(20261014)
    output={'run_id':run,'seed':20261014,'baseline':[],'materialized_writes':[],
            'partition_queries':[],'layer_queries':[],'scale_queries':[]}
    def checkpoint():
        save(f'experiments_{run}_progress.json',output)
    reference=json.loads((RESULTS/'reference_q3.json').read_text(encoding='utf-8-sig'))

    # A checksum baseline forces the same projected columns to be generated/read.
    # It is an independent workload, so its elapsed time is never subtracted from CTAS.
    checksum_reference={}
    for repeat in range(0,4):
        tasks=[(prefix,table) for prefix in ('tpch.sf1','lakehouse.bronze') for table in PROJECTIONS]
        rng.shuffle(tasks)
        for prefix,table in tasks:
            sql=f'SELECT count(*), to_hex(checksum(ROW({PROJECTIONS[table]}))) FROM {prefix}.{table}'
            r=query(sql,f'review:{run}:baseline:{prefix}:{table}:r{repeat}')
            row_count,digest=r['data'][0]
            if table not in checksum_reference:
                checksum_reference[table]=[row_count,digest]
            assert checksum_reference[table]==[row_count,digest]
            if repeat:
                output['baseline'].append(stats(r,source=prefix,table=table,repeat=repeat,
                                                rows=row_count,checksum=digest))
            print('BASELINE',prefix,table,repeat,r['stats']['elapsedTimeMillis']/1000,flush=True)
    checkpoint()

    # Compare writes with no TPC-H generator in the measured CTAS plan.
    for repeat in range(1,4):
        configs=list(COMBINATIONS)
        rng.shuffle(configs)
        for fmt,codec in configs:
            schema=f'write_{run}_{fmt.lower()}_{codec.lower()}'
            query(f'CREATE SCHEMA IF NOT EXISTS lakehouse.{schema}',f'review:{run}:schema')
            for table,projection in PROJECTIONS.items():
                name=f'{table}_r{repeat}'
                sql=(f"CREATE TABLE lakehouse.{schema}.{name} WITH(format='{fmt}',compression_codec='{codec}') "
                     f'SELECT {projection} FROM lakehouse.bronze.{table}')
                # Trino CTAS requires AS before SELECT.
                sql=sql.replace(') SELECT',') AS SELECT')
                r=query(sql,f'review:{run}:stored_write:{fmt}:{codec}:{table}:r{repeat}')
                metrics=table_metrics(schema,name)
                assert metrics['rows']==checksum_reference[table][0]
                output['materialized_writes'].append(stats(r,format=fmt,codec=codec,
                                                           repeat=repeat,table=table,**metrics))
                print('STORED WRITE',fmt,codec,table,repeat,r['stats']['elapsedTimeMillis']/1000,flush=True)
                checkpoint()

    # Keep the variant's task-specific Silver; remove only the previous all-NULL demo column.
    cols=query('SHOW COLUMNS FROM lakehouse.silver.order_lines',f'review:{run}:silver_columns')['data']
    if any(c[0]=='source_name' for c in cols):
        non_null=query('SELECT count(*) FROM lakehouse.silver.order_lines WHERE source_name IS NOT NULL',
                       f'review:{run}:source_name_nullcheck')['data'][0][0]
        assert non_null==0
        query('ALTER TABLE lakehouse.silver.order_lines DROP COLUMN source_name',f'review:{run}:remove_demo_column')

    full=f'order_lines_full_{run}'
    monthly=f'order_lines_monthly_{run}'
    medallion=(ROOT/'sql/medallion.sql').read_text(encoding='utf-8-sig')
    silver_statement=[s for s in medallion.split(';') if 'CREATE TABLE' in s and 'lakehouse.silver.order_lines' in s][0]
    silver_statement=silver_statement.replace('IF NOT EXISTS ','').replace('lakehouse.silver.order_lines',f'lakehouse.silver.{full}')
    silver_statement=silver_statement.split("WHERE c.mktsegment='BUILDING'")[0].strip()
    output['full_fact_sql']=silver_statement
    query(silver_statement,f'review:{run}:create_full_fact')
    monthly_sql=(f"CREATE TABLE lakehouse.silver.{monthly} WITH(format='PARQUET',compression_codec='ZSTD',"
                 f"partitioning=ARRAY['month(orderdate)']) AS SELECT * FROM lakehouse.silver.{full}")
    output['monthly_sql']=monthly_sql
    query(monthly_sql,f'review:{run}:create_monthly_fact')
    output['full_fact_metrics']=table_metrics('silver',full)
    output['monthly_fact_metrics']=table_metrics('silver',monthly)
    for name in (full,monthly):
        r=query(f'SELECT count(*),to_hex(checksum(ROW(orderkey,linenumber,custkey,orderdate,shippriority,shipdate,mktsegment,extendedprice,discount,net_revenue,shipment_lag_days))) FROM lakehouse.silver.{name}',
                f'review:{run}:fact_equivalence:{name}')
        output[name+'_checksum']=r['data'][0]
    assert output[full+'_checksum']==output[monthly+'_checksum']
    assert output['full_fact_metrics']['rows']==6001215
    output['fact_validation']=query(f"""SELECT count(*) FROM lakehouse.silver.{full}
      WHERE net_revenue IS NULL OR orderkey<=0 OR linenumber<=0
      OR extendedprice<0 OR discount<0 OR discount>1
      OR mktsegment<>upper(trim(mktsegment))
      OR net_revenue<>extendedprice*(DECIMAL '1.00'-discount)""",f'review:{run}:fact_quality')['data'][0][0]
    assert output['fact_validation']==0
    fact_q3=lambda name:(f'SELECT orderkey,sum(net_revenue) AS revenue,orderdate,shippriority FROM lakehouse.silver.{name} '
                         "WHERE mktsegment='BUILDING' AND orderdate<DATE '1995-03-15' AND shipdate>DATE '1995-03-15' "
                         'GROUP BY orderkey,orderdate,shippriority ORDER BY revenue DESC LIMIT 10')
    for repeat in range(0,6):
        names=[full,monthly]
        rng.shuffle(names)
        for name in names:
            r=query(fact_q3(name),f'review:{run}:partition:{name}:r{repeat}')
            assert same_q3(r['data'],reference)
            if repeat:
                output['partition_queries'].append(stats(r,table=name,repeat=repeat,correct=True))
            print('PARTITION',name,repeat,r['stats']['elapsedTimeMillis']/1000,flush=True)
    output['partition_plans']={name:query('EXPLAIN (TYPE DISTRIBUTED) '+fact_q3(name),
                                         f'review:{run}:plan:{name}')['data'][0][0] for name in (full,monthly)}
    output['partition_file_distribution']=query(f'SELECT count(*),min(file_size_in_bytes),max(file_size_in_bytes),avg(file_size_in_bytes) FROM lakehouse.silver."{monthly}$files"',
                                                f'review:{run}:partition_file_sizes')['data'][0]
    checkpoint()

    # Same session, interleaved rounds; all produce the same top 10.
    layer_sql={
      'Bronze':q3('lakehouse.bronze'),
      'Silver Q3':'SELECT orderkey,sum(net_revenue) AS revenue,orderdate,shippriority FROM lakehouse.silver.order_lines GROUP BY orderkey,orderdate,shippriority ORDER BY revenue DESC LIMIT 10',
      'Silver full':fact_q3(full),
      'Gold':'SELECT orderkey,revenue,orderdate,shippriority FROM lakehouse.gold.shipping_priority_top10 ORDER BY revenue DESC'}
    output['layer_sql']=layer_sql
    for repeat in range(0,6):
        names=list(layer_sql)
        rng.shuffle(names)
        for name in names:
            r=query(layer_sql[name],f'review:{run}:layer:{name}:r{repeat}')
            assert same_q3(r['data'],reference)
            if repeat:
                output['layer_queries'].append(stats(r,layer=name,repeat=repeat,correct=True))
            print('LAYER',name,repeat,r['stats']['elapsedTimeMillis']/1000,flush=True)
    checkpoint()

    # Schema evolution on a full copy of the 30 519-row Silver, with physical proof.
    evo=f'order_lines_evo_{run}'
    query(f"CREATE TABLE lakehouse.silver.{evo} WITH(format='PARQUET',compression_codec='ZSTD') AS SELECT * FROM lakehouse.silver.order_lines",f'review:{run}:evo_create')
    def evo_state(label):
        files=query(f'SELECT file_path,file_size_in_bytes,record_count FROM lakehouse.silver."{evo}$files" ORDER BY file_path',label+':files')['data']
        snapshots=query(f'SELECT snapshot_id,operation FROM lakehouse.silver."{evo}$snapshots" ORDER BY committed_at',label+':snapshots')['data']
        values=query(f'SELECT count(*),sum(net_revenue) FROM lakehouse.silver.{evo}',label+':values')['data']
        return {'files':files,'snapshots':snapshots,'values':values}
    evolution={'table':evo,'before':evo_state(f'review:{run}:evo_before')}
    query(f'ALTER TABLE lakehouse.silver.{evo} ADD COLUMN source_system VARCHAR',f'review:{run}:evo_add')
    query(f'ALTER TABLE lakehouse.silver.{evo} RENAME COLUMN source_system TO source_name',f'review:{run}:evo_rename')
    evolution['after']=evo_state(f'review:{run}:evo_after')
    evolution['null_count']=query(f'SELECT count(*) FROM lakehouse.silver.{evo} WHERE source_name IS NULL',f'review:{run}:evo_nulls')['data'][0][0]
    assert evolution['before']==evolution['after']
    output['evolution']=evolution
    checkpoint()

    # Additional scale only: canonical Bronze/Silver/Gold remain at sf1.
    scale_schema=f'scale_{run}_sf3'
    query(f'CREATE SCHEMA lakehouse.{scale_schema}',f'review:{run}:scale_schema')
    for table,projection in PROJECTIONS.items():
        query(f"CREATE TABLE lakehouse.{scale_schema}.{table} WITH(format='PARQUET',compression_codec='ZSTD') AS SELECT {projection} FROM tpch.sf3.{table}",f'review:{run}:scale_create:{table}')
    output['sf3_metrics']={table:table_metrics(scale_schema,table) for table in PROJECTIONS}
    reference3=query(q3('tpch.sf3'),f'review:{run}:reference_sf3')['data']
    for repeat in range(0,6):
        scales=[1,3]
        rng.shuffle(scales)
        for sf in scales:
            sql=q3('lakehouse.bronze' if sf==1 else 'lakehouse.'+scale_schema)
            r=query(sql,f'review:{run}:scale_q3:sf{sf}:r{repeat}')
            assert same_q3(r['data'],reference if sf==1 else reference3)
            if repeat:
                output['scale_queries'].append(stats(r,sf=sf,repeat=repeat,correct=True))
            print('SCALE',sf,repeat,r['stats']['elapsedTimeMillis']/1000,flush=True)
    output['gold_unchanged']=same_q3(query(layer_sql['Gold'],f'review:{run}:final_gold')['data'],reference)
    assert output['gold_unchanged']
    # Keep complete DDL for additional facts and the clean primary Silver.
    output['final_ddl']={name:query(f'SHOW CREATE TABLE lakehouse.silver.{name}',f'review:{run}:ddl:{name}')['data'][0][0]
                         for name in ('order_lines',full,monthly,evo)}
    save(output_name,output)
    print('REVIEW EXPERIMENTS COMPLETE',flush=True)


if __name__=='__main__':
    main()
