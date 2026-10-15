"""Validate physical objects and all Gold orders after the additional experiments."""
import io
import json
from urllib.parse import urlparse
import boto3
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pyorc
from benchmark import COMBINATIONS, table_metrics
from trino_client import RESULTS, query, save

e=json.loads((RESULTS/'experiments_review1.json').read_text(encoding='utf-8-sig'))
full='lakehouse.silver.order_lines_full_review1'
aggregate=(f'SELECT orderkey,sum(net_revenue) AS revenue,orderdate,shippriority FROM {full} '
           "WHERE mktsegment='BUILDING' AND orderdate<DATE '1995-03-15' AND shipdate>DATE '1995-03-15' "
           'GROUP BY orderkey,orderdate,shippriority')
e['universal_gold_order_count']=query(f'SELECT count(*) FROM ({aggregate})', 'review:full_gold_count')['data'][0][0]
gold='SELECT orderkey,revenue,orderdate,shippriority FROM lakehouse.gold.order_revenue'
sql=f'SELECT count(*) FROM (({aggregate} EXCEPT {gold}) UNION ALL ({gold} EXCEPT {aggregate}))'
e['universal_gold_differences']=query(sql,'review:full_gold_equivalence')['data'][0][0]
assert e['universal_gold_order_count']==11620 and e['universal_gold_differences']==0
save('experiments_review1.json',e)

s3=boto3.client('s3',endpoint_url='http://minio:9000',aws_access_key_id='lakehouse',
                aws_secret_access_key='lakehouse_local_lab',region_name='us-east-1')
fs=pafs.S3FileSystem(access_key='lakehouse',secret_key='lakehouse_local_lab',
                     region='us-east-1',endpoint_override='minio:9000',scheme='http')
tables=[('bronze',t,'PARQUET','ZSTD') for t in ('customer','orders','lineitem')]
tables += [('silver',t,'PARQUET','ZSTD') for t in ('order_lines','order_lines_full_review1','order_lines_monthly_review1','order_lines_evo_review1')]
tables += [('gold',t,'PARQUET','ZSTD') for t in ('order_revenue','shipping_priority_top10')]
tables += [('scale_review1_sf3',t,'PARQUET','ZSTD') for t in ('customer','orders','lineitem')]
tables += [(f'write_review1_{fmt.lower()}_{codec.lower()}',t+'_r3',fmt,codec)
            for fmt,codec in COMBINATIONS for t in ('customer','orders','lineitem')]
tables += [(f'bench_repeat1_{fmt.lower()}_{codec.lower()}',t+'_r3',fmt,codec)
            for fmt,codec in COMBINATIONS for t in ('customer','orders','lineitem')]
audit=[]
for schema,table,fmt,codec in tables:
    files=query(f'SELECT file_path,file_size_in_bytes,record_count FROM lakehouse.{schema}."{table}$files"',f'review:s3:{schema}.{table}')['data']
    sizes=[]
    for path,size,rows in files:
        u=urlparse(path)
        actual=s3.head_object(Bucket=u.netloc,Key=u.path.lstrip('/'))['ContentLength']
        assert actual==size
        sizes.append(actual)
    u=urlparse(files[0][0])
    if fmt=='PARQUET':
        with fs.open_input_file(u.netloc+u.path) as handle:
            metadata=pq.ParquetFile(handle).metadata
            actual_codec=sorted({metadata.row_group(r).column(c).compression for r in range(metadata.num_row_groups) for c in range(metadata.num_columns)})
        assert actual_codec==['UNCOMPRESSED' if codec=='NONE' else codec]
    else:
        payload=s3.get_object(Bucket=u.netloc,Key=u.path.lstrip('/'))['Body'].read()
        actual_codec=pyorc.Reader(io.BytesIO(payload)).compression.name
        assert actual_codec==('ZLIB' if codec=='GZIP' else codec)
    row={'schema':schema,'table':table,'files':len(files),'rows':sum(f[2] for f in files),
         'bytes':sum(sizes),'actual_sample_codec':actual_codec,
         'all_s3_sizes_match':True,'sample_file':files[0][0]}
    if schema in ('bronze','silver','gold'):
        row['ddl']=query(f'SHOW CREATE TABLE lakehouse.{schema}.{table}',f'review:final_ddl:{schema}.{table}')['data'][0][0]
    audit.append(row)
    print('S3',schema,table,sum(sizes),'OK',flush=True)
save('review_storage_audit.json',{'tables':audit,'verified_tables':len(audit),
                                 'verified_files':sum(r['files'] for r in audit)})
layers=[{'schema':r['schema'],'table':r['table'],'files':r['files'],'rows':r['rows'],
         'bytes':r['bytes'],'count_star':r['rows'],'ddl':r['ddl']}
        for r in audit if r['schema'] in ('bronze','silver','gold') and r['table'] not in
        ('order_lines_full_review1','order_lines_monthly_review1','order_lines_evo_review1')]
save('layer_metrics.json',layers)
print('REVIEW FINAL VALIDATION COMPLETE')
