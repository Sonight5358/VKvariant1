"""Verify $files byte counts against actual S3 HEADs and physical file codecs."""
import io
import argparse
import json
from urllib.parse import urlparse
import boto3
import pyarrow.parquet as pq
import pyorc
from trino_client import RESULTS, query, save
from benchmark import COMBINATIONS

s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='lakehouse',
                  aws_secret_access_key='lakehouse_local_lab', region_name='us-east-1')
audit = []
parser = argparse.ArgumentParser()
parser.add_argument('--run-id', default='main')
args = parser.parse_args()
if not args.run_id.replace('_','').isalnum():
    raise ValueError('Invalid run-id')
tables = [(f'bench_{args.run_id.lower()}_{fmt.lower()}_{codec.lower()}', f'{table}_r3', fmt, codec)
          for fmt, codec in COMBINATIONS for table in ('customer','orders','lineitem')]
tables += [(schema, table, 'PARQUET', 'ZSTD') for schema, table in
           [('bronze','customer'),('bronze','orders'),('bronze','lineitem'),
            ('silver','order_lines'),('gold','order_revenue'),('gold','shipping_priority_top10')]]
for schema, table, fmt, codec in tables:
    files = query(f'SELECT file_path,file_size_in_bytes,record_count,file_format FROM lakehouse.{schema}."{table}$files"', f'storage:{schema}.{table}')['data']
    item = {'schema':schema,'table':table,'format':fmt,'requested_codec':codec,
            'file_count':len(files),'metadata_bytes':sum(f[1] for f in files),
            's3_data_bytes':0,'all_s3_file_sizes_match':True,'files':[]}
    for path, size, rows, file_format in files:
        u = urlparse(path)
        head = s3.head_object(Bucket=u.netloc, Key=u.path.lstrip('/'))
        item['s3_data_bytes'] += head['ContentLength']
        item['all_s3_file_sizes_match'] &= size == head['ContentLength']
        item['files'].append({'path':path,'bytes':size,'rows':rows})
        assert file_format == fmt
    assert item['all_s3_file_sizes_match'], item
    # Read one actual file per table and inspect compression metadata.
    u = urlparse(files[0][0])
    payload = s3.get_object(Bucket=u.netloc,Key=u.path.lstrip('/'))['Body'].read()
    if fmt == 'PARQUET':
        metadata = pq.ParquetFile(io.BytesIO(payload)).metadata
        actual = sorted({metadata.row_group(r).column(c).compression
                         for r in range(metadata.num_row_groups) for c in range(metadata.num_columns)})
        expected = 'UNCOMPRESSED' if codec == 'NONE' else codec
        assert actual == [expected], (item, actual)
        item['sample_file_codec'] = actual
    else:
        reader = pyorc.Reader(io.BytesIO(payload))
        actual = reader.compression.name
        expected = 'ZLIB' if codec == 'GZIP' else codec
        assert actual == expected, (item, actual)
        item['sample_file_codec'] = actual
    prefix = u.path.lstrip('/').split('/data/')[0] + '/'
    objects = [o for page in s3.get_paginator('list_objects_v2').paginate(Bucket=u.netloc,Prefix=prefix)
               for o in page.get('Contents',[])]
    item['table_directory_all_bytes'] = sum(o['Size'] for o in objects)
    audit.append(item)
    print(schema, table, item['s3_data_bytes'], item['sample_file_codec'], 'OK', flush=True)
inventory = [o for page in s3.get_paginator('list_objects_v2').paginate(Bucket='lakehouse')
             for o in page.get('Contents',[])]
save('storage_audit.json', {'tables':audit,'bucket_object_count':len(inventory),
                           'bucket_total_bytes':sum(o['Size'] for o in inventory)})
