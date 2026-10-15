"""Inspect actual Parquet bounds to explain pruning of the monthly fact."""
from datetime import date
from urllib.parse import urlparse
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from trino_client import query,save

fs=pafs.S3FileSystem(access_key='lakehouse',secret_key='lakehouse_local_lab',
                     region='us-east-1',endpoint_override='minio:9000',scheme='http')
rows=query('SELECT file_path,record_count,file_size_in_bytes FROM lakehouse.silver."order_lines_monthly_review1$files"',
           'review:pruning:files')['data']
files=[]
for path,row_count,size in rows:
    u=urlparse(path)
    with fs.open_input_file(u.netloc+u.path) as handle:
        meta=pq.ParquetFile(handle).metadata
        bounds={}
        for name in ('orderdate','shipdate'):
            mins,maxs=[],[]
            for rg in range(meta.num_row_groups):
                for c in range(meta.num_columns):
                    col=meta.row_group(rg).column(c)
                    if col.path_in_schema==name:
                        assert col.statistics and col.statistics.has_min_max
                        mins.append(col.statistics.min)
                        maxs.append(col.statistics.max)
            bounds[name+'_min']=min(mins)
            bounds[name+'_max']=max(maxs)
        eligible=bounds['orderdate_min']<date(1995,3,15) and bounds['shipdate_max']>date(1995,3,15)
        files.append({'path':path,'rows':row_count,'bytes':size,
                      **{k:str(v) for k,v in bounds.items()},'eligible_by_dates':eligible})
save('partition_pruning.json',{'total_files':len(files),
     'eligible_files':sum(f['eligible_by_dates'] for f in files),
     'eligible_rows':sum(f['rows'] for f in files if f['eligible_by_dates']),
     'files':sorted(files,key=lambda f:f['orderdate_min'])})
print('Eligible date ranges:',[(f['orderdate_min'],f['orderdate_max'],f['rows']) for f in files if f['eligible_by_dates']])
