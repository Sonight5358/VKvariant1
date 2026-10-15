"""Independent engine reads Bronze via Iceberg REST catalog and S3."""
import json
from datetime import date
from decimal import Decimal
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from trino_client import ROOT, RESULTS, save
from benchmark import q3, same_q3

connection = duckdb.connect()
connection.execute("SET threads=2")
connection.execute("SET memory_limit='2GB'")
catalog = load_catalog('lakehouse', **{
    'type':'rest', 'uri':'http://lakekeeper:8181/catalog', 'warehouse':'lakehouse',
    's3.endpoint':'http://minio:9000', 's3.access-key-id':'lakehouse',
    's3.secret-access-key':'lakehouse_local_lab', 's3.region':'us-east-1'})
# PyIceberg resolves the current snapshot in REST and reads Parquet from S3.
# DuckDB queries the resulting Arrow batches without a second on-disk copy.
arrow_tables = {}
for table in ('customer','orders','lineitem'):
    iceberg_table = catalog.load_table(('bronze',table))
    # Use Arrow's S3 reader explicitly; no remote signer or optional s3fs dependency.
    iceberg_table.io = PyArrowFileIO(properties={
        's3.endpoint':'http://minio:9000','s3.access-key-id':'lakehouse',
        's3.secret-access-key':'lakehouse_local_lab','s3.region':'us-east-1'})
    arrow_tables[table] = iceberg_table.scan().to_arrow()
    connection.register(table, arrow_tables[table])


def json_rows(rows):
    return [[str(v) if isinstance(v, (date, Decimal)) else v for v in row] for row in rows]


demo = {'duckdb_version': duckdb.__version__,
        'connection':'Lakekeeper REST -> PyIceberg -> S3 Parquet -> Arrow -> DuckDB',
        'extension_note':'Native extension repository returned HTTP 403; the supported PyIceberg reader is used.',
        'metadata_locations': {t:catalog.load_table(('bronze',t)).metadata_location for t in arrow_tables}}
for table in ('customer', 'orders', 'lineitem'):
    demo[table + '_count'] = connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
    demo[table + '_sample'] = json_rows(connection.execute(f'SELECT * FROM {table} LIMIT 5').fetchall())
demo['segments'] = connection.execute("SELECT mktsegment, count(*) FROM customer GROUP BY mktsegment ORDER BY mktsegment").fetchall()
demo['q3'] = json_rows(connection.execute(q3('lakehouse.bronze').replace('lakehouse.bronze.','')).fetchall())
baseline = json.loads((RESULTS / 'reference_q3.json').read_text(encoding='utf-8-sig'))
demo['q3_matches_trino'] = same_q3(demo['q3'], baseline)
assert demo['q3_matches_trino']
assert [demo[t+'_count'] for t in ('customer','orders','lineitem')] == [150000,1500000,6001215]
save('duckdb_demo.json', demo)
print(json.dumps(demo, ensure_ascii=False, indent=2), flush=True)
