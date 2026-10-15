"""Actual Silver schema evolution and snapshot restore on a disposable demo table."""
from trino_client import ROOT, query, save

query("CREATE TABLE IF NOT EXISTS lakehouse.silver.order_lines_evo WITH(format='PARQUET',compression_codec='ZSTD') AS SELECT * FROM lakehouse.silver.order_lines",'schema:create_copy')
columns = query('SHOW COLUMNS FROM lakehouse.silver.order_lines_evo', 'schema:precheck')['data']
if any(c[0] == 'source_name' for c in columns):
    print('Schema demo already performed. Existing results/demos.json is retained.')
    raise SystemExit(0)

demo = {}
before = query('SELECT count(*), sum(net_revenue) FROM lakehouse.silver.order_lines_evo', 'schema:before')['data']
demo['files_before']=query('SELECT file_path,file_size_in_bytes FROM lakehouse.silver."order_lines_evo$files"','schema:files_before')['data']
demo['snapshots_before']=query('SELECT snapshot_id FROM lakehouse.silver."order_lines_evo$snapshots"','schema:snapshots_before')['data']
query('ALTER TABLE lakehouse.silver.order_lines_evo ADD COLUMN source_system VARCHAR', 'schema:add')
demo['new_column_null_rows'] = query('SELECT count(*) FROM lakehouse.silver.order_lines_evo WHERE source_system IS NULL', 'schema:nulls')['data'][0][0]
query('ALTER TABLE lakehouse.silver.order_lines_evo RENAME COLUMN source_system TO source_name', 'schema:rename')
after = query('SELECT count(*), sum(net_revenue) FROM lakehouse.silver.order_lines_evo', 'schema:after')['data']
demo['files_after']=query('SELECT file_path,file_size_in_bytes FROM lakehouse.silver."order_lines_evo$files"','schema:files_after')['data']
demo['snapshots_after']=query('SELECT snapshot_id FROM lakehouse.silver."order_lines_evo$snapshots"','schema:snapshots_after')['data']
assert demo['files_before']==demo['files_after'] and demo['snapshots_before']==demo['snapshots_after']
demo['before'] = before
demo['after'] = after
demo['data_unchanged'] = before == after
assert demo['data_unchanged']
demo['silver_columns'] = query('SHOW COLUMNS FROM lakehouse.silver.order_lines_evo', 'schema:columns')['data']

query("CREATE TABLE IF NOT EXISTS lakehouse.demo.snapshot_demo WITH(format='PARQUET',compression_codec='ZSTD') AS SELECT * FROM lakehouse.silver.order_lines ORDER BY orderkey,linenumber LIMIT 100", 'snapshot:create')
snapshot = query("SELECT snapshot_id FROM lakehouse.demo.\"snapshot_demo$refs\" WHERE name='main'", 'snapshot:id')['data'][0][0]
before_rows = query('SELECT orderkey, linenumber, net_revenue FROM lakehouse.demo.snapshot_demo ORDER BY orderkey,linenumber', 'snapshot:before')['data']
query('DELETE FROM lakehouse.demo.snapshot_demo WHERE orderkey = (SELECT min(orderkey) FROM lakehouse.demo.snapshot_demo)', 'snapshot:delete')
after_delete = query('SELECT count(*) FROM lakehouse.demo.snapshot_demo', 'snapshot:after_delete')['data'][0][0]
historical = query(f'SELECT orderkey,linenumber,net_revenue FROM lakehouse.demo.snapshot_demo FOR VERSION AS OF {snapshot} ORDER BY orderkey,linenumber', 'snapshot:historical')['data']
restore_sql = f'ALTER TABLE lakehouse.demo.snapshot_demo EXECUTE rollback_to_snapshot({snapshot})'
query(restore_sql, 'snapshot:restore')
restored = query('SELECT orderkey,linenumber,net_revenue FROM lakehouse.demo.snapshot_demo ORDER BY orderkey,linenumber', 'snapshot:restored')['data']
assert after_delete < 100 and restored == before_rows and historical == before_rows
demo['snapshot'] = {'id':snapshot, 'before_rows':len(before_rows), 'after_delete_rows':after_delete,
                    'historical_rows':len(historical), 'restored_rows':len(restored),
                    'all_values_restored':restored == before_rows,
                    'history':query('SELECT committed_at,snapshot_id,parent_id,operation FROM lakehouse.demo."snapshot_demo$snapshots" ORDER BY committed_at', 'snapshot:history')['data']}
(ROOT / 'sql/restore_snapshot.sql').write_text(f'-- Snapshot actually tested during this run.\nSELECT count(*) FROM lakehouse.demo.snapshot_demo FOR VERSION AS OF {snapshot};\n{restore_sql};\nSELECT count(*) FROM lakehouse.demo.snapshot_demo;\n', encoding='utf-8')
save('demos.json', demo)
print(demo, flush=True)
