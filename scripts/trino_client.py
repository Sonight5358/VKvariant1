"""Small synchronous Trino HTTP client; no driver dependency; full query audit."""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / 'results'
RESULTS.mkdir(exist_ok=True)


def query(sql, label='query', audit=True):
    sql = sql.strip().rstrip(';')
    server = os.environ.get('TRINO_URL', 'http://trino:8080')
    headers = {'X-Trino-User': 'voyaga', 'X-Trino-Source': 'lakehouse-q3-lab',
               'Content-Type': 'text/plain; charset=utf-8'}
    request = Request(server + '/v1/statement', data=sql.encode(), headers=headers)
    started = time.perf_counter()
    rows, columns, last, pages = [], [], {}, 0
    while True:
        with urlopen(request, timeout=300) as response:
            last = json.load(response)
        pages += 1
        columns = last.get('columns', columns)
        rows.extend(last.get('data', []))
        if 'error' in last:
            record = {'label': label, 'sql': sql, 'id': last.get('id'),
                      'error': last['error'], 'stats': last.get('stats', {})}
            if audit:
                with (RESULTS / 'queries.jsonl').open('a', encoding='utf-8') as handle:
                    handle.write(json.dumps(record, ensure_ascii=False) + '\n')
            raise RuntimeError(json.dumps(record, ensure_ascii=False))
        next_uri = last.get('nextUri')
        if not next_uri:
            break
        request = Request(next_uri, headers=headers)
    elapsed = time.perf_counter() - started
    record = {'timestamp_utc': datetime.now(timezone.utc).isoformat(),
              'label': label, 'sql': sql, 'id': last.get('id'), 'wall_s': elapsed,
              'stats': last.get('stats', {}), 'columns': columns, 'data': rows,
              'pages': pages}
    if audit:
        with (RESULTS / 'queries.jsonl').open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + '\n')
    return record


def save(name, value):
    (RESULTS / name).write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')


if __name__ == '__main__':
    import sys
    text = Path(sys.argv[1]).read_text(encoding='utf-8') if len(sys.argv) > 1 else 'SELECT version()'
    print(json.dumps(query(text), ensure_ascii=False, indent=2))
