"""Idempotent bootstrap for this local lab only."""
import json
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import boto3

s3 = boto3.client('s3', endpoint_url='http://minio:9000',
                  aws_access_key_id='lakehouse', aws_secret_access_key='lakehouse_local_lab',
                  region_name='us-east-1')
if not any(b['Name'] == 'lakehouse' for b in s3.list_buckets()['Buckets']):
    s3.create_bucket(Bucket='lakehouse')


def request(path, data=None):
    req = Request('http://lakekeeper:8181/management/v1/' + path,
                  data=json.dumps(data).encode() if data is not None else None,
                  headers={'Content-Type': 'application/json'})
    try:
        with urlopen(req, timeout=30) as response:
            body = response.read().decode()
            print(path, response.status, body)
            return json.loads(body) if body else {}
    except HTTPError as error:
        if path == 'bootstrap' and error.code in (400, 409):
            print('Already bootstrapped:', error.read().decode())
            return {}
        raise


request('bootstrap', {'accept-terms-of-use': True})
warehouses = request('warehouse?project-id=00000000-0000-0000-0000-000000000000')
if not any(w.get('warehouse-name', w.get('name')) == 'lakehouse' for w in warehouses.get('warehouses', [])):
    request('warehouse', {
        'warehouse-name': 'lakehouse',
        'project-id': '00000000-0000-0000-0000-000000000000',
        'storage-profile': {'type': 's3', 'bucket': 'lakehouse', 'key-prefix': 'warehouse',
                            'endpoint': 'http://minio:9000', 'region': 'us-east-1',
                            'path-style-access': True, 'flavor': 's3-compat', 'sts-enabled': False},
        'storage-credential': {'type': 's3', 'credential-type': 'access-key',
                               'access-key-id': 'lakehouse', 'secret-access-key': 'lakehouse_local_lab'}})
print('Bootstrap complete')
