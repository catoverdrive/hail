import hailtop.batch as hb
import timeit
import requests
import json
import os
import google.cloud.storage as gcs
import google.oauth2.service_account as gsa

import matplotlib.pyplot as plt

def plot_timings(output, **timings):
    fig = plt.figure(figsize=(6, 3 * len(timings)))
    for i, key in enumerate(timings.keys()):
        ax = plt.subplot(len(timings), 1, i + 1)        
        _, _, hist = ax.hist(timings[key])
        plt.title(f'{key} (n={len(timings[key])}, avg={sum(timings[key])/len(timings[key]):.4f} s)')
    fig.tight_layout()
    fig.savefig(output)

def get_client(key):
    credentials = gsa.Credentials.from_service_account_info(key)
    return gcs.Client(project='hail-vdc', credentials=credentials)

def get_etag(client, bucket, path):
    b = client.bucket(bucket).blob(path)
    b.reload()
    return b.etag

def test_request(url, n, params=None):
    return timeit.repeat(lambda: requests.get(url, params=params), repeat=n, number=1)

def test_gcs(client, bucket, path, n):
    return timeit.repeat(lambda: client.bucket(bucket).blob(path).download_as_string(), repeat=n, number=1)

def test_cluster(key, etag, n):
    res = {
        'getf_memory': lambda: test_request('http://internal.hail/wang/memory/api/v1alpha/objects', n, params={'q': 'gs://amwang/test/f', 'etag': etag}),
        'internal': lambda: test_request('http://internal.hail/wang/memory/healthcheck', n),
        'google': lambda: test_request('http://google.com', n),
        'getf_gcs': lambda: test_gcs(get_client(key), 'amwang', 'test/f', n),
        }
    return res

output = 'cluster_test2'
with open("/Users/wang/.hail/gcs-keys/wang-gsa-key.json") as f:
    key = json.loads(f.read())
client = get_client(key)
etag = get_etag(client, 'amwang', 'test/f')

print(test_request('http://google.com', 10))  
print(test_gcs(client, 'amwang', 'test/f', 10))    

if not os.path.exists(f'{output}.json'):
    with hb.BatchPoolExecutor(project='hail-vdc', image='gcr.io/hail-vdc/memory:latest') as bpe:
        tests = test_cluster(key, etag, 1000)
        waits = {t: bpe.submit(f) for t, f in tests.items()}
    def try_result(w):
        try:
            return w.result()
        except Exception as e:
            print(e)
            return None
    res = {t: try_result(w) for t, w in waits.items()}
    res = {t: v for t, v in res.items() if v is not None}

    with open(f'{output}.json', 'w') as f:
        f.write(json.dumps(res))
    
with open(f'{output}.json') as f:
    d = json.loads(f.read())
    plot_timings(f'{output}.pdf', **d)