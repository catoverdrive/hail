import json
import os
import subprocess as sp
import concurrent

import hail as hl
from hailtop.google_storage import GCS

import matplotlib.pyplot as plt

os.environ['HAIL_DONT_RETRY_500']='1'
os.environ['HAIL_QUERY_BACKEND'] = 'service'

filename = 'foo.json'

def test():
    t = hl.utils.range_table(1000, 100)
    t = t.filter((t.idx % 3 == 0) | ((t.idx / 7) % 3 == 0))
    n = t.count()
    print(f'n {n}')
    
def run():
    print(sp.check_output('hailctl dev query set flag log_service_timing timing_uncached', shell=True).decode('utf-8'))
    print(sp.check_output('hailctl dev query unset flag cache_service_input', shell=True).decode('utf-8'))
    try:
        test()
    except:
        print('cancelling')
    
    print(sp.check_output('hailctl dev query set flag log_service_timing timing_cached', shell=True).decode('utf-8'))
    print(sp.check_output('hailctl dev query set flag cache_service_input 1', shell=True).decode('utf-8'))
    try:
        test()
    except:
        print('cancelling')
        
def parse():
    tests = ['cached', 'single_cached', 'uncached', 'single_uncached']
    colors = ['blue', 'green', 'red', 'orange']
    with open('foo.json') as f:
        data = json.loads(f.read())

    bucket = sp.check_output('hailctl config get batch/bucket', shell=True).decode('utf-8').strip()

    files = {}
    for t in tests:
        lines = sp.check_output(f'gsutil ls gs://{bucket}/{t}', shell=True).decode('utf-8').strip().split('\n')
        files[t] = [l for l in lines if len(l) > 5 and l[:5] == 'gs://']

    client = GCS(concurrent.futures.ThreadPoolExecutor(), project='hail-vdc')
    for test in tests:
        if test not in data:
            data[test] = {}
        for f in files[test]:
            print(f"{test}: {f}")
            d = json.loads(client._read_gs_file(f))
            batch = str(d['batch'])
            if batch not in data[test]:
                print(f"adding data for {batch} in {test}. (existing: {data[test].keys()})")
                for i, res in enumerate(d['results']):
                    job = json.loads(sp.check_output(f'hailctl batch job {batch} {i} -o json', shell=True).decode('utf-8').strip())
                    for step in ['creating', 'starting', 'running']:
                        d['results'][i][step] = job["status"]["container_statuses"]["main"]["timing"][step]["duration"] / 1000.0
                    d['results'][i]['overhead'] = d['results'][i]['running'] - d['results'][i]['total']
                data[test][batch] = d
            else:
                print(f"found batch {batch} in {test}.")
    
    with open('foo.json', 'w') as f:
        f.write(json.dumps(data, indent=2))
    
    metrics = ["setup", "read", "execute", "write", "total", 'creating', 'starting', 'running', 'overhead']

    boxplot = {}
    for m in metrics:
        print(m)
        ds = [[r[m] for batch in data[t].values() for r in batch["results"]] for t in tests]
        boxplot[m] = ds
    #     print('; '.join(f'{t}: {len(d)}' for t, d in zip(tests, ds)))
        for t, d in zip(tests, ds):
            print(f'    {t}:')
            s = sorted(d)
            l = len(d)
            quantiles = [s[0], s[int(len(d) / 4)], s[int(len(d) / 2)], s[int(3 * len(d) / 4)], s[len(d) - 1]]
            average = sum(d)/len(d)
            print('      quantiles: ' + ', '.join('{:.2f}'.format(q) for q in quantiles))
            print(f'      average: {average:.2f}')
            print(f'      n: {len(d)}')

        fig = plt.figure()
        _, _, hists = plt.hist(ds, label=tests, color=colors)
        fig.legend(hists, tests)
        plt.title(m)
        fig.savefig(f"plot_{m}.pdf")
    
        fig = plt.figure()
        plt.boxplot(ds, labels=tests)
        plt.title(m)
        fig.savefig(f"plot_whiskers_{m}.pdf")
        
def plot_timings(output, **timings):
    fig = plt.figure(figsize=(6, 3 * len(timings)))
    for i, key in enumerate(timings.keys()):
        ax = plt.subplot(len(timings), 1, i + 1)        
        _, _, hist = ax.hist(timings[key])
        plt.title(f'{key} (n={len(timings[key])}, avg={sum(timings[key])/len(timings[key]):.4f} s)')
    fig.tight_layout()
    fig.savefig(output)

def parse_breakdowns(files, out):
    breakdowns = {}
    for k, fs in files.items():
        for fn in fs:
            with open(fn) as f:
                for r in json.loads(f.read())['results']:
                    for label, v in r['breakdown'].items():
                        if label not in breakdowns:
                            breakdowns[label] = [[],[]]
                        breakdowns[label].extend(v)
    print('; '.join(f'{k}: {len(v)}' for k, v in breakdowns.items()))
    plot_timings(out, **breakdowns)
    
# run()
# files = {'cached': ['cached.json'], 'uncached': ['uncached.json']}
# parse_breakdowns(files, 'out2.pdf')

def plot_firsts(files, output):
    timings = {}
    for fn in files:
        with open(fn) as f:
            for job in json.loads(f.read())['results']:
                for k, v in job['breakdown'].items():
                    if k not in timings:
                        timings[k] = [[], []]
                    if len(v) > 0:
                        timings[k][0].append(v[0])
                        timings[k][1].extend(v[1:])
    fig = plt.figure(figsize=(16, 3 * len(timings)))
    for i, key in enumerate(timings.keys()):
        ax = plt.subplot(len(timings), 3, 3 * i + 1)
        _, _, hists = ax.hist(timings[key], stacked=True)
        plt.legend(hists, ['first call', 'other calls'])
        plt.title(f'{key} (n={[len(f) for f in timings[key]]}, avg={[sum(f)/len(f) for f in timings[key]]} s)')
        
        ax = plt.subplot(len(timings), 3, 3 * i + 2)        
        _, _, hists = ax.hist(timings[key][0])
        plt.legend(hists, ['first call'])
        plt.title(f'{key} (n={len(timings[key][0])}, avg={sum(timings[key][0])/len(timings[key][0]):.4f} s)')
        ax = plt.subplot(len(timings), 3, 3 * i + 3)        
        _, _, hists = ax.hist(timings[key][1])
        plt.legend(hists, ['other calls'])
        plt.title(f'{key} (n={len(timings[key][1])}, avg={sum(timings[key][1])/len(timings[key][1]):.4f} s)')
    fig.tight_layout()
    fig.savefig(output)

colors = ['red', 'orange', 'green', 'blue', 'cyan', 'purple', 'yellow', 'magenta']
def plot2(batch):
    ts = {}
    for i, job in enumerate(batch):
        timings = {}
        for t in job:
            if t['label'] not in timings:
                timings[t['label']] = []
            timings[t['label']].append((t['start'], t['duration']))
        assert 'start' in timings and len(timings['start']) == 1
        start = timings['start'][0][0]
        timings = {label: [((s - start) / 1000_000_000, duration) for s, duration in times] for label, times in timings.items() if label != 'start'}
        for label, times in timings.items():
            if label not in ts:
                ts[label] = {}
            ts[label][i] = times
    color_map = dict(zip(ts.keys(), colors))
    print(ts.keys())
    print(color_map)
    labels = []
    for label, jobs in ts.items():
        for job_no, times in jobs.items():
            for start, duration in times:
                line, = plt.plot([start, start + duration], [.1 * job_no, .1 * job_no], color=color_map[label])
        labels.append((line, label))
    plt.legend([line for line, _ in labels], [label for _, label in labels])

def get_json_from_paths(paths, out):
    files = {}
    for p in paths:
        lines = sp.check_output(f'gsutil ls gs://amwang/{p}', shell=True).decode('utf-8').strip().split('\n')
        files[p] = [l for l in lines if len(l) > 5 and l[:5] == 'gs://' and l[-5:] == '.json']
        print('\n'.join(files[p]))

    client = GCS(concurrent.futures.ThreadPoolExecutor(), project='hail-vdc')
    
    d = {p: [json.loads(client._read_gs_file(f)) for f in fs] for p, fs in files.items()}
    with open(out, 'w') as f:
        f.write(json.dumps(d, indent=2))
    return d
        
        
jsonfile = 'updated.json'
if os.path.exists(jsonfile):
    with open(jsonfile) as f:
        d = json.loads(f.read())
else:
    d = get_json_from_paths(['tmp/hail/query/v8Lsk37Mk4Xmm9O0OnwX5vRQfgXcN_KbvTIFbiDddwA=', 'tmp/hail/query/ZK-ixab6c8lsLL-naLrAiKDwLjPcMO_xvVQt2FW9jeI='], jsonfile)
    
i = 0
for batch_name, batch in d.items():
    i += 1
    outfile = f'plot_{i}.pdf'
    fig = plt.figure()
    plot2(batch)
    fig.tight_layout()
    fig.savefig(outfile)
    
# run()
# plot_firsts(['cached.json', 'uncached.json'], 'first.pdf')
