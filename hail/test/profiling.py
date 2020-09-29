import subprocess as sp
import os
import json


def job_with_profiling(i):
    return {
        "name": f"profile_{i}",
        "flags": ['-Xrunhprof:cpu=samples,file=/tmp/profile.hprof'],
        "className": "is.hail.backend.service.Worker",
        "args": ["gs://amwang/test", str(i)],
        "outputFiles": [{
            "from": "/tmp/profile.hprof",
            "to": f"gs://amwang/test/profile_{i}.hprof"
        }],
    }
    
def make_batch(name, image):
    return {
        "name": name,
        "image": image,
        "project": "hail"
    }
    
image = 'gcr.io/hail-vdc/query:a685168f93ef23b940a763c4582b7b280413f2c6b351b649a02343c3a29936ae'

# import hailtop.batch as hb 
# backend = hb.ServiceBackend('hail', 'amwang')
# b = hb.Batch(backend=backend, name='profile_worker')
# for i in range(100):
#     j = b.new_job(name=f'profile_{i}') 
#     j.image(image)
#     j.command(f'java -Xrunhprof:cpu=samples,file={j.ofile},depth=30 -cp $SPARK_HOME/jars/*:/hail.jar is.hail.backend.service.Worker gs://amwang/test {i}') 
#     b.write_output(j.ofile, f'gs://amwang/test/profile_{i}.hprof')
# 
# b.run(open=True) 

# image = 'gcr.io/hail-vdc/query:a685168f93ef23b940a763c4582b7b280413f2c6b351b649a02343c3a29936ae'
# batch = make_batch("profile_worker", image)
# batch = dict(batch, jobs=[job_with_profiling(i) for i in range(100)])
# with open('scratch.json', 'w') as f:
#     f.write(json.dumps(batch, indent=2))
#     
# cmd = f"time java -Xrunhprof:cpu=samples,file=/tmp/profile.hprof -cp {os.environ['SPARK_HOME']}/jars/*:{os.environ['JAR_PATH']} is.hail.services.batch_client.JavaBatchExecutor scratch.json"
# print(cmd)
# sp.call(cmd, shell=True)

# for i in range(100):
#     sp.call(f'gsutil cp gs://amwang/test/profile_{i}.hprof profiling_output/profile_{i}.hprof.txt', shell=True)
    
    
def parse_trace(f):
    next_line = f.readline().strip()
    while next_line != '--------':
        next_line = f.readline().strip()

    traces = {}
    trace_no = None
    while True:
        next_line = f.readline().strip()
        if len(next_line) > 17 and next_line[:17] == 'CPU SAMPLES BEGIN':
            return traces
        if next_line != '' and next_line[:6] != "THREAD":
            if next_line[:5] == 'TRACE':
                if trace_no is not None:
                    traces[trace_no] = trace
                trace_no = int(next_line[6:-1])
                trace = []
            else:
                trace.append(next_line)

with open('profiling_output/profile_0.hprof.txt') as f:
    traces = parse_trace(f)
    print(len(traces))
    id, trace = list(traces.items())[0]
    ids = traces.keys()
    print(min(ids), max(ids), max(ids) - min(ids))
    print(id)
    print('\n'.join(trace))