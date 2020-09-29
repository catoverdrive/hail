import subprocess as sp
import os
import json
import sys

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
    
image = 'gcr.io/hail-vdc/jvm-profile:latest'
# 

def profiler_flag(logfile, profiler="honest"):
    if profiler == 'honest':
        return f"-agentpath:/honest-profiler/liblagent.so=interval=7,logPath={logfile}"
    elif profiler == 'honest-local':
        return f"-agentpath:/me/code/hail/query/honest-profiler/liblagent.so=interval=7,logPath={logfile}"
    elif profiler == 'hprof':
        return f"-Xrunhprof:cpu=samples,interval=7,depth=90,file={logfile}"

def process_hpls(hpls):
    for h in hpls:
        prefix = h.split('.')[0]
        sp.call(f'java -cp ../honest-profiler/honest-profiler.jar com.insightfullogic.honest_profiler.ports.console.FlameGraphDumperApplication {prefix}.hpl {prefix}.folded', shell=True)
        sp.call(f'/Users/wang/code/FlameGraph/flamegraph.pl {prefix}.folded > {prefix}.svg', shell=True)
        
def process_hprofs(hprofs):
    for h in hprofs:
        prefix = h.split('.')[0]
        sp.call(f'stackcollapse-hprof {prefix}.hprof > {prefix}.folded', shell=True)
        sp.call(f'/Users/wang/code/FlameGraph/flamegraph.pl {prefix}.folded > {prefix}.svg', shell=True)    
        
def process_batch(n, ofile_location, profiler):
    if profiler == "honest":
        process_hpls(run_batch(n, ofile_location, profiler))
    else:
        process_hprofs(run_batch(n, ofile_location, profiler))


def run_batch(n, ofile_location, profiler="honest"):
    import hailtop.batch as hb 
    ofiles = []
    backend = hb.ServiceBackend('hail', 'amwang')
    b = hb.Batch(backend=backend, name='profile_worker', default_cpu="1", default_memory="3.75Gi")
    for i in range(n):
        j = b.new_job(name=f'profile_{i}') 
        j.image(image)
        cmd = f"""
java {profiler_flag(j.ofile1, profiler)} -cp $SPARK_HOME/jars/*:/hail.jar is.hail.backend.service.Worker gs://amwang/test {i};
        """
        j.command(cmd) 
        ext = 'hpl' if profiler == 'honest' else 'hprof'
        out = f'gs://amwang/test/flame_graph/{ofile_location}/log_{i}.{ext}'
        b.write_output(j.ofile1, out)
        ofiles.append(out)

    b.run(open=True) 
    for f in ofiles:
        sp.call(f'gsutil cp {f} {ofile_location}/', shell=True)
    return [f"{ofile_location}/{f.split('/')[-1]}" for f in ofiles]

def run_local(n, ofile_location):
    image = 'jvm-profile:latest'
    user = "/Users/wang"
    folder = f"/me/code/hail/query"
    key = "/me/.hail/gcs-keys/wang-gsa-key.json"
    ofiles = []
    for i in range(n):
        ofile = f'{folder}/profiling/{ofile_location}/log_{i}.hpl'
        ofiles.append(ofile)
        cmd = f'java -agentpath:{folder}/honest-profiler/liblagent.so=interval=7,logPath={ofile} -cp $SPARK_HOME/jars/*:/hail.jar is.hail.backend.service.Worker gs://amwang/test {i}'
        cmd = f'docker run --mount type=bind,src={user},dst=/me --env HAIL_GSA_KEY_FILE={key} {image} {cmd}'
        print(cmd)
        sp.call(cmd, shell=True)
    return ofiles
    
    
def run_in_image(n, ofile_location):
    folder = f"/me/code/hail/query"
    key_files = ["/me/.hail/gcs-keys/wang-gsa-key.json", "/me/.hail/gcs-keys/sharkturus-gsa-key.json"]
    for i in range(n):
        ofile = f'{folder}/profiling/{ofile_location}/log_{i}.hpl'
#         ofiles.append(ofile)
        cmd = f'java -agentpath:{folder}/honest-profiler/liblagent.so=interval=7,logPath={ofile} -cp $SPARK_HOME/jars/*:/hail.jar is.hail.backend.service.RunWorker gs://amwang/test {i} 2 2 {" ".join(key_files)}'
        print(cmd)
        sp.call(cmd, shell=True)
#     return ofiles

# process_hpls(run_local(10, 'local'))

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.argv.append(None)
    if len(sys.argv) < 3:
        sys.argv.append(None)
    if sys.argv[1] == 'docker':
        run_in_image(10, 'local')
    elif sys.argv[1] == 'local':
        process_hpls([f"local/{f}" for f in os.listdir('local') if len(f) > 4 and f[-4:] == '.hpl'])
    elif sys.argv[1] == 'wang':
        sp.call('hailctl dev config wang', shell=True)
        process_batch(24, 'batch_wang', sys.argv[2])
    elif sys.argv[1] == 'default':
        sp.call('hailctl dev config default', shell=True)
        process_batch(24, 'batch_default', sys.argv[2])
    
# def parse_trace(f):
#     next_line = f.readline().strip()
#     while next_line != '--------':
#         next_line = f.readline().strip()
# 
#     traces = {}
#     trace_no = None
#     while True:
#         next_line = f.readline().strip()
#         if len(next_line) > 17 and next_line[:17] == 'CPU SAMPLES BEGIN':
#             return traces
#         if next_line != '' and next_line[:6] != "THREAD":
#             if next_line[:5] == 'TRACE':
#                 if trace_no is not None:
#                     traces[trace_no] = trace
#                 trace_no = int(next_line[6:-1])
#                 trace = []
#             else:
#                 trace.append(next_line)
# 
# with open('profiling_output/profile_0.hprof.txt') as f:
#     traces = parse_trace(f)
#     print(len(traces))
#     id, trace = list(traces.items())[0]
#     ids = traces.keys()
#     print(min(ids), max(ids), max(ids) - min(ids))
#     print(id)
#     print('\n'.join(trace))