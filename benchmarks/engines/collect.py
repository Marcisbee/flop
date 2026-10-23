#!/usr/bin/env python3
"""Run isolated, alternating engine samples; retain failures rather than dropping them."""
import argparse, hashlib, json, os, pathlib, platform, random, subprocess, time
p=argparse.ArgumentParser()
p.add_argument('--baseline',required=True,type=pathlib.Path)
p.add_argument('--candidate',required=True,type=pathlib.Path)
p.add_argument('--output',required=True,type=pathlib.Path)
p.add_argument('--repeats',type=int,default=5)
p.add_argument('--smoke',action='store_true')
a=p.parse_args()
if a.output.exists():raise SystemExit('output must be new')
a.output.mkdir(parents=True);data=a.output.resolve()/'data';data.mkdir()
engines=[('flop-before',a.baseline,'flop'),('flop-after',a.candidate,'flop'),('sqlite',a.candidate,'sqlite'),('turso',a.candidate,'turso')]
manifest={'host':platform.uname()._asdict(),'repeats':a.repeats,'smoke':a.smoke,'seed':42,'cwd':os.getcwd(),'engines':{},'sqlite_driver':'github.com/mattn/go-sqlite3 v1.14.52','turso_driver':'turso.tech/database/tursogo v0.8.3','filesystem':subprocess.check_output(['findmnt','-T',str(data),'-no','FSTYPE,SOURCE'],text=True).strip()}
for label,directory,kind in engines:
 directory=directory.resolve();manifest['engines'][label]={'revision':(directory/'revision').read_text().strip(),'sha256':hashlib.sha256((directory/'engine.test').read_bytes()).hexdigest()}
(a.output/'manifest.json').write_text(json.dumps(manifest,indent=2))
cases=[]
for rate in [1000,0]:
 for workers in [1,2,4,8,16,32,64]:
  for shared in [0,1]:cases.append(('tx',workers,shared,rate,0,64))
for work in ['read','update','insert','delete','mixed']:
 for workers in [1,8,64]:cases.append((work,workers,0,0,0,1024 if work in ['read','delete','mixed'] else 64))
for work in ['update','tx']:
 for workers in [8,32]:cases.append((work,workers,0,1000,1,64))
if a.smoke:cases=[(w,8,0,1000,0,64) for w in ['read','update','insert','delete','tx','mixed']]+[('tx',8,1,1000,0,64),('tx',8,0,1000,1,64)]
seq=0;start=time.time()
with (a.output/'samples.jsonl').open('w') as out:
 for repeat in range(a.repeats):
  for case_index,case in enumerate(cases):
   # Rotate/reverse engine order to reduce systematic drift; each comparison is adjacent.
   order=engines[repeat%4:]+engines[:repeat%4]
   if repeat%2:order=list(reversed(order))
   for label,directory,kind in order:
    work,workers,shared,rate,cp,rows=case
    env=os.environ.copy();env.update(TMPDIR=str(data),GOMAXPROCS='32',EC_ENGINE=kind,EC_REVISION=manifest['engines'][label]['revision'],EC_WORKLOAD=work,EC_WORKERS=str(workers),EC_SHARED=str(shared),EC_RATE=str(rate),EC_OPS='64' if a.smoke else '1000',EC_SECONDS='2',EC_CHECKPOINT=str(cp),EC_ROWS=str(rows))
    seq+=1;name=f'{seq:04d}-{label}-r{repeat+1}-{work}-w{workers}-s{shared}-rate{rate}-cp{cp}'
    r=subprocess.run([str((directory/'engine.test').resolve()),'-test.run','^TestEngineComparison$','-test.v','-test.timeout','120s'],env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=130)
    (a.output/(name+'.log')).write_text(r.stdout)
    lines=[line.removeprefix('ENGINE_RESULT ') for line in r.stdout.splitlines() if line.startswith('ENGINE_RESULT ')]
    if r.returncode or len(lines)!=1:raise SystemExit(f'failed {name}; inspect log; series incomplete')
    result=json.loads(lines[0]);result.update(label=label,repeat=repeat+1,case_index=case_index,log=name+'.log')
    out.write(json.dumps(result)+'\n');out.flush()
    print(f'{seq}/{len(cases)*a.repeats*4} {label} {work} w={workers} shared={shared} rate={rate} cp={cp}: {result["throughput"]:.1f}/s errors={result["errors"]} verify={result["verification"]} elapsed={time.time()-start:.0f}s',flush=True)
(a.output/'complete').write_text(str(seq)+'\n')
