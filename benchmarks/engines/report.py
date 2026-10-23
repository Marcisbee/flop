#!/usr/bin/env python3
"""Summarize a complete collection without ranking failed cases."""
import argparse, collections, csv, json, pathlib, statistics
p=argparse.ArgumentParser();p.add_argument('directory',type=pathlib.Path);a=p.parse_args();d=a.directory
if not (d/'complete').exists():raise SystemExit('Refusing to summarize an incomplete series')
manifest=json.loads((d/'manifest.json').read_text());samples=[json.loads(x) for x in (d/'samples.jsonl').read_text().splitlines()]
groups=collections.defaultdict(list)
for s in samples:groups[(s['case_index'],s['label'])].append(s)
if any(len(v)!=manifest['repeats'] for v in groups.values()):raise SystemExit('Missing repeats')
columns=['case','engine','workload','workers','shared','offered_rate','checkpoints','runs','eligible','throughput_median','throughput_min','throughput_max','p50_ms','p99_ms','p999_ms','peak_rss_mib','errors','retries','read_errors','checkpoint_errors','checkpoint_calls','read_p999_ms','checkpoint_request_p999_ms']
rows=[]
for (case,label),ss in sorted(groups.items()):
 c=ss[0]['config'];median=lambda key:statistics.median(s[key] for s in ss)
 percentile=lambda key,index:statistics.median(s[key][index] for s in ss if s.get(key)) if any(s.get(key) for s in ss) else ''
 rows.append(dict(zip(columns,[case,label,c['Workload'],c['Workers'],c['Shared'],c['Rate'],c['Checkpoints'],len(ss),all(s['verification']=='passed' and s['reopen_verified'] for s in ss),median('throughput'),min(s['throughput'] for s in ss),max(s['throughput'] for s in ss),percentile('latency_ms',0),percentile('latency_ms',1),percentile('latency_ms',2),median('peak_rss_bytes')/1048576,sum(s['errors'] for s in ss),sum(s['retries'] for s in ss),sum(s['read_errors'] for s in ss),sum(s['checkpoint_errors'] for s in ss),sum(s['checkpoint_calls'] for s in ss),percentile('read_ms',2),percentile('checkpoint_request_ms',2)])))
with (d/'summary.csv').open('w') as f:w=csv.DictWriter(f,fieldnames=columns);w.writeheader();w.writerows(rows)
lines=['# Same-host embedded database comparison','',f"{manifest['repeats']} fresh-process samples per case. Values are medians; ranges are observed min–max, not confidence intervals.",'','Latency includes scheduling and retries in offered-load cases. Rate=0 is a two-second saturated run (deletes stop if the seeded rows run out). RSS is process high-water memory including setup, recorder, and reopen verification. Percentiles have up to 1% histogram quantization error; small samples cannot resolve rare tails reliably.','', 'SQLite uses WAL/FULL and BEGIN IMMEDIATE; Turso 0.8.3 uses MVCC/FULL and BEGIN CONCURRENT. SQL lock/conflict errors and Flop explicit row conflicts retry the whole operation every 1 ms, bounded at 30 seconds. All retry time is included. These are compact key/value workloads, not a universal database ranking.','']
for case in sorted({r['case'] for r in rows}):
 rr=[r for r in rows if r['case']==case];c=rr[0]
 lines += [f"## {c['workload']}, {c['workers']} workers, shared={c['shared']}, rate={c['offered_rate']}, checkpoints={c['checkpoints']}",'','| Engine | Successful ops/s [range] | p50 / p99 / p99.9 ms | Peak RSS MiB | Failed operations / retries | Eligible |','|---|---:|---:|---:|---:|---|']
 for r in rr:
  ps=' / '.join(f'{r[k]:.3f}' if r[k]!='' else '—' for k in ['p50_ms','p99_ms','p999_ms'])
  lines.append(f"| {r['engine']} | {r['throughput_median']:.1f} [{r['throughput_min']:.1f}–{r['throughput_max']:.1f}] | {ps} | {r['peak_rss_mib']:.1f} | {r['errors']} / {r['retries']} | {'yes' if r['eligible'] else 'no'} |")
 eligible=sorted([r for r in rr if r['engine']!='flop-before' and r['eligible']],key=lambda r:r['throughput_median'],reverse=True)
 if len(eligible)<2:verdict='Insufficient eligible engines for a ranking.'
 elif eligible[0]['throughput_min']>max(r['throughput_max'] for r in eligible[1:]):verdict=f"Throughput leader among eligible engines: {eligible[0]['engine']} (non-overlapping observed ranges)."
 else:verdict='Observed ranges overlap; no clear throughput winner.'
 lines+=['',verdict,'']
 if c['checkpoints']:
  for r in rr:lines.append(f"{r['engine']}: {r['checkpoint_calls']} checkpoint requests, {r['checkpoint_errors']} checkpoint errors, {r['read_errors']} failed diagnostic reads; read p99.9={r['read_p999_ms']} ms, checkpoint-request p99.9={r['checkpoint_request_p999_ms']} ms.")
  lines+=['','Checkpoint request latency is not proof of completed persistence. Flop may defer checkpoints while writes are pending; SQLite/Turso have different checkpoint policies. Diagnostic read timers coalesce under load.','']
(d/'report.md').write_text('\n'.join(lines)+'\n')
print(f'Wrote {len(rows)} summaries from {len(samples)} samples to {d}')
