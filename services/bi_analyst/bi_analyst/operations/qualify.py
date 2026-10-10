"""Explicit, bounded hosted STAGING load. Creates analyst state and calls Gemini.

Session tokens are read only from BI_ANALYST_LOAD_TOKENS (a JSON string array).
Outputs contain aggregate timings/statuses, never questions/results or tokens.
"""
import argparse
import asyncio
from collections import Counter
import json
import math
import os
from pathlib import Path
import time
from urllib.parse import urlsplit
from uuid import uuid4

import httpx

from ..metrics.compiler import Compiler
from ..settings import DEFAULT_WORKFLOW_TIMEOUT_SECONDS


def origin(value, *, local=False):
    url = urlsplit(value)
    if (not url.hostname or url.path or url.query or url.fragment or url.username or url.password
        or (url.scheme != 'https' and not (local and url.scheme == 'http' and url.hostname in {'127.0.0.1','localhost'}))):
        raise ValueError('Use an exact HTTPS staging origin')
    return value


def summarize(durations, statuses, concurrency):
    values = sorted(durations)
    samples = sum(statuses.values())
    return {'samples':samples,'concurrency':concurrency,'successful':len(values),
            'p95_seconds':values[max(0,math.ceil(len(values)*0.95)-1)] if values else None,
            'failure_rate':(samples-len(values))/samples if samples else 1,
            'outcomes':dict(sorted(statuses.items()))}


async def load(client, tokens, *, samples, concurrency, mode):
    if not 1 <= concurrency <= 8 or not 1 <= samples <= 500 or not tokens:
        raise ValueError('Invalid load bounds')
    health = (await client.get('/health/ready')).json()
    if (health.get('environment') != 'staging' or health.get('metric_execution') != 'owner_accepted'
        or health.get('identity') != 'monday' or health.get('pilot_only') is not True
        or health.get('schema_version') != 7 or mode == 'answer' and health.get('workflow') != 'enabled'):
        raise ValueError('Dedicated enabled staging pilot required')
    metrics = Compiler().catalogue.metrics
    semaphore, durations, statuses = asyncio.Semaphore(concurrency), [], Counter()

    async def sample(index):
        async with semaphore:
            headers = {'Authorization':'Bearer '+tokens[index % len(tokens)]}
            started, run_id = time.monotonic(), None
            try:
                async def post(path, body):
                    response = await client.post(path,json=body,headers=headers)
                    response.raise_for_status()
                    return response.json()
                metric = metrics[index % len(metrics)]
                async with asyncio.timeout(DEFAULT_WORKFLOW_TIMEOUT_SECONDS + 60 if mode == 'answer' else 120):
                    conversation = await post('/v1/conversations',{'title':'Release qualification'})
                    if mode == 'query':
                        run = await post(f"/v1/conversations/{conversation['id']}/runs",{'question':'Release qualification'})
                        run_id = run['id']
                        result = await post(f'/v1/runs/{run_id}/metric',{'metric_id':metric.id,'metric_version':metric.version,
                            'population':metric.population,'period':metric.periods[0],'include_total':True})
                        if result.get('provenance',{}).get('metric_id') != metric.id:
                            raise ValueError('Unexpected metric')
                    else:
                        run = await post(f"/v1/conversations/{conversation['id']}/messages",{
                            'idempotency_key':str(uuid4()),'question':metric.examples[0]})
                        run_id = run['run_id']
                        while run['status'] in {'registered','running'}:
                            await asyncio.sleep(1)
                            response = await client.get(f'/v1/runs/{run_id}/workflow',headers=headers)
                            response.raise_for_status()
                            run = response.json()
                        if run['status'] != 'completed':
                            statuses[run['status'] if run['status'] in {'failed','cancelled','interrupted','awaiting_clarification'} else 'invalid'] += 1
                            return
                    durations.append(time.monotonic()-started)
                    statuses['success'] += 1
            except httpx.HTTPStatusError as error:
                statuses[str(error.response.status_code)] += 1
            except TimeoutError:
                statuses['timeout'] += 1
                if run_id:
                    try:
                        await client.post(f'/v1/runs/{run_id}/cancel',headers=headers)
                    except httpx.HTTPError:
                        pass
            except (httpx.HTTPError, KeyError, ValueError):
                statuses['unavailable'] += 1
    await asyncio.gather(*(sample(i) for i in range(samples)))
    return {'mode':mode,**summarize(durations,statuses,concurrency),
            'api_version':health['api_version'],'catalogue_sha256':health['catalogue_sha256']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--origin',required=True)
    parser.add_argument('--execute',action='store_true',help='Create staging state and incur model usage')
    parser.add_argument('--mode',choices=('query','answer'),required=True)
    parser.add_argument('--samples',type=int,default=50)
    parser.add_argument('--concurrency',type=int,default=2)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    if not args.execute:
        parser.error('--execute is required for staging load')
    async def run():
        target = origin(args.origin)
        tokens = json.loads(os.environ['BI_ANALYST_LOAD_TOKENS'])
        if not isinstance(tokens,list) or not 1 <= len(tokens) <= 100 or not all(isinstance(t,str) and t.strip() for t in tokens):
            raise ValueError('Invalid sessions')
        async with httpx.AsyncClient(base_url=target,timeout=60,follow_redirects=False,trust_env=False) as client:
            return await load(client,tokens,samples=args.samples,concurrency=args.concurrency,mode=args.mode)
    try:
        result = asyncio.run(run())
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    except Exception:
        print('{"status":"qualification_failed"}')
        raise SystemExit(2) from None
    if result['failure_rate'] > 0:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
