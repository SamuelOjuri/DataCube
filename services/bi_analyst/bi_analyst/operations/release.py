"""Fingerprint release inputs and fail closed on missing qualification evidence.

This is an offline evidence validator, not a deployment client. Evidence is a
reviewed attestation backed by files, not proof of an external service's state.
"""
import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
import tomllib

from .. import __version__
from ..metrics.compiler import COMPILER_VERSION, Compiler
from ..workflow.provider import GENERATION, MODEL, PROMPT_VERSION

CHECKS = ('metric_parity','permissions','presentation','recovery','hosted_auth',
          'hosted_streaming','frontend_secrets','preview_isolation','rollback',
          'monitoring','retention','pilot_acceptance','source_coverage_freshness',
          'model_evaluation','load','cost')


def fingerprint(root):
    root = Path(root).resolve()
    for required in ('services/bi_analyst/pyproject.toml','services/bi_analyst/requirements.lock',
                     'services/bi_analyst/bi_analyst/semantic/catalogue.json','web/package-lock.json','netlify.toml'):
        if not (root/required).is_file():
            raise ValueError('Complete candidate repository required')
    inputs = set()
    for folder, suffixes in (('services/bi_analyst', {'.py','.sql','.json','.toml','.lock','.yaml','.yml'}),
                            ('src/database/migrations', {'.sql'}), ('src/database/schema', {'.sql'}),
                            ('web', {'.ts','.tsx','.mjs','.json','.toml','.html','.css'}),
                            ('.github/workflows', {'.yml','.yaml'})):
        for path in (root/folder).rglob('*'):
            if path.is_file() and path.suffix in suffixes and not any(part in
                {'node_modules','dist','__pycache__','.pytest_cache','build','.venv'} or part.endswith('.egg-info')
                for part in path.relative_to(root).parts):
                inputs.add(path)
    inputs.add(root/'netlify.toml')
    # Canonical text endings make Windows and Linux checkouts agree on a candidate.
    files = {p.relative_to(root).as_posix():sha256(p.read_bytes().replace(b'\r\n',b'\n')).hexdigest() for p in sorted(inputs)}
    compiler = Compiler()
    return {'api_version':__version__,'contract_version':'v1','schema_version':7,
        'catalogue_version':compiler.catalogue.version,'catalogue_sha256':compiler.catalogue_hash,
        'compiler_version':COMPILER_VERSION,'model':MODEL,'prompt':PROMPT_VERSION,'generation':GENERATION,
        'files':files,'sha256':sha256(json.dumps(files,sort_keys=True).encode()).hexdigest()}


def validate(root, evidence, policy, *, now=None):
    current = fingerprint(root)
    errors = []
    if evidence.get('release_sha256') != current['sha256']:
        errors.append('release_fingerprint_mismatch')
    if evidence.get('environment') != 'staging':
        errors.append('dedicated_staging_required')
    try:
        created = datetime.fromisoformat(evidence['recorded_at'])
        age = ((now or datetime.now(timezone.utc))-created).total_seconds()
        if not 0 <= age <= policy['max_evidence_age_days'] * 86400:
            errors.append('evidence_expired')
    except (KeyError, ValueError, TypeError):
        errors.append('evidence_date_required')
    if not all(isinstance(evidence.get(k),str) and evidence[k].strip() for k in
               ('reviewer','deployment_reference','dataset_reference')):
        errors.append('reviewer_deployment_and_dataset_required')
    for check in CHECKS:
        item = evidence.get('checks',{}).get(check,{})
        if item.get('passed') is not True:
            errors.append(check + '_pending')
            continue
        try:
            artifact = (Path(root)/item['artifact']).resolve()
            artifact.relative_to(Path(root).resolve())
            if sha256(artifact.read_bytes()).hexdigest() != item['sha256']:
                errors.append(check + '_artifact_mismatch')
        except (KeyError, ValueError, OSError, TypeError):
            errors.append(check + '_artifact_required')
    required_metrics = {m.id for m in Compiler().catalogue.metrics}
    if set(evidence.get('enabled_metrics',[])) != required_metrics:
        errors.append('all_core_metric_variants_required')
    targets = policy.get('targets',{})
    if targets.get('approved') is not True or not targets.get('approval_reference'):
        errors.append('performance_targets_not_approved')
    measured = evidence.get('load',{})
    for field in ('query_p95_seconds','answer_p95_seconds','failure_rate','usd_per_answer'):
        value, maximum = measured.get(field), targets.get('max_'+field)
        if (not isinstance(value,(int,float)) or isinstance(value,bool) or not isinstance(maximum,(int,float))
            or isinstance(maximum,bool) or not math.isfinite(maximum) or not 0 <= value <= maximum):
            errors.append(field + '_target_not_met')
    for field in ('concurrency','samples'):
        value, minimum = measured.get(field), targets.get('min_'+field)
        if not isinstance(value,int) or isinstance(value,bool) or not isinstance(minimum,int) or isinstance(minimum,bool) or not 1 <= minimum <= value:
            errors.append(field + '_target_not_met')
    return {'qualified':not errors,'release_sha256':current['sha256'],'errors':errors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('manifest','template','check'))
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--evidence',type=Path)
    parser.add_argument('--policy',type=Path)
    args = parser.parse_args()
    if args.command == 'manifest':
        result = fingerprint(args.root)
    elif args.command == 'template':
        result = {'release_sha256':fingerprint(args.root)['sha256'],'environment':'staging',
            'recorded_at':datetime.now(timezone.utc).isoformat(),'reviewer':'','deployment_reference':'','dataset_reference':'',
            'enabled_metrics':[m.id for m in Compiler().catalogue.metrics],
            'checks':{name:{'passed':False,'artifact':'','sha256':''} for name in CHECKS}, 'load':{}}
    else:
        if not args.evidence or not args.policy:
            parser.error('check requires --evidence and --policy')
        try:
            result = validate(args.root,json.loads(args.evidence.read_text(encoding='utf-8')),
                              tomllib.loads(args.policy.read_text(encoding='utf-8')))
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            result = {'qualified':False,'errors':['invalid_evidence_or_policy']}
    payload = json.dumps(result,indent=2,allow_nan=False) + '\n'
    if args.output:
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(payload,encoding='utf-8')
    else:
        print(payload,end='')
    if args.command == 'check' and not result['qualified']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
