"""Read-only hosted deployment preflight; never applies migrations or grants."""
import argparse
import asyncio
import json
import sys

from .. import __version__
from ..database import Database
from ..metrics.compiler import Compiler
from ..settings import Settings


async def check(settings, *, pilot=False):
    errors = []
    if settings.environment not in {'staging','production'}:
        errors.append('hosted_environment_required')
    if settings.metric_evaluation_enabled:
        errors.append('synthetic_evaluation_forbidden')
    if settings.deployment_overlap != 2:
        errors.append('rolling_deployment_connection_reserve_required')
    if not settings.telemetry_token:
        errors.append('telemetry_token_required')
    if pilot:
        if not settings.pilot_only or not settings.pilot_subjects:
            errors.append('explicit_pilot_required')
        if not settings.analyst_enabled or not settings.workflow_enabled or settings.auth_provider != 'monday':
            errors.append('identity_and_features_required')
        if settings.model_input_usd_per_million is None:
            errors.append('reviewed_model_pricing_required')
        if settings.disabled_metrics:
            errors.append('core_release_metrics_disabled')
    if not Compiler().catalogue.owner_accepted:
        errors.append('catalogue_acceptance_required')
    database = Database(settings)
    try:
        await database.open()
        await database.ready()
        if database.schema_version != 7:
            errors.append('schema_7_required')
        # No user data; verify the additive maintenance role has been installed.
        async with database.transaction() as conn:
            if not await (await conn.execute("SELECT 1 FROM pg_roles WHERE rolname='bi_analyst_maintenance'")).fetchone():
                errors.append('operations_migration_required')
        if pilot:
            for subject in settings.pilot_subjects:
                async with database.transaction(subject=subject) as conn:
                    row = await (await conn.execute('SELECT enabled,company_wide FROM analyst_state.principals WHERE subject=%s',(subject,))).fetchone()
                    if not row or not row['enabled'] or not row['company_wide']:
                        errors.append('pilot_principal_not_provisioned')
                        break
    except Exception:
        errors.append('database_or_privilege_preflight_failed')
    finally:
        await database.close()
    return {'passed':not errors,'api_version':__version__,'environment':settings.environment,
            'reserved_connections':settings.connection_budget,'errors':errors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pilot',action='store_true')
    args = parser.parse_args()
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        result = asyncio.run(check(Settings.from_env(),pilot=args.pilot))
    except Exception:
        result = {'passed':False,'errors':['invalid_configuration']}
    print(json.dumps(result))
    if not result['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
