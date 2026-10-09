"""Bounded Prometheus exposition and allowlisted JSON events; no request payloads.

Counters are process-local. Scrape each instance; use rate/increase across restarts.
No question, SQL, URL, exception text, identity, result, or model output is accepted.
"""
import asyncio
from collections import Counter
from functools import wraps
import json
import logging
import time

BOUNDS = (0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300)
STAGES = {"http", "query", "model", "workflow"}
OUTCOMES = {"success", "failed", "timeout", "cancelled", "interrupted", "clarification", "rejected"}
log = logging.getLogger("bi_analyst.operations")


class Telemetry:
    def __init__(self):
        self.calls, self.buckets, self.seconds, self.tokens = Counter(), Counter(), Counter(), Counter()
        self.coverage_values = {}
        self.coverage_observed_at = 0

    def observe(self, stage, outcome, seconds):
        if stage not in STAGES or outcome not in OUTCOMES:
            raise ValueError("Unbounded telemetry label")
        self.calls[stage, outcome] += 1
        self.seconds[stage] += seconds
        for bound in BOUNDS:
            if seconds <= bound:
                self.buckets[stage, bound] += 1
        log.info(json.dumps({"event": "analyst_operation", "stage": stage,
                             "outcome": outcome, "duration_ms": round(seconds * 1000)}))

    def usage(self, usage):
        for kind, field in (("input", "promptTokenCount"), ("output", "candidatesTokenCount"), ("thinking", "thoughtsTokenCount")):
            value = usage.get(field, 0)
            if isinstance(value, int) and 0 <= value <= 10_000_000:
                self.tokens[kind] += value

    def coverage(self, coverage):
        # Keys come only from the fixed gateway, never model/client labels.
        from ..semantic import load_catalogue
        if not hasattr(self, '_coverage_columns'):
            self._coverage_columns = next(r.columns for r in load_catalogue().relations if r.id == "coverage_v1")
        self.coverage_values = {k: int(v) for k, v in coverage.items()
                                if k in self._coverage_columns and isinstance(v, (int, float)) and v >= 0}
        self.coverage_observed_at = time.time()

    def render(self, database, metrics, workflow):
        families = {}
        types = {'operations_total':'counter','duration_seconds':'histogram',
                 'model_tokens_total':'counter','model_estimated_usd_total':'counter'}
        def sample(name, value, **labels):
            suffix = "{" + ",".join(f'{k}="{v}"' for k, v in labels.items()) + "}" if labels else ""
            family = 'duration_seconds' if name.startswith('duration_seconds_') else name
            families.setdefault(family,[]).append(f"bi_analyst_{name}{suffix} {value}")
        for stage in sorted(STAGES):
            for outcome in sorted(OUTCOMES):
                sample("operations_total", self.calls[stage, outcome], stage=stage, outcome=outcome)
            for bound in BOUNDS:
                sample("duration_seconds_bucket", self.buckets[stage, bound], stage=stage, le=bound)
            count = sum(self.calls[stage, o] for o in OUTCOMES)
            sample("duration_seconds_bucket", count, stage=stage, le="+Inf")
            sample("duration_seconds_count", count, stage=stage)
            sample("duration_seconds_sum", self.seconds[stage], stage=stage)
        for name, pool in (("read", database.read), ("state", database.state)):
            stats = pool.get_stats()
            for field in ("pool_size", "pool_available", "requests_waiting", "requests_errors", "requests_wait_ms"):
                sample("pool_" + field, stats.get(field, 0), pool=name)
        sample("active_queries", metrics.active)
        sample("active_workflows", len(workflow.tasks))
        for kind in ("input", "output", "thinking"):
            sample("model_tokens_total", self.tokens[kind], kind=kind)
        config = database.settings
        priced = config.model_input_usd_per_million is not None
        sample("model_cost_configured", int(priced))
        if priced:
            sample("model_estimated_usd_total", (self.tokens['input'] * config.model_input_usd_per_million +
                (self.tokens['output'] + self.tokens['thinking']) * config.model_output_usd_per_million) / 1_000_000)
        # Source lineage is not available yet. Unknown must alert, never report query time as freshness.
        for source in ("ingestion", "rollup", "refresh", "snapshot"):
            sample("source_freshness_known", 0, source=source)
        sample("coverage_observed_timestamp_seconds", self.coverage_observed_at)
        for name, value in sorted(self.coverage_values.items()):
            sample("coverage", value, counter=name)
        lines = []
        for family, samples in families.items():
            lines.append(f"# TYPE bi_analyst_{family} {types.get(family,'gauge')}")
            lines.extend(samples)
        return "\n".join(lines) + "\n"


def measured(stage):
    def decorate(method):
        @wraps(method)
        async def invoke(self, *args, **kwargs):
            started, outcome = time.monotonic(), "failed"
            try:
                result = await method(self, *args, **kwargs)
                outcome = "success"
                return result
            except asyncio.CancelledError:
                outcome = "cancelled"
                raise
            except TimeoutError:
                outcome = "timeout"
                raise
            finally:
                telemetry = getattr(self, "telemetry", None) or self.db.telemetry
                telemetry.observe(stage, outcome, time.monotonic() - started)
        return invoke
    return decorate
