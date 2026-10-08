"""Bounded Monday source evidence: fixed queries only, independent of OAuth/ETL."""
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import HTTPException, Request
import httpx
from pydantic import Field, model_validator

from .contracts import Contract
from .monday_auth import API_URL

# Reviewed app boards/financial columns from src/config.py, copied without importing
# the ETL's environment or credentials. Callers cannot extend these allowlists.
BOARDS = {"projects": "1825117125", "subitems": "1825117144", "hidden_items": "1825138260"}
COLUMNS = {
    "projects": ("status4__1", "date9__1", "dropdown7__1", "dropdown__1", "mirror5__1",
                 "lookup_mkqanpbe", "formula_mkpp85yw", "lookup_mkpptgsn", "lookup_mkppq0gc"),
    "subitems": ("formula_mkqa31kh", "formula_mkpp1x74", "formula_mkppwkkq", "mirror03__1",
                 "mirror77__1", "mirror98__1", "mirror082__1", "mirror226__1", "mirror5__1",
                 "mirror17__1", "mirror73__1", "connect_boards8__1"),
    "hidden_items": ("formula_mkncjq9", "numbers98__1", "numbers3__1", "formula63__1",
                     "dropdown2__1", "date__1", "date42__1", "numbers56__1", "date7__1", "status__1"),
}
SCOPE_QUERY = """query AnalystSourceScope($ids: [ID!]!) {
  me { account { id } }
  items(ids: $ids, limit: 20, exclude_nonactive: false) { id board { id } }
}"""
EVIDENCE_QUERY = """query AnalystSourceEvidence($ids: [ID!]!, $boards: [ID!]!, $columns: [String!]!) {
  me { account { id } }
  boards(ids: $boards) { id columns(ids: $columns) { id title type settings } }
  items(ids: $ids, limit: 20, exclude_nonactive: false) {
    id name state updated_at board { id }
    column_values(ids: $columns) {
      id type text value __typename
      ... on FormulaValue { display_value }
      ... on MirrorValue { display_value mirrored_items { linked_board_id linked_item { id } } }
      ... on BoardRelationValue { linked_item_ids }
    }
  }
}"""
MondayID = Annotated[str, Field(pattern=r"^[1-9][0-9]{0,29}$")]
ColumnID = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")]


class SourceCheckRequest(Contract):
    metric_id: ColumnID
    metric_version: str = Field(pattern=r"^[1-9][0-9]*\.[0-9]+\.[0-9]+$")
    population: Literal["reportable", "hidden_inventory"]
    reason: Literal["metric_not_certified", "source_discrepancy", "missing_source", "freshness_unknown"]
    board: Literal["projects", "subitems", "hidden_items"]
    item_ids: list[MondayID] = Field(min_length=1, max_length=20)
    column_ids: list[ColumnID] = Field(min_length=1, max_length=5)

    @model_validator(mode="after")
    def allowed_scope(self):
        if len(set(self.item_ids)) != len(self.item_ids) or len(set(self.column_ids)) != len(self.column_ids):
            raise ValueError("Use distinct exact IDs")
        if not set(self.column_ids) <= set(COLUMNS[self.board]):
            raise ValueError("Unsupported source columns")
        return self


class SourceEvidence(Contract):
    id: UUID
    run_id: UUID
    permissions_version: int
    request: SourceCheckRequest
    source: Literal["monday_graphql"] = "monday_graphql"
    read_only: Literal[True] = True
    dataset_scope: Literal["requested_live_items"] = "requested_live_items"
    status: Literal["observed", "partial"]
    observed_at: datetime
    api_version: str
    query_reference: str
    metric_acceptance: Literal["owner_accepted", "not_accepted"]
    board_id: str
    columns: list[dict]
    items: list[dict]
    missing_item_ids: list[str]
    missing_column_ids: list[str]
    source_text_is_untrusted: Literal[True] = True
    limitations: list[str]


class MondaySourceReader:
    def __init__(self, settings, client: httpx.AsyncClient):
        self.settings, self.client = settings, client
        self.active = 0

    @property
    def configured(self):
        return self.settings.monday_read_token is not None and self.settings.monday_account_id is not None

    def capability(self):
        return {"available": self.configured, "method": "POST",
                "path": "/v1/runs/{run_id}/source-check", "read_only": True,
                "requires": ["exact_item_ids", "approved_column_ids"],
                "max_items": 20, "max_columns": 5, "boards": BOARDS, "columns": COLUMNS}

    async def _read(self, operation: Literal["scope", "evidence"], variables: dict):
        # No raw query argument, endpoint override, redirects, pagination or retries.
        query = {"scope": SCOPE_QUERY, "evidence": EVIDENCE_QUERY}[operation]
        try:
            async with self.client.stream("POST", API_URL, follow_redirects=False,
                    headers={"Authorization": self.settings.monday_read_token.get_secret_value(),
                             "API-Version": self.settings.monday_read_api_version},
                    json={"query": query, "variables": variables}) as response:
                if response.status_code == 429:
                    raise HTTPException(429, "monday_source_rate_limited", headers={"Retry-After": "60"})
                response.raise_for_status()
                if response.headers.get("API-Version") != self.settings.monday_read_api_version:
                    raise HTTPException(503, "monday_source_api_version_mismatch")
                raw = bytearray()
                async for chunk in response.aiter_bytes():
                    raw.extend(chunk)
                    if len(raw) > self.settings.monday_read_max_bytes:
                        raise HTTPException(503, "monday_source_response_too_large")
                def invalid_constant(value):
                    raise ValueError("Invalid JSON constant")
                payload = json.loads(raw, parse_float=Decimal, parse_constant=invalid_constant)
                if not isinstance(payload, dict) or payload.get("errors") or not isinstance(payload.get("data"), dict):
                    raise ValueError("No complete GraphQL response")
                data = payload["data"]
                if str(data["me"]["account"]["id"]) != self.settings.monday_account_id:
                    raise HTTPException(403, "monday_source_account_denied")
                # Preserve decimal text if a JSON settings object contains numbers.
                return json.loads(json.dumps(data, default=str))
        except (httpx.HTTPError, ValueError, TypeError, KeyError):
            raise HTTPException(503, "monday_source_unavailable") from None

    @staticmethod
    def _items(data, body):
        items = data.get("items")
        if not isinstance(items, list) or len(items) > len(body.item_ids):
            raise HTTPException(503, "monday_source_unavailable")
        seen = set()
        for item in items:
            if (not isinstance(item, dict) or item.get("id") not in body.item_ids
                    or item["id"] in seen or not isinstance(item.get("board"), dict)):
                raise HTTPException(503, "monday_source_unavailable")
            if item["board"].get("id") != BOARDS[body.board]:
                raise HTTPException(403, "monday_source_board_denied")
            seen.add(item["id"])
        return items

    async def lookup(self, body: SourceCheckRequest):
        if not self.configured:
            raise HTTPException(503, "monday_source_not_configured")
        if self.active >= self.settings.monday_read_concurrency:
            raise HTTPException(429, "monday_source_capacity_reached", headers={"Retry-After": "1"})
        self.active += 1
        try:
            async with asyncio.timeout(self.settings.monday_read_timeout_seconds):
                scope = await self._read("scope", {"ids": body.item_ids})
                allowed = self._items(scope, body)
                # Never send ids=[]: the provider may interpret it as an unfiltered query.
                if not allowed:
                    return [], [], body.item_ids, body.column_ids, []
                variables = {"ids": [item["id"] for item in allowed],
                             "boards": [BOARDS[body.board]], "columns": body.column_ids}
                data = await self._read("evidence", variables)
                items = self._items(data, body)
                boards = data.get("boards")
                if (not isinstance(boards, list) or len(boards) != 1 or not isinstance(boards[0], dict)
                        or boards[0].get("id") != BOARDS[body.board]):
                    raise HTTPException(503, "monday_source_unavailable")
                definitions = boards[0].get("columns")
                if not isinstance(definitions, list) or any(not isinstance(c, dict) or c.get("id") not in body.column_ids for c in definitions):
                    raise HTTPException(503, "monday_source_unavailable")
                limitations = []
                for item in items:
                    values = item.get("column_values")
                    if not isinstance(values, list) or any(not isinstance(c, dict) or c.get("id") not in body.column_ids for c in values):
                        raise HTTPException(503, "monday_source_unavailable")
                    item["missing_column_ids"] = sorted(set(body.column_ids) - {c["id"] for c in values})
                    for value in values:
                        # Links are evidence, not a traversal permission or a total.
                        if "mirrored_items" in value:
                            links = value["mirrored_items"]
                            if not isinstance(links, list) or any(not isinstance(link, dict) for link in links):
                                raise HTTPException(503, "monday_source_unavailable")
                            value["mirrored_items"] = [link for link in links if link.get("linked_board_id") in BOARDS.values()]
                            if len(value["mirrored_items"]) != len(links):
                                limitations.append("Mirror links outside approved boards were omitted.")
                        if value.get("__typename") == "FormulaValue" and not value.get("display_value"):
                            limitations.append("An empty formula display is unavailable evidence, not numeric zero.")
                missing_items = sorted(set(body.item_ids) - {item["id"] for item in items})
                missing_columns = sorted(set(body.column_ids) - {column["id"] for column in definitions})
                return definitions, items, missing_items, missing_columns, limitations
        except TimeoutError:
            raise HTTPException(504, "monday_source_timeout") from None
        finally:
            self.active -= 1


class SourceCheckService:
    def __init__(self, store, compiler, reader):
        self.store, self.compiler, self.reader = store, compiler, reader

    async def check(self, actor, run_id: UUID, body: SourceCheckRequest, request: Request | None = None):
        # Ownership/version checks run before any external request, even for an
        # uncertified metric. Source inspection does not complete or modify a run.
        state = await self.store.run(actor, run_id)
        if state["status"] not in {"registered", "running", "awaiting_clarification", "completed"}:
            raise HTTPException(409, "run_not_inspectable")
        self.compiler.metric(body.metric_id, body.metric_version, body.population)
        task = asyncio.create_task(self.reader.lookup(body))
        try:
            while not task.done():
                await asyncio.wait({task}, timeout=0.2)
                state = await self.store.run(actor, run_id)
                if state["status"] not in {"registered", "running", "awaiting_clarification", "completed"}:
                    raise HTTPException(409, "run_not_inspectable")
                if request is not None and await request.is_disconnected():
                    raise HTTPException(409, "request_disconnected")
            columns, items, missing_items, missing_columns, limitations = await task
            async with self.store.scoped(actor):
                pass  # Recheck current permissions after the network read; no DB lease during HTTP.
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return SourceEvidence(id=uuid4(), run_id=run_id, permissions_version=actor.permissions_version,
            request=body, status="partial" if missing_items or missing_columns or limitations or any(
                item["missing_column_ids"] for item in items) else "observed",
            observed_at=datetime.now(timezone.utc), api_version=self.reader.settings.monday_read_api_version,
            query_reference=sha256((SCOPE_QUERY + EVIDENCE_QUERY + body.model_dump_json()).encode()).hexdigest(),
            metric_acceptance="owner_accepted" if self.compiler.catalogue.owner_accepted else "not_accepted",
            board_id=BOARDS[body.board], columns=columns, items=items,
            missing_item_ids=missing_items, missing_column_ids=missing_columns,
            limitations=list(dict.fromkeys(limitations)) + [
                "Live source evidence covers only the requested IDs and is not a population total or frozen dataset.",
                "Missing, inaccessible and deleted items cannot be distinguished from this response alone.",
                "Mirror display text is not a numeric total; formula displays may be unsupported or unavailable.",
                "This observation does not change metric certification or database freshness.",
            ])
