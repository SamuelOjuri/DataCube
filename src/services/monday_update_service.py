import json
import logging
from typing import Any, Dict, Optional

from ..config import (
    PARENT_BOARD_ID,
    PARENT_COLUMN_GESTATION_DAYS,
    PARENT_COLUMN_EXPECTED_CONVERSION,
    PARENT_COLUMN_PROJECT_RATING,
)
from ..database.supabase_client import SupabaseClient
from ..core.monday_client import MondayClient

logger = logging.getLogger(__name__)


class MondayUpdateService:
    """Push analysis results from Supabase into Monday board columns."""

    def __init__(
        self,
        db_client: Optional[SupabaseClient] = None,
        monday_client: Optional[MondayClient] = None,
        board_id: Optional[str] = None,
    ):
        self.db = db_client or SupabaseClient()
        self.monday = monday_client or MondayClient()
        self.board_id = board_id or PARENT_BOARD_ID

    def sync_project(
        self,
        project_id: str,
        analysis: Optional[Dict[str, Any]] = None,
        include_update: bool = True,
        throttle: bool = True,
    ) -> Dict[str, Any]:
        """
        Ensure Monday columns reflect the latest numeric analysis for the item.
        """
        project_id = str(project_id)
        payload = analysis or self.db.get_latest_analysis_result(project_id)
        if not payload:
            logger.warning("No analysis payload found for project %s", project_id)
            return {"success": False, "error": "analysis_not_found"}

        column_values = self._build_column_values(payload)
        if not column_values:
            logger.warning("Analysis payload for %s lacked numeric predictions", project_id)
            return {"success": False, "error": "invalid_payload"}

        try:
            columns_res = self.monday.update_item_columns(
                self.board_id,
                project_id,
                column_values,
                throttle=throttle,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Monday column update failed for %s: %s", project_id, exc)
            return {"success": False, "error": "column_update_failed", "detail": str(exc)}

        update_result: Optional[Dict[str, Any]] = None
        update_action: Optional[str] = None
        if include_update:
            body = self._format_update_body(payload)
            if body:
                existing_update_id = self._find_existing_update_id(project_id)
                try:
                    if existing_update_id:
                        update_result = self.monday.edit_item_update(
                            existing_update_id,
                            body,
                            throttle=throttle,
                        )
                        update_action = "edit"
                    else:
                        update_result = self.monday.create_item_update(
                            project_id,
                            body,
                            throttle=throttle,
                        )
                        update_action = "create"
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Unable to sync Monday update for %s: %s", project_id, exc)

        return {
            "success": True,
            "columns": columns_res,
            "update": update_result,
            "update_action": update_action,
        }

    def _to_int(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            try:
                return int(float(value))
            except (ValueError, TypeError):
                return None

    def _to_float(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _build_column_values(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        vals: Dict[str, Any] = {}

        gestation = self._to_int(analysis.get("expected_gestation_days"))
        if gestation is not None:
            vals[PARENT_COLUMN_GESTATION_DAYS] = str(gestation)

        conversion = self._to_float(analysis.get("expected_conversion_rate"))
        if conversion is not None:
            percent = round(conversion * 100.0, 2)
            vals[PARENT_COLUMN_EXPECTED_CONVERSION] = f"{percent:.2f}"

        rating = self._to_int(analysis.get("rating_score"))
        if rating is not None:
            vals[PARENT_COLUMN_PROJECT_RATING] = str(rating)

        return vals

    def _format_update_body(self, analysis: Dict[str, Any]) -> Optional[str]:
        html = [
            "<strong>Datacube Analysis Update</strong>",
            "<ul>",
        ]

        def conf_fmt(value: Optional[float]) -> str:
            if isinstance(value, (int, float)):
                return f" ({value:.0%} confidence)"
            return ""

        gest = analysis.get("expected_gestation_days")
        if gest is not None:
            html.append(
                f"<li>Expected gestation: <strong>{int(gest)} days</strong>{conf_fmt(analysis.get('gestation_confidence'))}</li>"
            )

        conv = analysis.get("expected_conversion_rate")
        if conv is not None:
            pct = round(float(conv) * 100.0, 1)
            html.append(
                f"<li>Expected conversion: <strong>{pct:.1f}%</strong>{conf_fmt(analysis.get('conversion_confidence'))}</li>"
            )

        rating = analysis.get("rating_score")
        if rating is not None:
            html.append(f"<li>Project rating: <strong>{int(rating)}/100</strong></li>")

        html.append("</ul>")

        reasoning = analysis.get("reasoning")
        if isinstance(reasoning, dict):
            def add_para(label: str, text: Optional[str]):
                if isinstance(text, str) and text.strip():
                    html.append(f"<p><em>{label}:</em> {text.strip()}</p>")

            add_para("Gestation rationale", reasoning.get("gestation"))
            add_para("Conversion rationale", reasoning.get("conversion"))
            add_para("Rating rationale", reasoning.get("rating"))

            cluster = reasoning.get("cluster")
            if cluster:
                html.append(f"<p><em>Cluster:</em> <code>{json.dumps(cluster, ensure_ascii=False)}</code></p>")

            metrics = reasoning.get("metrics_used") or reasoning.get("metrics")
            if metrics:
                html.append("<p><em>Metrics used:</em></p><ul>")
                for key, value in metrics.items():
                    html.append(f"<li>{key}: {value}</li>")
                html.append("</ul>")

        body = "".join(html).strip()
        return body or None

    def _find_existing_update_id(self, item_id: str) -> Optional[str]:
        """
        Return the ID of the most recent Datacube update for this item, if any.
        """
        try:
            updates = self.monday.get_item_updates(item_id, limit=10)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to inspect updates for item %s: %s", item_id, exc)
            return None

        for update in updates:
            text = (update.get("text_body") or update.get("body") or "").strip()
            normalized = text.lower()
            if "datacube analysis update" in normalized:
                return update.get("id")
        return None