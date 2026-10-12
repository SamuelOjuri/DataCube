"""Natural-language answers when the data cannot answer a question. Lead text is server-owned;
model wording is accepted only when it states no figures beyond dates and small counts."""
import re

from .contracts import ExplanationAnswer

TITLES = {
    "forecast": "Forecasts aren't available",
    "data_unavailable": "The data for this isn't available",
    "not_analytical": "This is outside the business data",
    "unsafe": "I can't do that",
    "query_failed": "I couldn't answer this reliably",
    "unsupported": "This needs a report that isn't enabled yet",
}
LEADS = {
    "forecast": "I report recorded results only, so I can't forecast or predict future values.",
    "data_unavailable": "The data needed to answer this isn't recorded in the business data I can read.",
    "not_analytical": ("I answer questions about enquiries, orders, invoices, bookings, conversion and project "
                       "timing. This question isn't about that data."),
    "unsafe": "I can only read business data to answer analytical questions, so I can't do that.",
    "query_failed": ("I couldn't build a reliable query for this question within the time and safety limits. "
                     "Try asking for one measure and a specific period."),
    "unsupported": "This question needs analysis beyond the certified reports currently enabled.",
}
# Known-good certified questions, matching the workspace's starting suggestions.
CERTIFIED_SUGGESTIONS = ["Show revenue for the last completed month.", "Show stored order value by category.",
                         "What is the five-year conversion rate?"]
NUMBER = re.compile(r"\d[\d,.]*")
PERMITTED_NUMBER = re.compile(r"(?:19|20)\d{2}|\d{1,2}")
# Money, rates, markup, links and database identifiers such as invoice_reporting_facts_v1.
FORBIDDEN = re.compile(r"[£$€%<>{}]|\bGBP\b|https?://|www\.|\w+_\w+", re.IGNORECASE)


def narrative(text, limit=400) -> str | None:
    """Model wording without amounts, rates or links; None means use server text only."""
    text = " ".join(str(text or "").split())
    if not text or len(text) > limit or FORBIDDEN.search(text):
        return None
    # The model never sees result data, so any other figure would be invented.
    if any(not PERMITTED_NUMBER.fullmatch(token.rstrip(".,")) for token in NUMBER.findall(text)):
        return None
    return text


def explain(reason, *, detail="", suggestions=(), notices=()) -> ExplanationAnswer:
    lines = [LEADS[reason]]
    # Unsafe requests may carry injected text; never repeat model wording for them.
    if reason != "unsafe" and (checked := narrative(detail, 500)):
        lines.append(checked)
    ideas = [] if reason == "unsafe" else [s for s in (narrative(x, 200) for x in suggestions) if s]
    if reason == "unsupported" and not ideas:
        ideas = CERTIFIED_SUGGESTIONS
    return ExplanationAnswer(kind="explanation", reason=reason, title=TITLES[reason], text="\n".join(lines),
                             suggestions=list(dict.fromkeys(ideas))[:3], notices=list(notices))
