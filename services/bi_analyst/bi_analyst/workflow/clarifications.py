"""Resolve offered choices without interpreting arbitrary reply text as parameters."""
from .contracts import ClarificationResponse


def match_option(options: list[str], answer: str) -> str | None:
    answer = answer.strip()
    exact = list(dict.fromkeys(option for option in options if option == answer))
    if len(exact) == 1:
        return exact[0]
    matches = list(dict.fromkeys(option for option in options if option.casefold() == answer.casefold()))
    return matches[0] if len(matches) == 1 else None


def selected_scope(responses: list[ClarificationResponse], permitted: dict) -> dict:
    selected = {}
    for response in responses:
        pending = response.clarification
        option = match_option(pending.options, response.answer)
        if option is None:
            # Free text may revise an earlier choice. Keep its history and working
            # plan, but let the planner interpret the revision with the full catalogue.
            selected.clear()
        elif pending.reason == 'metric':
            selected.pop('metric_id', None)
            matches = [m.id for m in permitted.values() if f'{m.label} ({m.id})' == option]
            if len(matches) == 1:
                selected['metric_id'] = matches[0]
                if selected.get('period') not in permitted[matches[0]].periods:
                    selected.pop('period', None)
        elif pending.reason == 'period':
            selected.pop('period', None)
            if any(option in m.periods for m in permitted.values()):
                selected['period'] = option
    return selected
