"""Lecture des événements d'Asset qui ont déclenché un DAG."""
from __future__ import annotations


def dates_from_events(triggering_asset_events, extra_key: str) -> list[str]:
    """Dates transportées dans le champ ``extra`` des événements (str ou liste de str)."""
    dates: set[str] = set()
    for asset in triggering_asset_events or {}:
        for event in triggering_asset_events[asset]:
            value = (event.extra or {}).get(extra_key)
            if isinstance(value, str):
                dates.add(value)
            elif value:
                dates.update(value)
    return sorted(dates)
