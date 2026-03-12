"""Indicator registry: loads and validates indicators from registry.yaml."""

from pathlib import Path

import yaml

from macro_tracker.schema import IndicatorSpec


def load_registry(path: Path | str | None = None) -> list[IndicatorSpec]:
    """Load and validate all indicators from the YAML registry file."""
    if path is None:
        from macro_tracker.config import REGISTRY_PATH

        path = REGISTRY_PATH

    path = Path(path)
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not raw or "indicators" not in raw:
        raise ValueError(f"Invalid registry file: missing 'indicators' key in {path}")

    indicators = []
    seen_ids: set[str] = set()
    for entry in raw["indicators"]:
        spec = IndicatorSpec(**entry)
        if spec.id in seen_ids:
            raise ValueError(f"Duplicate indicator id: {spec.id}")
        seen_ids.add(spec.id)
        indicators.append(spec)

    return indicators


def get_indicators_by_source(
    indicators: list[IndicatorSpec], source: str
) -> list[IndicatorSpec]:
    """Filter indicators by source name."""
    return [i for i in indicators if i.source.value == source]


def get_indicators_by_category(
    indicators: list[IndicatorSpec], category: str
) -> list[IndicatorSpec]:
    """Filter indicators by category."""
    return [i for i in indicators if i.category.value == category]
