from typing import Any, Dict, Optional


def normalize_run_config(run_config: Optional[Any]) -> Dict[str, Any]:
    """Normalize run configuration objects to plain dictionaries."""
    if not run_config:
        return {}
    if isinstance(run_config, dict):
        return run_config
    if hasattr(run_config, "model_dump"):
        return run_config.model_dump(by_alias=False)
    return dict(run_config)
