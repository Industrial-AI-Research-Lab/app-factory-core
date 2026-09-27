from __future__ import annotations

from typing import Any, Dict, Optional


def normalize_deploy_spec(spec: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(spec, dict):
        return None

    stack = spec.get("stack")
    if isinstance(stack, str):
        stack = stack.strip().lower()
    if stack not in {"static", "react", "node", "python"}:
        stack = None

    port = spec.get("port")
    try:
        port_i = int(port)
    except Exception:
        port_i = 8000

    dockerfile = spec.get("dockerfile")
    if not isinstance(dockerfile, str) or not dockerfile.strip():
        dockerfile = None

    extra_files = spec.get("extra_files")
    if not isinstance(extra_files, dict):
        extra_files = None
    else:
        cleaned: Dict[str, str] = {}
        for k, v in extra_files.items():
            if not isinstance(k, str) or not isinstance(v, str):
                continue
            kk = k.replace("\\", "/").lstrip("./")
            if not kk:
                continue
            cleaned[kk] = v
        extra_files = cleaned

    details = spec.get("details")
    if not isinstance(details, dict):
        details = {}

    return {
        "stack": stack,
        "port": port_i,
        "dockerfile": dockerfile,
        "extra_files": extra_files,
        "details": details,
    }
