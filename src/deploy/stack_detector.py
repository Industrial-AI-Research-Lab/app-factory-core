from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import json


@dataclass(frozen=True)
class StackDetection:
    stack: str  # static | react | node | python
    details: Dict[str, Any]


def detect_stack(file_artifacts: List[Dict[str, Any]]) -> StackDetection:
    files = {(_norm_path(a.get("path") or "")): (a.get("content") or "") for a in file_artifacts if isinstance(a, dict)}

    pkg_txt = files.get("package.json")
    if isinstance(pkg_txt, str) and pkg_txt.strip():
        pkg = _parse_json(pkg_txt)
        if isinstance(pkg, dict):
            deps = _flatten_deps(pkg)
            scripts = pkg.get("scripts") if isinstance(pkg.get("scripts"), dict) else {}

            if _has_any(deps, {"react", "react-dom"}):
                build_dir = _react_build_dir(deps, scripts)
                install_cmd = _node_install_cmd(files)
                build_cmd = _node_build_cmd(scripts)
                return StackDetection(
                    stack="react",
                    details={
                        "install_cmd": install_cmd,
                        "build_cmd": build_cmd,
                        "build_dir": build_dir,
                    },
                )

            if _has_any(deps, {"express", "fastify", "koa", "hono"}):
                start_cmd = _node_start_cmd(files, scripts)
                install_cmd = _node_install_cmd(files)
                return StackDetection(
                    stack="node",
                    details={
                        "install_cmd": install_cmd,
                        "start_cmd": start_cmd,
                    },
                )

    # Python signals
    if "requirements.txt" in files and ("main.py" in files or "app.py" in files or "wsgi.py" in files):
        entry = "main.py" if "main.py" in files else ("app.py" if "app.py" in files else "wsgi.py")
        content = files.get("requirements.txt") or ""
        framework = _python_framework(content)
        return StackDetection(stack="python", details={"entry": entry, "framework": framework})

    return StackDetection(stack="static", details={})


def _python_framework(requirements_txt: str) -> str:
    txt = requirements_txt.lower()
    if "fastapi" in txt or "uvicorn" in txt:
        return "fastapi"
    if "flask" in txt:
        return "flask"
    if "django" in txt:
        return "django"
    return "python"


def _node_install_cmd(files: Dict[str, str]) -> List[str]:
    if "package-lock.json" in files:
        return ["sh", "-lc", "npm ci"]
    if "pnpm-lock.yaml" in files:
        return ["sh", "-lc", "corepack enable && pnpm install --frozen-lockfile || pnpm install"]
    if "yarn.lock" in files:
        return ["sh", "-lc", "corepack enable && yarn install --frozen-lockfile || yarn install"]
    return ["sh", "-lc", "npm install"]


def _node_build_cmd(scripts: Dict[str, Any]) -> List[str]:
    if isinstance(scripts, dict) and isinstance(scripts.get("build"), str):
        return ["sh", "-lc", "npm run build"]
    return ["sh", "-lc", "npm run build || true"]


def _node_start_cmd(files: Dict[str, str], scripts: Dict[str, Any]) -> List[str]:
    if isinstance(scripts, dict) and isinstance(scripts.get("start"), str):
        return ["sh", "-lc", "npm start"]
    for candidate in ("server.js", "index.js", "app.js"):
        if candidate in files:
            return ["sh", "-lc", f"node {candidate}"]
    return ["sh", "-lc", "node server.js"]


def _react_build_dir(deps: Dict[str, str], scripts: Dict[str, Any]) -> str:
    # Vite defaults to dist; CRA defaults to build.
    if _has_any(deps, {"vite"}):
        return "dist"
    if _has_any(deps, {"react-scripts"}):
        return "build"
    # If script hints at next, prefer next (but we don't special-case next yet).
    if isinstance(scripts, dict):
        build = scripts.get("build")
        if isinstance(build, str) and "next" in build:
            return ".next"
    return "dist"


def _flatten_deps(pkg: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k in ("dependencies", "devDependencies", "peerDependencies"):
        v = pkg.get(k)
        if isinstance(v, dict):
            for name, ver in v.items():
                if isinstance(name, str):
                    out[name.lower()] = str(ver)
    return out


def _has_any(deps: Dict[str, str], names: set[str]) -> bool:
    return any(n.lower() in deps for n in names)


def _parse_json(txt: str) -> Optional[Any]:
    try:
        return json.loads(txt)
    except Exception:
        return None


def _norm_path(p: str) -> str:
    p = (p or "").replace("\\", "/").lstrip("./")
    return p
