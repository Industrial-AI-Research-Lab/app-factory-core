from __future__ import annotations

from typing import Any, Dict, Tuple


def dockerfile_for_stack(
    *,
    stack: str,
    details: Dict[str, Any],
    port: int = 8000,
) -> Tuple[str, Dict[str, str]]:
    """Return (dockerfile_text, extra_files).

    extra_files is a mapping of relative path -> file content to be written into the build context.
    """
    s = (stack or "static").lower()

    if s == "react":
        build_dir = (details.get("build_dir") or "dist").strip() or "dist"
        install_cmd = _shell_cmd(details.get("install_cmd") or ["sh", "-lc", "npm ci || npm install"])
        build_cmd = _shell_cmd(details.get("build_cmd") or ["sh", "-lc", "npm run build"])

        nginx_conf = _nginx_conf(port)
        dockerfile = f"""FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN {install_cmd}
COPY . .
RUN {build_cmd}

FROM nginx:1.25-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/{build_dir} /usr/share/nginx/html
EXPOSE {port}
CMD [\"nginx\", \"-g\", \"daemon off;\"]
"""
        return dockerfile, {"nginx.conf": nginx_conf}

    if s == "node":
        install_cmd = _shell_cmd(details.get("install_cmd") or ["sh", "-lc", "npm ci || npm install"])
        start_cmd = _shell_cmd(details.get("start_cmd") or ["sh", "-lc", "npm start"])
        dockerfile = f"""FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN {install_cmd}
COPY . .
ENV PORT={port}
EXPOSE {port}
CMD {start_cmd}
"""
        return dockerfile, {}

    if s == "python":
        entry = (details.get("entry") or "main.py").strip() or "main.py"
        framework = (details.get("framework") or "python").lower()
        if framework == "fastapi":
            module = entry[:-3] if entry.endswith(".py") else entry
            cmd = f"python -m uvicorn {module}:app --host 0.0.0.0 --port {port}"
        elif framework == "django":
            cmd = f"python {entry} runserver 0.0.0.0:{port}"
        elif framework == "flask":
            cmd = f"python {entry}"
        else:
            cmd = f"python {entry}"

        dockerfile = f"""FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE {port}
CMD [\"sh\", \"-lc\", \"{cmd}\"]
"""
        return dockerfile, {}

    # static fallback
    dockerfile = f"""FROM python:3.11-slim
WORKDIR /app
COPY . .
EXPOSE {port}
CMD [\"python\", \"-m\", \"http.server\", \"{port}\"]
"""
    return dockerfile, {}


def _nginx_conf(port: int) -> str:
    return (
        "server {\n"
        f"    listen {int(port)};\n"
        "    server_name _;\n"
        "    root /usr/share/nginx/html;\n"
        "    index index.html;\n"
        "    location / {\n"
        "        try_files $uri $uri/ /index.html;\n"
        "    }\n"
        "}\n"
    )


def _shell_cmd(cmd: Any) -> str:
    """Convert a JSON-style command array to a shell string for Dockerfile RUN.

    For RUN we need a shell expression. This helper tolerates already-string inputs.
    """
    if isinstance(cmd, str):
        return cmd
    if isinstance(cmd, list) and cmd:
        # If it's like ["sh","-lc","..."] extract last part.
        if len(cmd) >= 3 and str(cmd[0]).lower() in {"sh", "bash"} and str(cmd[1]) in {"-lc", "-c"}:
            return str(cmd[2])
        return " ".join(str(x) for x in cmd)
    return "true"
