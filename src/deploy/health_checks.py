"""Health check utilities for deployments."""

from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime
import asyncio

from sandbox.host_cli import run_host_cli


async def run_health_check_with_retry(
    namespace: str,
    service_name: str,
    port: int = 8000,
    max_retries: int = 3,
    retry_delay: float = 5.0,
    pod_ready_timeout: float = 30.0,
) -> Dict[str, Any]:
    """Run in-cluster health check with pod readiness wait and retry logic.

    Returns:
        dict with status ("healthy", "unhealthy", "unknown"), checked_at, details
    """
    health_status = "unknown"
    health_details = ""
    checked_at: Optional[str] = None

    try:
        # 1) Wait for pod to be ready before running health check
        wait_cmd = [
            "kubectl", "wait", "--for=condition=Ready",
            "pod", "-l", f"app={service_name}",
            "-n", namespace,
            f"--timeout={int(pod_ready_timeout)}s",
        ]
        try:
            wait_res = await asyncio.wait_for(
                run_host_cli(wait_cmd, cwd=None),
                timeout=pod_ready_timeout + 5,
            )
            if int(wait_res.get("exit_code", 1)) != 0:
                health_details = f"Pod not ready: {(wait_res.get('stderr') or wait_res.get('stdout') or '')[:200]}"
                return {"status": "unhealthy", "checked_at": None, "details": health_details}
        except asyncio.TimeoutError:
            health_details = f"Timed out waiting for pod to be ready ({pod_ready_timeout}s)"
            return {"status": "unknown", "checked_at": None, "details": health_details}

        # 2) Get pod name
        get_pod_cmd = [
            "kubectl", "get", "pods",
            "-n", namespace,
            "-l", f"app={service_name}",
            "-o", "jsonpath={.items[0].metadata.name}",
        ]
        pod_res = await asyncio.wait_for(
            run_host_cli(get_pod_cmd, cwd=None),
            timeout=5.0,
        )
        if int(pod_res.get("exit_code", 1)) != 0:
            health_details = f"kubectl get pods failed: {pod_res.get('stderr', '')[:200]}"
            return {"status": "unknown", "checked_at": None, "details": health_details}

        pod_name = (pod_res.get("stdout") or "").strip()
        if not pod_name:
            return {"status": "unknown", "checked_at": None, "details": "No pod name found"}

        # 3) Run health check with retries
        health_cmd = [
            "kubectl", "exec", "-n", namespace, pod_name, "--",
            "python", "-c",
            f"import urllib.request,sys; "
            f"resp=urllib.request.urlopen('http://127.0.0.1:{port}/', timeout=5); "
            f"sys.exit(0 if resp.getcode()==200 else 1)",
        ]

        last_error = ""
        for attempt in range(max_retries):
            try:
                health_res = await asyncio.wait_for(
                    run_host_cli(health_cmd, cwd=None),
                    timeout=10.0,
                )
                exit_code = int(health_res.get("exit_code", 1))
                checked_at = datetime.utcnow().isoformat()

                if exit_code == 0:
                    return {"status": "healthy", "checked_at": checked_at, "details": ""}

                last_error = (health_res.get("stderr") or health_res.get("stdout") or "")[-200:]
            except asyncio.TimeoutError:
                last_error = "health check timed out"

            # Retry after delay (except on last attempt)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)

        health_status = "unhealthy"
        health_details = f"Health check failed after {max_retries} attempts: {last_error}"
        checked_at = datetime.utcnow().isoformat()

    except Exception as e:
        health_details = f"health check exception: {e!r}"

    return {"status": health_status, "checked_at": checked_at, "details": health_details}


async def run_external_url_health_check(
    url: str,
    timeout: float = 10.0,
    verify_ssl: bool = False,
) -> Dict[str, Any]:
    """Run an external HTTP health check against a public URL.

    This exercises the full ingress/DNS/TLS path from the backend.
    Uses Python's urllib to make a GET request to the root path.

    Args:
        url: Full URL to check (e.g., "https://a87b3fc0.apps.example.com/")
        timeout: Request timeout in seconds
        verify_ssl: Whether to verify SSL certificates (False for self-signed/new certs)

    Returns:
        dict with status ("healthy", "unhealthy", "unknown"), checked_at, details
    """
    import ssl
    import urllib.request
    import urllib.error

    checked_at = datetime.utcnow().isoformat()
    try:
        # Create SSL context that optionally skips verification
        ctx = ssl.create_default_context()
        if not verify_ssl:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        req = urllib.request.Request(url, method="GET")
        req.add_header("User-Agent", "AppFactoryHealthCheck/1.0")

        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            status_code = resp.getcode()
            if status_code == 200:
                return {"status": "healthy", "checked_at": checked_at, "details": f"HTTP {status_code}"}
            else:
                return {"status": "unhealthy", "checked_at": checked_at, "details": f"HTTP {status_code}"}

    except urllib.error.HTTPError as e:
        return {"status": "unhealthy", "checked_at": checked_at, "details": f"HTTP {e.code}: {e.reason}"}
    except urllib.error.URLError as e:
        return {"status": "unhealthy", "checked_at": checked_at, "details": f"URL error: {e.reason}"}
    except TimeoutError:
        return {"status": "unknown", "checked_at": checked_at, "details": f"Timeout after {timeout}s"}
    except Exception as e:
        return {"status": "unknown", "checked_at": checked_at, "details": f"Error: {e!r}"}


def combine_health_results(
    in_cluster_health: Dict[str, Any],
    external_health: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Combine in-cluster and external health check results.
    
    Priority: both healthy -> healthy, any unhealthy -> unhealthy, else unknown
    """
    combined_status = in_cluster_health.get("status", "unknown")
    combined_details = f"in-cluster: {in_cluster_health.get('details', 'ok') or 'ok'}"
    
    if external_health:
        ext_status = external_health.get("status", "unknown")
        combined_details += f"; external: {external_health.get('details', 'ok') or 'ok'}"
        if combined_status == "healthy" and ext_status == "healthy":
            combined_status = "healthy"
        elif combined_status == "unhealthy" or ext_status == "unhealthy":
            combined_status = "unhealthy"
        elif combined_status == "unknown" or ext_status == "unknown":
            combined_status = "unknown"

    return {
        "status": combined_status,
        "checked_at": in_cluster_health.get("checked_at") or datetime.utcnow().isoformat(),
        "details": combined_details,
    }
