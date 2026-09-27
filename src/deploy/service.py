"""
DeployService - Main deployment service coordinator.

Delegates to specialized modules for local/prod deployments and health checks.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
import logging

from context.shared_context import SharedContext
from deploy.local_deploy import (
    deploy_static_demo_local as _deploy_local,
    teardown_deployment_local as _teardown_local,
)
from deploy.prod_deploy import (
    deploy_static_demo_prod as _deploy_prod,
    deploy_from_artifacts_prod as _deploy_artifacts_prod,
    teardown_deployment_prod as _teardown_prod,
)
from deploy.build_helpers import (
    build_and_push_static_demo_prod,
    build_and_push_from_artifacts,
)
from deploy.health_checks import (
    run_health_check_with_retry,
    run_external_url_health_check,
)

from deploy.prod_redeploy import redeploy_image_prod
from storage.artifact_store import ArtifactStore


logger = logging.getLogger(__name__)


class DeployService:
    """Main deployment service that coordinates local and prod deployments."""
    
    def __init__(self, storage_backend, event_emitter, container_manager):
        self.storage = storage_backend
        self.event_emitter = event_emitter
        self.container_manager = container_manager

    # -------------------------------------------------------------------------
    # Local deployment methods
    # -------------------------------------------------------------------------

    async def deploy_static_demo_local(
        self,
        project_id: str,
        shared_context: SharedContext,
        deploy_slug: Optional[str] = None,
        target_namespace: Optional[str] = None,
        cluster_target: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Deploy a trivial static app for a project in local dev mode."""
        return await _deploy_local(
            project_id=project_id,
            shared_context=shared_context,
            event_emitter=self.event_emitter,
            container_manager=self.container_manager,
            deploy_slug=deploy_slug,
            target_namespace=target_namespace,
            cluster_target=cluster_target,
        )

    async def teardown_deployment_local(
        self,
        project_id: str,
        shared_context: SharedContext,
        deployment_id: str,
    ) -> Dict[str, Any]:
        """Teardown a previously created local static demo deployment."""
        return await _teardown_local(
            project_id=project_id,
            shared_context=shared_context,
            event_emitter=self.event_emitter,
            deployment_id=deployment_id,
        )

    # -------------------------------------------------------------------------
    # Prod deployment methods
    # -------------------------------------------------------------------------

    async def deploy_static_demo_prod(
        self,
        project_id: str,
        shared_context: SharedContext,
        deploy_slug: Optional[str] = None,
        target_namespace: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Deploy a trivial static app for a project in a prod-capable cluster."""
        return await _deploy_prod(
            project_id=project_id,
            shared_context=shared_context,
            event_emitter=self.event_emitter,
            container_manager=self.container_manager,
            deploy_slug=deploy_slug,
            target_namespace=target_namespace,
        )

    async def deploy_from_artifacts_prod(
        self,
        project_id: str,
        shared_context: SharedContext,
        deploy_slug: Optional[str] = None,
        target_namespace: Optional[str] = None,
        deploy_spec: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Deploy a project based on its artifacts into a prod-capable cluster."""
        return await _deploy_artifacts_prod(
            project_id=project_id,
            shared_context=shared_context,
            event_emitter=self.event_emitter,
            container_manager=self.container_manager,
            deploy_slug=deploy_slug,
            target_namespace=target_namespace,
            deploy_spec=deploy_spec,
            storage=self.storage,
        )

    async def teardown_deployment_prod(
        self,
        project_id: str,
        shared_context: SharedContext,
        deployment_id: str,
    ) -> Dict[str, Any]:
        """Teardown a previously created prod static demo deployment."""
        return await _teardown_prod(
            project_id=project_id,
            shared_context=shared_context,
            event_emitter=self.event_emitter,
            deployment_id=deployment_id,
        )

    # -------------------------------------------------------------------------
    # Build helpers (exposed for direct use if needed)
    # -------------------------------------------------------------------------

    async def build_and_push_static_demo_prod(
        self,
        project_id: str,
        base_slug: Optional[str],
        deployment_id: str,
    ) -> Dict[str, Any]:
        """Build and push a trivial static app image to the prod registry."""
        return await build_and_push_static_demo_prod(
            project_id=project_id,
            base_slug=base_slug,
            deployment_id=deployment_id,
            container_manager=self.container_manager,
        )

    async def build_and_push_from_artifacts(
        self,
        project_id: str,
        deployment_id: str,
    ) -> Dict[str, Any]:
        """Build and push an image from persisted project artifacts."""
        return await build_and_push_from_artifacts(
            project_id=project_id,
            deployment_id=deployment_id,
            container_manager=self.container_manager,
            storage=self.storage,
        )

    # -------------------------------------------------------------------------
    # Health check helpers (exposed for direct use if needed)
    # -------------------------------------------------------------------------

    async def run_health_check(
        self,
        namespace: str,
        service_name: str,
        port: int = 8000,
        max_retries: int = 3,
        retry_delay: float = 5.0,
        pod_ready_timeout: float = 30.0,
    ) -> Dict[str, Any]:
        """Run in-cluster health check with pod readiness wait and retry logic."""
        return await run_health_check_with_retry(
            namespace=namespace,
            service_name=service_name,
            port=port,
            max_retries=max_retries,
            retry_delay=retry_delay,
            pod_ready_timeout=pod_ready_timeout,
        )

    async def run_external_health_check(
        self,
        url: str,
        timeout: float = 10.0,
        verify_ssl: bool = False,
    ) -> Dict[str, Any]:
        """Run an external HTTP health check against a public URL."""
        return await run_external_url_health_check(
            url=url,
            timeout=timeout,
            verify_ssl=verify_ssl,
        )

    # -------------------------------------------------------------------------
    # Deployment Recovery (Bulletproof Persistence 2.3)
    # -------------------------------------------------------------------------

    async def check_deployment_health(
        self,
        project_id: str,
        shared_context: SharedContext,
    ) -> Dict[str, Any]:
        """
        Check health of a project's most recent deployment.
        
        Returns:
            dict with:
                - deployment_id: ID of the checked deployment (or None)
                - status: "healthy", "unhealthy", "unknown", "no_deployment"
                - details: Human-readable details
                - recoverable: Whether the deployment can be recovered
                - ingress_host: The deployment URL if available
        """
        deployments = shared_context.get("deployments", [])
        if not deployments:
            return {
                "deployment_id": None,
                "status": "no_deployment",
                "details": "No deployments found for this project",
                "recoverable": False,
                "ingress_host": None,
            }
        
        # Get most recent deployment
        latest = deployments[-1] if isinstance(deployments, list) else None
        if not latest or not isinstance(latest, dict):
            return {
                "deployment_id": None,
                "status": "no_deployment",
                "details": "No valid deployment found",
                "recoverable": False,
                "ingress_host": None,
            }
        
        deployment_id = latest.get("deployment_id")
        ingress_host = latest.get("ingress_host")
        namespace = latest.get("namespace", "AppFactory-apps")
        service_name = latest.get("service_name")
        port = latest.get("port", 8000)
        
        # Check if we have enough info to run health check
        if not service_name:
            return {
                "deployment_id": deployment_id,
                "status": "unknown",
                "details": "Missing service_name in deployment record",
                "recoverable": True,
                "ingress_host": ingress_host,
            }
        
        # Run health check
        try:
            health = await run_health_check_with_retry(
                namespace=namespace,
                service_name=service_name,
                port=port,
                max_retries=2,
                retry_delay=2.0,
                pod_ready_timeout=10.0,
            )
            
            return {
                "deployment_id": deployment_id,
                "status": health.get("status", "unknown"),
                "details": health.get("details", ""),
                "recoverable": health.get("status") != "healthy",
                "ingress_host": ingress_host,
            }
        except Exception as e:
            return {
                "deployment_id": deployment_id,
                "status": "unknown",
                "details": f"Health check failed: {e}",
                "recoverable": True,
                "ingress_host": ingress_host,
            }

    async def recover_deployment(
        self,
        project_id: str,
        shared_context: SharedContext,
        deploy_slug: Optional[str] = None,
        target_namespace: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Recover a failed deployment by redeploying from stored artifacts.
        
        This is the "one-click redeploy" functionality for Bulletproof Persistence.
        
        Args:
            project_id: Project to recover
            shared_context: Project's shared context with artifacts
            deploy_slug: Optional custom slug for the new deployment
            target_namespace: Optional namespace override
            
        Returns:
            dict with new deployment result or error
        """
        # Check if we have artifacts to deploy
        artifacts = []
        try:
            artifact_store = ArtifactStore(self.storage)
            await artifact_store.initialize()
            if artifact_store.collection is None:
                logger.warning(
                    "[DEPLOY] [RECOVER] project_id=%s artifact_store_ready=false - ArtifactStore collection unavailable for recovery",
                    project_id,
                )
            else:
                artifacts = await artifact_store.get_all_files(project_id)
        except Exception as exc:
            logger.warning(
                "[DEPLOY] [RECOVER] project_id=%s storage_init_failed=true error=%s - failed to initialize ArtifactStore for recovery",
                project_id,
                exc,
            )
            return {
                "status": "failed",
                "error": "Artifact storage unavailable for recovery",
                "deployment_id": None,
            }

        if not artifacts:
            logger.warning(
                "[DEPLOY] [RECOVER] project_id=%s artifacts=0 - no file_artifacts available for recovery",
                project_id,
            )
            return {
                "status": "failed",
                "error": "No artifacts available for recovery",
                "deployment_id": None,
            }

        logger.info(
            "[DEPLOY] [RECOVER] project_id=%s artifacts=%d - recovering deployment from ArtifactStore",
            project_id,
            len(artifacts),
        )
        
        # Attempt to redeploy using existing artifact deployment flow
        try:
            result = await self.deploy_from_artifacts_prod(
                project_id=project_id,
                shared_context=shared_context,
                deploy_slug=deploy_slug,
                target_namespace=target_namespace,
            )
            
            if result.get("status") == "success":
                # Emit recovery event. Deployment is project-scoped (one
                # deploy per project, persistent across runs) — run_id=None.
                await self.event_emitter.emit("deployment_recovered", getattr(shared_context, "run_id", None), {
                    "project_id": project_id,
                    "deployment_id": result.get("deployment_id"),
                    "ingress_host": result.get("ingress_host"),
                })
            
            return result
            
        except Exception as e:
            return {
                "status": "failed",
                "error": f"Recovery failed: {e}",
                "deployment_id": None,
            }

    # -------------------------------------------------------------------------
    # Deploy Retry/Redeploy/Rollback (TASKS 3.3)
    # -------------------------------------------------------------------------

    async def retry_deployment_prod(
        self,
        *,
        project_id: str,
        shared_context: SharedContext,
        deployment_id: str,
    ) -> Dict[str, Any]:
        deployments = shared_context.get("deployments", []) or []
        target = next((d for d in deployments if isinstance(d, dict) and d.get("deployment_id") == deployment_id), None)
        if not target:
            raise ValueError("Deployment not found")

        image_ref = target.get("image_ref")
        namespace = target.get("namespace") or "AppFactory-apps"
        service_name = target.get("service_name")
        port = int(target.get("port") or 8000)

        if not image_ref or not service_name:
            raise ValueError("Deployment missing image_ref or service_name")

        return await redeploy_image_prod(
            project_id=project_id,
            shared_context=shared_context,
            event_emitter=self.event_emitter,
            namespace=namespace,
            service_name=service_name,
            image_ref=image_ref,
            port=port,
            retry_of_deployment_id=deployment_id,
        )

    async def rollback_deployment_prod(
        self,
        *,
        project_id: str,
        shared_context: SharedContext,
        target_deployment_id: str,
    ) -> Dict[str, Any]:
        deployments = shared_context.get("deployments", []) or []
        target = next((d for d in deployments if isinstance(d, dict) and d.get("deployment_id") == target_deployment_id), None)
        if not target:
            raise ValueError("Deployment not found")

        image_ref = target.get("image_ref")
        namespace = target.get("namespace") or "AppFactory-apps"
        service_name = target.get("service_name")
        port = int(target.get("port") or 8000)

        if not image_ref or not service_name:
            raise ValueError("Deployment missing image_ref or service_name")

        return await redeploy_image_prod(
            project_id=project_id,
            shared_context=shared_context,
            event_emitter=self.event_emitter,
            namespace=namespace,
            service_name=service_name,
            image_ref=image_ref,
            port=port,
            rollback_of_deployment_id=target_deployment_id,
        )
