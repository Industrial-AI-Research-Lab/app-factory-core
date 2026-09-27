# AppFactory: optional thin image with ENTRYPOINT npx (same argv as plain npx on the host).
# Default UI preset uses public node:22-bookworm-slim + explicit "npx" in docker args instead.
# Build: docker build -t mcp-npx-runner:latest -f deploy/mcp-npx-runner/Dockerfile deploy/mcp-npx-runner
# Push:   to a registry reachable from the API host; set that tag in Tool Configurations (override image field).
FROM node:22-bookworm-slim
ENV NODE_ENV=production \
    NPM_CONFIG_UPDATE_NOTIFIER=false
WORKDIR /app
ENTRYPOINT ["npx"]
CMD []
