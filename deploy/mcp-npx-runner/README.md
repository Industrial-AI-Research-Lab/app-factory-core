# MCP NPX runner image

This optional image provides an `npx` entrypoint for MCP configurations that
cannot use the public Node image directly.

```bash
docker build -f deploy/mcp-npx-runner/Dockerfile -t mcp-npx-runner:latest deploy/mcp-npx-runner
```

The image has no Compose file and no runtime environment variables. Push the
result to a registry reachable by the AppFactory backend and select that tag in
the tool configuration.
