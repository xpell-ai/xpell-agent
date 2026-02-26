# @xpell/agent-ui (alpha)

`@xpell/agent-ui` is a minimal Xpell operational cockpit for `@xpell/agent`. It boots a JSON-first XVM app with Dashboard, Skills, and Tasks views, and keeps a clean Wormholes client path ready for live runtime integration.

## Wormholes connection to @xpell/agent

The UI uses a dedicated Wormholes client service (`src/services/wormholes-client.ts`) to:

- connect/disconnect to `/wh/v2`
- send XCmd calls (`agent.status`, `agent.run_task`)
- update connection state in XData

When no server is connected, the UI runs in deterministic disconnected mode using `src/services/mock-agent.ts`.

## Run locally

```bash
pnpm -C packages/agent-ui install --ignore-workspace
pnpm -C packages/agent-ui dev
```

Open:

- `http://127.0.0.1:5173/`

## Production build target (served by agent-core)

```bash
pnpm -C packages/agent-ui build
```

This build writes the web assets directly to:

- `packages/agent-core/public`

Then run the server:

```bash
pnpm -C packages/agent-core dev
```

## Alpha disclaimer

This package is **alpha** (`0.1.0-alpha.0`). API contracts, view schemas, and command surface may change before stable release.
