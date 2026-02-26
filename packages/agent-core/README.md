# @xpell/agent (alpha)

`@xpell/agent` is the minimal Node runtime skeleton for Xpell Agent. It boots a deterministic server runtime, registers Bot OS core modules, and exposes a Wormholes v2-compatible REST bridge for command execution.

## What it includes

- Agent runtime bootstrap (`AgentRuntime`)
- Base runtime module (`AgentModule`)
- Bot OS modules:
  - `UsersModule` (`users`)
  - `ConversationsModule` (`conv`)
  - `ChannelsModule` (`channels`)
  - `KnowledgeModule` (`kb`)
  - `SkillManagerModule` (`skills`)
- Wormholes server bridge (`/wh/v2/hello`, `/wh/v2/call`, optional WS on `/wh/v2`) with strict envelope + XCmd validation and server-side `_ctx` injection

## Install

```bash
pnpm add @xpell/agent
```

```bash
npm install @xpell/agent
```

## Run (minimal)

```bash
pnpm -C packages/agent-core install
pnpm -C packages/agent-core dev
```

Build + start:

```bash
pnpm -C packages/agent-core build
pnpm -C packages/agent-core start
```

Call ops from CLI:

```bash
pnpm -C packages/agent-core agent:call users list
pnpm -C packages/agent-core agent:call channels list
pnpm -C packages/agent-core agent:call conv list_threads '{"limit":20}'
pnpm -C packages/agent-core agent:call agent status
```

Run the smoke flow:

```bash
pnpm -C packages/agent-core agent:smoke
```

Environment:

- `AGENT_HOST` (default: `127.0.0.1`)
- `AGENT_PORT` (default: `3090`)
- `AGENT_PUBLIC_DIR` (default: `packages/agent-core/public` from current working directory)

## Skills (XBotSkill)

Authoritative contract:

```ts
export const skill: XBotSkill
```

Legacy fallback is still supported:

```ts
export function registerSkill(ctx): void | Promise<void>
```

The skill manager reads allow/enabled state from `packages/agent-core/agent.config.json`.
Privileged skill operations are capability-gated (`capabilities.kernel_ops`) and enforced by module kernel-cap guards.

Skill commands:

```bash
pnpm -C packages/agent-core agent:call skills list
```

`skills.enable`, `skills.disable`, and `skills.reload_enabled` are privileged kernel ops and require server-side kernel capability context.

See [SKILLS.md](/Users/tamirfridman/Documents/projects/xpell.ai/xpell-agent/packages/agent-core/docs/SKILLS.md) for the official contract, capability model, and security constraints.

Telegram connector skill package is available at [@xpell/agent-skill-telegram](/Users/tamirfridman/Documents/projects/xpell.ai/xpell-agent/packages/agent-skill-telegram/README.md).

Azure skill package is available at [@xpell/agent-skill-azure](/Users/tamirfridman/Documents/projects/xpell.ai/xpell-agent/packages/agent-skill-azure/package.json).

Enablement example:

```json
{
  "skills": {
    "allow": ["@xpell/agent-skill-azure"],
    "enabled": ["@xpell/agent-skill-azure"],
    "resolve": {
      "node_modules": true,
      "local_paths": ["packages/agent-skill-azure"]
    }
  }
}
```

## Module Ops

`agent`
- `agent.ping`
- `agent.status`
- `agent.run_task`

`users`
- `users.bootstrap_owner`
- `users.add_admin_identity`
- `users.resolve_identity`
- `users.list`

`conv`
- `conv.get_or_create_thread`
- `conv.ensure_thread`
- `conv.append_message`
- `conv.list_threads`
- `conv.get_thread`
- `conv.get_thread_by_channel` (internal helper for channel routing)
- `conv.get_thread_by_key` (channel_id + thread_key helper)

`channels`
- `channels.register`
- `channels.configure`
- `channels.list`
- `channels.route_inbound_message`
- `channels.send_message`

`kb`
- `kb.list_sources`
- `kb.get_source`
- `kb.search`
- `kb.build_context`

`skills`
- `skills.list`
- `skills.enable`
- `skills.disable`
- `skills.reload_enabled`

## Runtime State Keys

No XData keys are used by these modules.

State is kept in module-local in-memory maps:
- `users`: `_users_by_id`, `_identity_to_user_id`, `_identity_by_key`, `_owner_user_id`
- `conv`: `_threads_by_id`, `_thread_id_by_key`, `_messages_by_thread_id`
- `channels`: `_registrations`
- `skills`: `_config`, `_enabled`, `_loaded`, `_skill_to_modules`, `_module_to_skill`

## Architecture in 60 seconds

- `AgentRuntime` owns startup and transport boundaries.
- All behavior is implemented as XModule commands (`agent`, `users`, `conv`, `channels`, `kb`, `skills`).
- Inbound Wormholes `REQ` envelopes are validated, server context is injected (`_ctx._wid`, optional `_ctx._sid`), then commands execute via `_x.execute`.

## Alpha disclaimer

This package is **alpha** (`0.1.0-alpha.0`). API shape, transport contracts, and module interfaces may change before stable release.
