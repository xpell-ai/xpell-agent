# XBot Skills Contract (`@xpell/agent`)

## What is a skill

An XBot skill is a controlled extension package loaded by `SkillManagerModule` (`skills`).
Skills can register XModules and execute runtime commands only through the agent command path.

Authoritative export format:

```ts
export const skill: XBotSkill
```

## Interface (short form)

```ts
type XBotSkillCapability = {
  kernel_ops?: string[]; // ["module.op"]
  channels?: string[];
  network?: boolean;
};

interface XBotSkill {
  id: string;
  version: string;
  name?: string;
  description?: string;
  settings?: {
    defaults?: Record<string, any>;
    schema?: {
      title?: string;
      fields: Array<{
        key: string;
        label: string;
        type: "string" | "number" | "boolean" | "select" | "string_list";
        help?: string;
        secret?: boolean;
        options?: Array<{ label: string; value: any }>;
        placeholder?: string;
      }>;
    };
    sensitive?: string[]; // dotted paths, e.g. ["bot_token", "oauth.client_secret"]
  };
  capabilities?: XBotSkillCapability;
  onEnable(ctx: XBotSkillContext): void | Promise<void>;
  onDisable?(ctx: XBotSkillContext): void | Promise<void>;
}

interface XBotSkillContext {
  execute(module: string, op: string, params?: any, meta?: any): Promise<any>;
  registerModule(moduleInstance: any): void;
  emit(eventName: string, payload: any): void;
  log(level: "debug" | "info" | "warn" | "error", msg: string, meta?: any): void;
  skill: { id: string; version: string };
}
```

## Skill Settings Contract

Skills can declare settings metadata in `skill.settings`:

- `defaults`: default values used when first initializing a skill in `settings`
- `schema`: UI-friendly field schema for ACP rendering
- `sensitive`: dotted paths masked by `settings.get_skill`

Runtime behavior:

- `SkillManagerModule` stores each loaded skill settings meta.
- On skill enable/load, `SkillManagerModule` calls:
  - `settings.get_skill` (deterministic read path)
  - initializes defaults once if no stored record exists (does not overwrite existing values)
- ACP should read/write skill settings through `settings` module ops.

## Capabilities

`capabilities.kernel_ops` declares privileged kernel ops a skill can execute with server-side capability injection.

- Format: `"module.op"` (example: `"channels.configure"`)
- If an op is not listed, skill calls execute without kernel capability injection.
- If the target module requires kernel capability, the op is rejected.

Other declarations:
- `channels`: channel ids the skill provides/needs.
- `network`: whether the skill performs outbound network calls.

## Security model

- Skills do not receive `_x` or internal runtime objects via API.
- Privileged operations are guarded in target modules using kernel capability checks.
- Kernel capability is generated at server boot, stored in runtime private state, and never logged.
- Transport (`Wormholes`/REST) is untrusted: client-provided `_ctx.kernel_cap` or `_ctx.actor` is discarded and replaced by server context.
- Importing `_x` directly from a skill is unsupported and does not bypass module guards for privileged ops.
- No `eval`, no arbitrary code-string execution, no network install path.

## Config

Config file: `packages/agent-core/agent.config.json`

```json
{
  "skills": {
    "allow": ["xpell-agent-skill-echo"],
    "enabled": ["xpell-agent-skill-echo"],
    "resolve": {
      "node_modules": true,
      "local_paths": ["packages/agent-core/examples/skill-echo"]
    }
  }
}
```

Rules:
- Skill must be allowlisted.
- Skill is loaded only if enabled.
- Resolver only uses `node_modules` and configured local paths.

## Example

```ts
export const skill = {
  id: "xpell-agent-skill-echo",
  version: "0.1.0-alpha.0",
  capabilities: {
    kernel_ops: ["channels.configure"]
  },
  async onEnable(ctx) {
    ctx.registerModule(new EchoModule());
    await ctx.execute("channels", "register", { channel: "echo", connector_module: "echo" });
    await ctx.execute("channels", "configure", { channel: "echo", config: { mode: "echo" } });
  }
};
```

## Legacy compatibility

Legacy skills are still supported:

```ts
export function registerSkill(ctx): void | Promise<void>
```

Legacy mode is treated as no declared capabilities (`kernel_ops=[]`).
It is deprecated and will be removed in a future major version.

## Skill ops (`skills` module)

- `skills.list`
- `skills.enable`
- `skills.disable`
- `skills.reload_enabled`

## Telegram Skill

Skill id:

- `@xpell/agent-skill-telegram`

Declared capabilities:

- `channels: ["telegram"]`
- `network: true`
- `kernel_ops: ["channels.route_inbound_message", "channels.send_message", "conv.list_threads"]`

Connector module ops:

- `telegram.configure`
- `telegram.start`
- `telegram.stop`
- `telegram.send`
- `telegram.status`
- `telegram.set_webhook` (stub)
- `telegram.handle_webhook_update` (stub)

Operational notes:

- Customer inbound traffic is routed through `channels.route_inbound_message` to preserve unified conversation storage.
- Outbound replies prefer `channels.send_message` so conversation outbound messages are persisted.
- Long polling uses Telegram `getUpdates(timeout=N)` and avoids interval schedulers.
- Webhook mode is scaffolded via module ops; production HTTP forwarding should call `telegram.handle_webhook_update`.
