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
  intents?: Array<{
    intent_id: string;
    title: string;
    description?: string;
    roles_allowed: Array<"owner" | "admin" | "customer">;
    channels_allowed?: string[];
    handler: { module: string; op: string };
  }>;
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

## Intent declarations

Skills may declare `skill.intents` metadata. These are static, JSON-safe declarations only.

- Only allowlisted and enabled skills contribute intents to runtime discovery.
- The LLM never chooses a module/op directly.
- Runtime resolves:
  - `intent_id -> handler.module + handler.op`
  - from the discovered skill manifest plus the persisted intent registry overlay

Example:

```ts
intents: [
  {
    intent_id: "admin.conv.summary",
    title: "Conversation Summary",
    roles_allowed: ["admin", "owner"],
    channels_allowed: ["telegram"],
    handler: { module: "admin_cmd", op: "conv_summary" },
    examples: ["summarize today", "show me recent conversations"]
  }
]
```

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
- `skills.init_on_boot`
- `skills.state_get`
  - input: `{ _skill_id, _key }`
  - output: `{ found, _value? }`
  - access: kernel-cap only (used by skill context)
- `skills.state_set`
  - input: `{ _skill_id, _key, _value }`
  - output: `{ ok }`
  - access: kernel-cap only (used by skill context)
- `skills.list_intents`
  - output: `{ items }`
  - access: requires authenticated `admin` / `owner` actor role for client calls (kernel-capable server calls are also allowed)

## Intent Registry Module (`intent`)

- `intent.init_on_boot`
- `intent.list_all`
- `intent.set_enabled`
- `intent.update_config`
- `intent.get_enabled_for_context`

Intent config is stored in XDB entity:

- `agent.intents::${_app_id}::${_env}`

Built-in admin intent:

- `admin.conv.summary_today`
  - default: enabled
  - roles: `owner`, `admin`
  - handler: `conv.summary_today`
  - deterministic override: admin messages like `summarize today` are forced to this intent before chat fallback

## Knowledge Base Inbox (`kb`)

KB changes must go through the core `kb` module. Skills should not write KB files directly.

Inbox ops:

- `kb.show`
- `kb.append`
- `kb.append_to_section`
- `kb.remove_from_section`
- `kb.update_price`
- `kb.patch_section`
- `kb.replace_propose`
- `kb.replace_confirm`
- `kb.delete_section_propose`
- `kb.delete_section_confirm`
- `kb.history`
- `kb.get_current`

Rules:
- only `admin` / `owner` actors (or kernel-cap) may call KB inbox ops
- append / patch execute immediately with actor audit
- `kb.show` without a section returns an index of `##` / `###` headings only
- `kb.show section <title>` returns only the matched markdown section and never falls back to the full KB on misses
- line removals use `kb.remove_from_section`: first pass returns a preview, then `confirm` / `apply` / `yes` removes exactly one matching line
- price updates use `kb.update_price`: first pass returns a preview, then `confirm` / `apply` / `yes` publishes the pending change
- pending KB price previews are stored in XData under keys:
  - `pending_kb_patch::<_sid|user_id>::<wid>`
  - plus a stable pointer key `pending_kb_patch::<_sid|user_id>::latest`
- replace / delete-section require explicit confirm through pending actions
- audit rows are stored in `agent.kb_audit::${_app_id}::${_env}`
- current docs are stored in `agent.kb_docs::${_app_id}::${_env}`
- pending confirmations are stored in `agent.admin_pending_actions::${_app_id}::${_env}`
- customers never receive KB mutation instructions; admin natural-language requests are translated into deterministic `kb.*` ops in the server routing path
- manual checks:
  - `show kb` -> index only
  - `show kb section Bebidas` -> only that section
  - `show kb section not_a_real_section` -> not found + heading suggestions
- non-command messages that do not contain explicit KB-management trigger terms (`kb`, `knowledge base`, `base de conocimiento`, `patch`, `append`, `replace`, `delete`) bypass KB management and go to the normal KB-backed Q&A path

## Users Module (`users`)

Core ops used by ACP and channels:

- `users.list`
  - input: `{ _limit?, _cursor?, _q?, role? }`
  - output: `{ items, next_cursor? }`
  - item shape: `{ _id, _display_id?, _display_name, _role, _channels, _created_at, _updated_at }`
  - access: requires authenticated `admin` / `owner` actor role for client calls (kernel-capable server calls are also allowed)
- `users.upsert_from_channel_identity`
  - input: `{ channel_id, external_user_id, external_username?, display_name?, meta? }`
  - output: `{ user_id }`
  - used by `channels.route_inbound_message` before inbound message persistence so chat users are persisted in XDB
- `users.set_role`
  - input: `{ _user_id, _role }`
  - output: `{ _ok, _user_id, _role }`
  - access: requires authenticated `admin` / `owner` actor role; only `owner` may set `_role: "owner"`
- `users.reset_storage`
  - input: `{}`
  - output: `{ users_deleted, sessions_deleted }`
  - access: requires authenticated `admin` / `owner` actor role for client calls (kernel-capable server calls are also allowed)
  - used by `agent.reset_db`

Migration note:
- one-time flag stored at `migrations.users_id_v1_done`
- legacy `_user_id` values are preserved into `_display_id` during migration
- internal kernel op `conv.remap_user_ids` is used during that migration to rewrite legacy conversation thread foreign keys to canonical user `_id`

## Agent Module (`agent`)

- `agent.reset_db`
  - input: `{}`
  - output: `{ ok, threads_deleted, messages_deleted, users_deleted, sessions_deleted }`
  - access: requires authenticated `admin` / `owner` actor role for client calls (kernel-capable server calls are also allowed)
  - behavior: deletes all data from `agent.conv_threads`, `agent.conv_messages`, `agent.users`, and `agent.user_sessions`

## Conversations Module (`conv`)

- `conv.reset_storage`
  - input: `{}`
  - output: `{ threads_deleted, messages_deleted }`
  - access: requires authenticated `admin` / `owner` actor role for client calls (kernel-capable server calls are also allowed)
  - used by `agent.reset_db`
- `conv.add_participant`
- `conv.list_participants`
- `conv.ensure_thread_participant`

## Channels Module (`channels`)

- `channels.resolve_or_create_user_for_inbound`
  - input: `{ _channel_id, _external_user_id, _display_name?, _profile? }`
  - output: `{ _user_id, _is_new }`
  - behavior: uses `agent.channel_identities` as the canonical `(channel_id, external_user_id) -> user_id` mapping
- `channels.route_inbound_message`
  - accepts the existing rich payload and the simplified snake_case payload:
    - `{ _channel, _channel_user_id, _thread_key?, _text, _external_id?, _meta? }`
  - persists the inbound message first, then:
    - admin / owner + `/command` => routes through `admin_cmd.handle_message`
    - all other inbound messages => sends the current placeholder reply through `channels.send_message`

## Admin Commands Module (`admin_cmd`)

- `admin_cmd.is_command`
  - input: `{ _text }`
  - output: `{ _is_command, _cmd?, _args? }`
- `admin_cmd.handle_message`
  - input: `{ _text, _thread_id?, _user_id?, _ctx }`
  - output: `{ _reply_text }`
  - access: requires authenticated `admin` / `owner` actor role
  - Telegram admin checks:
    - `/users` -> lists public user records
    - `/users telegram` -> shows compact Telegram identities for recipient verification
    - `/broadcast preview Tonight: karaoke 9pm!` -> previews Telegram customer recipients
    - `/broadcast Tonight: karaoke 9pm!` -> sends immediately via `channels.send_message`
    - `/broadcast --include-admins Test message` -> includes admins in the audience
    - Natural admin confirmation is thread-scoped:
      - after a draft preview, reply `send`, `yes`, `confirm`, `ok`, or `sure` to send immediately
      - reply `cancel`, `no`, or `stop` to discard it
      - pending confirmation is stored on the conversation thread and is cleared after send/cancel

## Quick Test

1. Start `agent-core` and `agent-ui`.
2. Send a Telegram message from a new chat and confirm the user appears in ACP `Users`.
3. Use the Users table action to promote that user to `admin`.
4. Send `/help` from the same Telegram chat and confirm the bot replies with the admin help text.
5. Confirm the outbound admin reply is persisted in the conversation thread.
6. Send a normal Telegram message and confirm it follows the normal placeholder reply path.

## Intent Checklist

- Skill declares intents with stable ids.
- Intent handlers are XModule ops only.
- ACP enables/disables intents; it does not execute arbitrary module/op input.
- LLM chooses only from enabled intents returned by the registry.
- Role and channel checks are enforced before execution.

## KB Module (`kb`)

- `kb.sources_list`
- `kb.sources_upsert`
- `kb.sources_enable`
- `kb.sources_disable`
- `kb.reload_source`
- `kb.reload_all_enabled`
- `kb.docs_list`
- `kb.docs_get`
- storage entities:
  - `agent.kb_sources`
  - `agent.kb_docs`

## Telegram Skill

Skill id:

- `@xpell/agent-skill-telegram`

Declared capabilities:

- `channels: ["telegram"]`
- `network: true`
- `kernel_ops: ["channels.resolve_or_create_user_for_inbound", "channels.route_inbound_message", "channels.send_message", "conv.list_threads", "agent.handle_inbound", "skills.state_get", "skills.state_set"]`

Connector module ops:

- `telegram.configure`
- `telegram.start`
- `telegram.stop`
- `telegram.send`
- `telegram.status`
- `telegram.set_webhook` (stub)
- `telegram.handle_webhook_update` (stub)

Operational notes:

- Inbound customer flow now resolves/creates users via `channels.resolve_or_create_user_for_inbound` before `channels.route_inbound_message`.
- Customer inbound traffic is routed through `channels.route_inbound_message` to preserve unified conversation storage.
- Outbound replies prefer `channels.send_message` so conversation outbound messages are persisted.
- Polling cursor state (`telegram.last_update_id`) is stored in `agent.skill_state` through `skills.state_get` / `skills.state_set`.
- Long polling uses Telegram `getUpdates(timeout=N)` and avoids interval schedulers.
- Webhook mode is scaffolded via module ops; production HTTP forwarding should call `telegram.handle_webhook_update`.
