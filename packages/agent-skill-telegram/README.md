# @xpell/agent-skill-telegram (alpha)

`@xpell/agent-skill-telegram` is an installable XBot skill that provides a Telegram connector module for:

1. Admin control chat (owner/admin chat IDs)
2. Customer chat routing into `channels.route_inbound_message`

It supports long-polling now and webhook scaffolding APIs for production integration later.

## What it exports

- `export const skill` (XBotSkill contract)
- Connector module name: `telegram`

Connector ops:
- `telegram.configure`
- `telegram.start`
- `telegram.stop`
- `telegram.reload_settings`
- `telegram.send`
- `telegram.status`
- `telegram.set_webhook` (stub)
- `telegram.handle_webhook_update` (stub target for future HTTP forwarding)

## Local workspace install

From repo root:

```bash
pnpm -C packages/agent-skill-telegram build
pnpm -C packages/agent-core build
```

Ensure `packages/agent-core/agent.config.json` has:
- `skills.allow` includes `@xpell/agent-skill-telegram`
- `skills.resolve.local_paths` includes `packages/agent-skill-telegram`

## Configure Telegram (kernel settings)

Telegram skill configuration is persisted in kernel `settings` under:

- `skills["@xpell/agent-skill-telegram"]`

ACP writes through:

- `settings.set_skill`

Skill enable reads through:

- `settings.get_skill` with `include_masked:false`

Default settings contract:

```json
{
  "bot_token": "",
  "admin_chat_ids": [],
  "mode": "polling",
  "auto_start": false,
  "polling": { "timeout_sec": 30 },
  "webhook": { "url": "", "secret_token": "" }
}
```

Sensitive fields:

- `bot_token`
- `webhook.secret_token`

ACP readbacks return masked sentinel (`••••••••`) for sensitive values.

You can also update settings manually:

```bash
node packages/agent-core/dist/cli/call.js settings set_skill '{
  "skill_id":"@xpell/agent-skill-telegram",
  "patch":{
    "bot_token":"<BOT_TOKEN>",
    "admin_chat_ids":["123456789"],
    "mode":"polling",
    "polling":{"timeout_sec":30}
  }
}'
```

Apply current settings to connector without reloading skill:

```bash
node packages/agent-core/dist/cli/call.js telegram reload_settings '{}'
```

## Start long polling

```bash
node packages/agent-core/dist/cli/call.js telegram start '{"mode":"polling"}'
```

If `auto_start=true` and a bot token is present, skill enable auto-runs `telegram.start`.

Polling behavior:
- Uses Telegram `getUpdates(timeout=N)` (default 30, clamped 5..60)
- No interval scheduler
- Stops after bounded consecutive API errors

## Admin commands

From an admin chat id:
- `/status`
- `/customers [N]`
- `/say <chat_id> <text>`

`/say` prefers `channels.send_message` so outbound messages are stored in conversations. If that fails, it falls back to direct Telegram send and logs that storage was skipped.

## Webhook scaffolding (alpha)

Implemented stubs:
- `telegram.set_webhook { url, secret_token? }`
- `telegram.handle_webhook_update { update, headers? }`

Current behavior:
- No webhook HTTP server in this package
- `set_webhook` returns `webhook_not_implemented_in_alpha`
- `handle_webhook_update` validates optional header `X-Telegram-Bot-Api-Secret-Token` against configured secret and routes update if valid

To enable production webhook later:
1. Add an HTTPS endpoint in `agent-core` transport layer
2. Forward raw webhook payload + headers into `telegram.handle_webhook_update`
3. Keep Telegram auth/secret checks at that trust boundary

## Alpha notes

- This package is alpha (`0.1.0-alpha.0`)
- API and command surface may change
