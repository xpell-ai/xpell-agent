# Settings Module (`settings`)

`SettingsModule` is the kernel settings API for ACP and skills. It is backed by `@xpell/node` `XSettings` (`_xs`) and persists a single JSON document under:

- `work/settings/server-settings.json` (or the configured runtime work dir)

## Storage Model

Authoritative root shape:

```json
{
  "ui": {
    "theme": "dark"
  },
  "skills": {
    "@xpell/agent-skill-telegram": {
      "bot_token": "123456:ABC",
      "admin_chat_ids": ["12345"],
      "mode": "polling"
    }
  }
}
```

Stable root keys used now:
- `ui`
- `skills`

Per-skill settings are stored at:
- `skills.<skill_id>`

## Ops

- `settings.get { key? }`
- `settings.set { key, value }`
- `settings.get_skill { skill_id, include_schema?, include_masked? }`
- `settings.set_skill { skill_id, patch }`
- `settings.reset_skill { skill_id }`
- `settings.schema { skill_id }`

Write ops (`set`, `set_skill`, `reset_skill`) require privileged context:
- valid `kernel_cap`, or
- authenticated actor role `admin`/`owner`

## Sensitive Masking

Skills can declare sensitive dotted paths in `skill.settings.sensitive`.

On `settings.get_skill` (default `include_masked=true`):
- sensitive values are replaced with `••••••••`
- `masked` map marks masked paths

Example:

```json
{
  "ok": true,
  "result": {
    "skill_id": "@xpell/agent-skill-telegram",
    "settings": {
      "bot_token": "••••••••",
      "admin_chat_ids": ["12345"],
      "mode": "polling"
    },
    "masked": {
      "bot_token": true
    }
  }
}
```

Patch sentinel behavior in `settings.set_skill`:
- if a sensitive field in `patch` is `••••••••`, stored secret is preserved (not overwritten)

## Schema Exposure

If a skill defines `skill.settings.schema`, ACP can request:

- `settings.get_skill { include_schema: true }`, or
- `settings.schema { skill_id }`

No schema returns:

```json
{ "ok": false, "reason": "no_schema" }
```
