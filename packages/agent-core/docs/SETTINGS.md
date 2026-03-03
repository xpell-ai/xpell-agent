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
  "agent": {
    "name": "XBot",
    "business_name": "Ruta1",
    "identity": {
      "name": "XBot",
      "role": "",
      "system_prompt": "",
      "language_policy": "auto"
    }
  },
  "skills": {
    "@xpell/agent-skill-telegram": {
      "bot_token": "123456:ABC",
      "admin_chat_ids": ["12345"],
      "mode": "polling"
    }
  },
  "kb": {
    "source_path": "ruta1_kb_real.md",
    "default_file": "ruta1_kb.md",
    "current_path": "/abs/path/to/work/kb/xbot/default/kb.md",
    "allow_export": false,
    "export_roles": ["owner", "admin"],
    "max_export_chars": 8000
  }
}
```

Stable root keys used now:
- `ui`
- `agent`
- `skills`
- `kb`

KB settings used by the runtime:
- `kb.source_path`
  - default: `"ruta1_kb_real.md"` when that file exists in the repo root, otherwise `""`
  - used only to seed the first current KB file if `kb.md` does not exist yet
- `kb.current_path`
  - default: `<work_dir>/kb/<agent_id>/<env>/kb.md`
  - authoritative current KB file path used by the KB inbox apply flow
- `kb.default_file`
  - default: `"ruta1_kb.md"`
  - canonical KB filename used by `kb.show`, `kb.update_price`, and the mirrored KB write path when no `_kb_file` is provided

Agent profile keys used by ACP:
- `agent.identity.name`
- `agent.identity.role`
- `agent.identity.system_prompt`
- `agent.identity.language_policy`

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
