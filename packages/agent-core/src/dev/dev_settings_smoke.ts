import { randomBytes } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { _x, _xlog, Settings as XSettings } from "@xpell/node";

import { SettingsModule, SETTINGS_MODULE_NAME } from "../modules/SettingsModule.js";
import { setKernelCapSecret } from "../runtime/guards.js";
import type { SkillSettingsMeta } from "../types/settings.js";

type Dict = Record<string, unknown>;

const MASK_SENTINEL = "••••••••";
const FAKE_SKILL_ID = "@xpell/agent-skill-fake";

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function assert_true(condition: boolean, message: string): void {
  if (!condition) {
    throw new Error(message);
  }
}

async function main(): Promise<void> {
  const work_dir = await fs.mkdtemp(path.join(os.tmpdir(), "agent-settings-smoke-"));
  const kernel_cap = `kcap_${randomBytes(32).toString("hex")}`;
  setKernelCapSecret(kernel_cap);

  const fake_meta: SkillSettingsMeta = {
    defaults: {
      mode: "polling"
    },
    sensitive: ["bot_token"]
  };

  const settings_module = new SettingsModule({
    _work_dir: work_dir,
    _resolve_skill_meta: (skill_id) => (skill_id === FAKE_SKILL_ID ? fake_meta : undefined)
  });
  _x.loadModule(settings_module);

  const _ctx = {
    kernel_cap,
    actor: {
      role: "system",
      source: "dev:settings"
    }
  };

  await _x.execute({
    _module: SETTINGS_MODULE_NAME,
    _op: "init_on_boot",
    _params: { _ctx }
  });

  await _x.execute({
    _module: SETTINGS_MODULE_NAME,
    _op: "set_skill",
    _params: {
      skill_id: FAKE_SKILL_ID,
      patch: {
        bot_token: "real_secret_token",
        mode: "polling"
      },
      _ctx
    }
  });

  const masked_read = await _x.execute({
    _module: SETTINGS_MODULE_NAME,
    _op: "get_skill",
    _params: {
      skill_id: FAKE_SKILL_ID,
      include_masked: true,
      include_schema: false,
      _ctx
    }
  });

  const masked_result = is_plain_object(masked_read) && is_plain_object(masked_read.result) ? masked_read.result : {};
  const masked_settings = is_plain_object(masked_result.settings) ? masked_result.settings : {};
  const masked_map = is_plain_object(masked_result.masked) ? masked_result.masked : {};

  assert_true(masked_settings.bot_token === MASK_SENTINEL, "Expected masked bot_token in get_skill response");
  assert_true(masked_map.bot_token === true, "Expected masked map to include bot_token=true");

  await _x.execute({
    _module: SETTINGS_MODULE_NAME,
    _op: "set_skill",
    _params: {
      skill_id: FAKE_SKILL_ID,
      patch: {
        bot_token: MASK_SENTINEL,
        mode: "webhook"
      },
      _ctx
    }
  });

  const stored_read = await _x.execute({
    _module: SETTINGS_MODULE_NAME,
    _op: "get",
    _params: {
      key: `skills.${FAKE_SKILL_ID}`,
      _ctx
    }
  });

  const stored_settings = is_plain_object(stored_read) && is_plain_object(stored_read.value) ? stored_read.value : {};

  assert_true(stored_settings.bot_token === "real_secret_token", "Masked sentinel overwrite changed stored secret");
  assert_true(stored_settings.mode === "webhook", "Expected non-sensitive field patch to persist");

  _xlog.log(`[dev:settings] PASS work_dir=${work_dir}`);
  XSettings.close();
}

main().catch((err) => {
  const message = err instanceof Error ? err.message : String(err);
  _xlog.error(`[dev:settings] FAIL ${message}`);
  XSettings.close();
  process.exitCode = 1;
});
