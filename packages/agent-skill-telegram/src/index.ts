import { _xlog } from "@xpell/node";
import { TelegramConnectorModule } from "./TelegramConnectorModule.js";
import { TELEGRAM_SETTINGS_DEFAULTS, TELEGRAM_SETTINGS_META, normalize_telegram_skill_settings } from "./settings.js";

type Dict = Record<string, unknown>;

type XBotSkillCapability = {
  kernel_ops?: string[];
  channels?: string[];
  network?: boolean;
};

type XBotSkillContext = {
  execute(module: string, op: string, params?: unknown, meta?: unknown): Promise<unknown>;
  registerModule(moduleInstance: unknown): void;
  emit(eventName: string, payload: unknown): void;
  log(level: "debug" | "info" | "warn" | "error", msg: string, meta?: unknown): void;
  skill: { id: string; version: string };
};

type XBotSkill = {
  id: string;
  version: string;
  name?: string;
  description?: string;
  settings?: {
    defaults?: Record<string, unknown>;
    sensitive?: string[];
    schema?: {
      title?: string;
      fields: Array<{
        key: string;
        label: string;
        type: "string" | "number" | "boolean" | "select" | "string_list";
        help?: string;
        secret?: boolean;
        options?: Array<{ label: string; value: unknown }>;
        placeholder?: string;
      }>;
    };
  };
  capabilities?: XBotSkillCapability;
  onEnable(ctx: XBotSkillContext): Promise<void> | void;
  onDisable?(ctx: XBotSkillContext): Promise<void> | void;
};

type SettingsGetSkillResult = {
  settings?: unknown;
};

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function read_settings_payload(raw: unknown): SettingsGetSkillResult {
  if (!is_plain_object(raw)) return {};
  const result = is_plain_object(raw.result) ? raw.result : {};
  return {
    ...(Object.prototype.hasOwnProperty.call(result, "settings") ? { settings: result.settings } : {})
  };
}

async function ensure_settings_initialized(ctx: XBotSkillContext): Promise<void> {
  await ctx.execute("settings", "get_skill", {
    skill_id: ctx.skill.id,
    include_schema: false,
    include_masked: false
  });

  const root_skills = await ctx.execute("settings", "get", { key: "skills" });
  const value = is_plain_object(root_skills) && is_plain_object(root_skills.value) ? root_skills.value : {};
  const stored_entry = Object.prototype.hasOwnProperty.call(value, ctx.skill.id) ? value[ctx.skill.id] : undefined;
  const has_non_empty_entry = is_plain_object(stored_entry) && Object.keys(stored_entry).length > 0;
  if (has_non_empty_entry) return;

  await ctx.execute("settings", "set_skill", {
    skill_id: ctx.skill.id,
    patch: TELEGRAM_SETTINGS_DEFAULTS
  });
}

async function read_skill_settings(ctx: XBotSkillContext) {
  const out = await ctx.execute("settings", "get_skill", {
    skill_id: ctx.skill.id,
    include_schema: false,
    include_masked: false
  });
  const payload = read_settings_payload(out);
  return normalize_telegram_skill_settings(payload.settings);
}

export const skill: XBotSkill = {
  id: "@xpell/agent-skill-telegram",
  version: "0.1.0-alpha.0",
  name: "Telegram Connector",
  description: "Telegram channel connector for XBot (admin + customer chats).",
  settings: TELEGRAM_SETTINGS_META,
  capabilities: {
    channels: ["telegram"],
    network: true,
    kernel_ops: ["channels.route_inbound_message", "channels.send_message", "conv.list_threads", "agent.handle_inbound"]
  },
  async onEnable(ctx) {
    _xlog.log("[agent-core][telegram] skill enable start", { skill_id: ctx.skill.id, version: ctx.skill.version });
    ctx.registerModule(new TelegramConnectorModule(ctx));

    try {
      await ctx.execute("channels", "register", {
        channel: "telegram",
        connector_module: "telegram"
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      ctx.log("warn", "channels.register failed during telegram skill enable", { error: message });
    }

    await ensure_settings_initialized(ctx);
    const settings = await read_skill_settings(ctx);

    await ctx.execute("telegram", "configure", {
      bot_token: settings.bot_token,
      mode: settings.mode,
      polling: settings.polling,
      webhook: settings.webhook,
      admins: { chat_ids: settings.admin_chat_ids }
    });
    _xlog.log("[agent-core][telegram] skill configured from settings", {
      mode: settings.mode,
      has_bot_token: settings.bot_token.trim().length > 0,
      admins_count: settings.admin_chat_ids.length,
      timeout_sec: settings.polling.timeout_sec,
      auto_start: settings.auto_start
    });

    if (settings.auto_start === true && settings.bot_token.trim().length > 0) {
      await ctx.execute("telegram", "start", { mode: settings.mode });
      ctx.log("info", "telegram auto_start executed", { mode: settings.mode });
      _xlog.log("[agent-core][telegram] skill auto_start executed", { mode: settings.mode });
    }

    ctx.log("info", "telegram skill enabled with settings from kernel SettingsModule", {
      skill: ctx.skill.id,
      mode: settings.mode,
      auto_start: settings.auto_start
    });
  },
  async onDisable(ctx) {
    try {
      await ctx.execute("telegram", "stop", {});
      _xlog.log("[agent-core][telegram] skill disabled and polling stopped", { skill_id: ctx.skill.id });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      ctx.log("warn", "telegram.stop failed on disable", { error: message });
      _xlog.log("[agent-core][telegram] skill disable stop failed", { error: message });
    }
  }
};

export default skill;
