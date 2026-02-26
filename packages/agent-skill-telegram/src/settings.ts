type Dict = Record<string, unknown>;
type SettingsSchemaFieldType = "string" | "number" | "boolean" | "select" | "string_list";

type SettingsSchemaField = {
  key: string;
  label: string;
  type: SettingsSchemaFieldType;
  help?: string;
  secret?: boolean;
  options?: Array<{ label: string; value: unknown }>;
  placeholder?: string;
};

type TelegramSettingsMeta = {
  defaults: TelegramSkillSettings;
  sensitive: string[];
  schema: {
    title: string;
    fields: SettingsSchemaField[];
  };
};

export type TelegramMode = "polling" | "webhook";

export type TelegramSkillSettings = {
  bot_token: string;
  admin_chat_ids: string[];
  mode: TelegramMode;
  auto_start: boolean;
  polling: {
    timeout_sec: number;
  };
  webhook: {
    url: string;
    secret_token: string;
  };
};

export const TELEGRAM_SETTINGS_DEFAULTS: TelegramSkillSettings = {
  bot_token: "",
  admin_chat_ids: [],
  mode: "polling",
  auto_start: false,
  polling: {
    timeout_sec: 30
  },
  webhook: {
    url: "",
    secret_token: ""
  }
};

export const TELEGRAM_SETTINGS_META: TelegramSettingsMeta = {
  defaults: TELEGRAM_SETTINGS_DEFAULTS,
  sensitive: ["bot_token", "webhook.secret_token"],
  schema: {
    title: "Telegram",
    fields: [
      {
        key: "bot_token",
        label: "Telegram: Bot Token",
        type: "string",
        secret: true,
        placeholder: "123456:ABC-DEF..."
      },
      {
        key: "admin_chat_ids",
        label: "Telegram: Admin Chat IDs",
        type: "string_list",
        help: "Comma-separated or list input; stored as array of strings"
      },
      {
        key: "mode",
        label: "Telegram: Mode",
        type: "select",
        options: [
          { label: "Polling", value: "polling" },
          { label: "Webhook", value: "webhook" }
        ]
      },
      {
        key: "polling.timeout_sec",
        label: "Telegram: Poll Timeout (sec)",
        type: "number",
        help: "Clamped to 5..60 seconds"
      },
      {
        key: "webhook.url",
        label: "Telegram: Webhook URL",
        type: "string",
        help: "Public HTTPS URL required (production)"
      },
      {
        key: "webhook.secret_token",
        label: "Telegram: Webhook Secret Token",
        type: "string",
        secret: true
      },
      {
        key: "auto_start",
        label: "Telegram: Auto Start",
        type: "boolean",
        help: "If true and bot token exists, telegram.start runs on skill enable."
      }
    ]
  }
};

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function as_non_empty_text(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function as_text(value: unknown, fallback = ""): string {
  if (typeof value !== "string") return fallback;
  return value.trim();
}

function as_mode(value: unknown): TelegramMode {
  return value === "webhook" ? "webhook" : "polling";
}

function as_bool(value: unknown, fallback: boolean): boolean {
  return typeof value === "boolean" ? value : fallback;
}

function as_timeout_sec(value: unknown): number {
  const fallback = TELEGRAM_SETTINGS_DEFAULTS.polling.timeout_sec;
  const raw = typeof value === "number" ? value : Number.parseInt(String(value ?? fallback), 10);
  if (!Number.isFinite(raw)) return fallback;
  return Math.max(5, Math.min(60, Math.trunc(raw)));
}

function as_admin_chat_ids(value: unknown): string[] {
  if (Array.isArray(value)) {
    const out: string[] = [];
    const seen = new Set<string>();
    for (const item of value) {
      const text = as_non_empty_text(typeof item === "number" ? String(Math.trunc(item)) : item);
      if (!text || seen.has(text)) continue;
      seen.add(text);
      out.push(text);
    }
    return out;
  }

  const csv = as_non_empty_text(value);
  if (!csv) return [];
  return csv
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

export function normalize_telegram_skill_settings(raw: unknown): TelegramSkillSettings {
  const input = is_plain_object(raw) ? raw : {};
  const polling = is_plain_object(input.polling) ? input.polling : {};
  const webhook = is_plain_object(input.webhook) ? input.webhook : {};

  return {
    bot_token: as_text(input.bot_token, ""),
    admin_chat_ids: as_admin_chat_ids(input.admin_chat_ids),
    mode: as_mode(input.mode),
    auto_start: as_bool(input.auto_start, TELEGRAM_SETTINGS_DEFAULTS.auto_start),
    polling: {
      timeout_sec: as_timeout_sec(polling.timeout_sec)
    },
    webhook: {
      url: as_text(webhook.url, ""),
      secret_token: as_text(webhook.secret_token, "")
    }
  };
}
