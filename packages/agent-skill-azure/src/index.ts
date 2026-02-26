import { XError, XModule, type XCommandData } from "@xpell/node";

type Dict = Record<string, unknown>;

type SkillLogLevel = "debug" | "info" | "warn" | "error";

type XBotSkillCapability = {
  kernel_ops?: string[];
  channels?: string[];
  network?: boolean;
};

type XBotSkillContext = {
  execute(module: string, op: string, params?: unknown, meta?: unknown): Promise<unknown>;
  registerModule(moduleInstance: unknown): void;
  emit(eventName: string, payload: unknown): void;
  log(level: SkillLogLevel, msg: string, meta?: unknown): void;
  skill: { id: string; version: string };
};

type XBotSkillSettingsField = {
  key: string;
  label: string;
  type: "string" | "number" | "boolean" | "select" | "string_list";
  help?: string;
  secret?: boolean;
  options?: Array<{ label: string; value: unknown }>;
  placeholder?: string;
};

type XBotSkillSettingsMeta = {
  defaults?: Record<string, unknown>;
  sensitive?: string[];
  schema?: {
    title?: string;
    fields: XBotSkillSettingsField[];
  };
};

type XBotSkill = {
  id: string;
  version: string;
  name?: string;
  description?: string;
  settings?: XBotSkillSettingsMeta;
  capabilities?: XBotSkillCapability;
  onEnable(ctx: XBotSkillContext): Promise<void> | void;
  onDisable?(ctx: XBotSkillContext): Promise<void> | void;
};

const AZURE_SKILL_ID = "@xpell/agent-skill-azure";
const AZURE_ALLOWED_KEYS = [
  "AZURE_OPENAI_API_KEY",
  "AZURE_OPENAI_ENDPOINT",
  "AZURE_OPENAI_DEPLOYMENT",
  "AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT",
  "AZURE_SPEECH_KEY",
  "AZURE_SPEECH_REGION"
] as const;

type AzureSettingsKey = (typeof AZURE_ALLOWED_KEYS)[number];
type AzureSettings = Record<AzureSettingsKey, string>;

const AZURE_SECRET_KEYS: AzureSettingsKey[] = ["AZURE_OPENAI_API_KEY", "AZURE_SPEECH_KEY"];
const MASK_SENTINEL = "••••••••";
const AZURE_OPENAI_API_VERSION = "2024-02-15-preview";

const AZURE_SETTINGS_DEFAULTS: AzureSettings = {
  AZURE_OPENAI_API_KEY: "",
  AZURE_OPENAI_ENDPOINT: "",
  AZURE_OPENAI_DEPLOYMENT: "",
  AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT: "",
  AZURE_SPEECH_KEY: "",
  AZURE_SPEECH_REGION: ""
};

const AZURE_SETTINGS_META: XBotSkillSettingsMeta = {
  defaults: AZURE_SETTINGS_DEFAULTS,
  sensitive: AZURE_SECRET_KEYS,
  schema: {
    title: "Azure",
    fields: [
      {
        key: "AZURE_OPENAI_API_KEY",
        label: "Azure OpenAI API Key",
        type: "string",
        secret: true,
        placeholder: "****************"
      },
      {
        key: "AZURE_OPENAI_ENDPOINT",
        label: "Azure OpenAI Endpoint (https://...openai.azure.com)",
        type: "string",
        help: "Azure OpenAI resource endpoint URL.",
        placeholder: "https://my-resource.openai.azure.com"
      },
      {
        key: "AZURE_OPENAI_DEPLOYMENT",
        label: "Azure OpenAI Deployment (chat/completions)",
        type: "string",
        help: "Deployment name used for chat/completions requests.",
        placeholder: "gpt-4o"
      },
      {
        key: "AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT",
        label: "Azure OpenAI Embeddings Deployment",
        type: "string",
        help: "Deployment name used for embeddings requests.",
        placeholder: "text-embedding-3-large"
      },
      {
        key: "AZURE_SPEECH_KEY",
        label: "Azure Speech Key",
        type: "string",
        secret: true,
        placeholder: "****************"
      },
      {
        key: "AZURE_SPEECH_REGION",
        label: "Azure Speech Region (e.g., westeurope)",
        type: "string",
        help: "Azure Speech region identifier.",
        placeholder: "westeurope"
      }
    ]
  }
};

type AzureStatusResult = {
  configured: boolean;
  has_openai: boolean;
  has_speech: boolean;
  endpoint?: string;
  deployment?: string;
  speech_region?: string;
};

type AzureConnectionProbeResult = {
  ok: boolean;
  detail?: string;
};

type AzureImportEnvResult = AzureStatusResult & {
  imported_count: number;
  imported_keys: AzureSettingsKey[];
  detail?: string;
};

type AzureOpenAIChatMessage = {
  role: "system" | "user" | "assistant";
  content: string;
};

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function has_function(value: unknown): boolean {
  if (typeof value === "function") return true;
  if (Array.isArray(value)) return value.some(has_function);
  if (is_plain_object(value)) return Object.values(value).some(has_function);
  return false;
}

function ensure_optional_string(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function ensure_optional_number(value: unknown): number | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new XError("E_AZURE_BAD_PARAMS", "number field must be finite");
  }
  return value;
}

function ensure_json_object(value: unknown, field_name: string): Dict {
  if (!is_plain_object(value)) {
    throw new XError("E_AZURE_BAD_PARAMS", `${field_name} must be an object`);
  }
  if (has_function(value)) {
    throw new XError("E_AZURE_BAD_PARAMS", `${field_name} must be JSON-safe`);
  }
  return { ...value };
}

function normalize_endpoint(value: unknown): string {
  const endpoint = ensure_optional_string(value);
  if (!endpoint) return "";
  return endpoint.endsWith("/") ? endpoint.slice(0, -1) : endpoint;
}

function sanitize_connection_error(err: unknown): string {
  const message = err instanceof Error ? err.message : String(err);
  return message.replace(/[A-Za-z0-9_-]{20,}/g, "[redacted]");
}

function is_azure_key(value: string): value is AzureSettingsKey {
  return (AZURE_ALLOWED_KEYS as readonly string[]).includes(value);
}

function sanitize_settings(raw: unknown): AzureSettings {
  const input = is_plain_object(raw) ? raw : {};
  const out: AzureSettings = { ...AZURE_SETTINGS_DEFAULTS };
  for (const key of AZURE_ALLOWED_KEYS) {
    const value = input[key];
    out[key] = typeof value === "string" ? value.trim() : "";
  }
  out.AZURE_OPENAI_ENDPOINT = normalize_endpoint(out.AZURE_OPENAI_ENDPOINT);
  return out;
}

function status_from_settings(settings: AzureSettings): AzureStatusResult {
  const has_openai = Boolean(settings.AZURE_OPENAI_ENDPOINT && settings.AZURE_OPENAI_API_KEY);
  const has_speech = Boolean(settings.AZURE_SPEECH_REGION && settings.AZURE_SPEECH_KEY);
  return {
    configured: has_openai || has_speech,
    has_openai,
    has_speech,
    ...(settings.AZURE_OPENAI_ENDPOINT ? { endpoint: settings.AZURE_OPENAI_ENDPOINT } : {}),
    ...(settings.AZURE_OPENAI_DEPLOYMENT ? { deployment: settings.AZURE_OPENAI_DEPLOYMENT } : {}),
    ...(settings.AZURE_SPEECH_REGION ? { speech_region: settings.AZURE_SPEECH_REGION } : {})
  };
}

function parse_openai_message(raw: unknown): AzureOpenAIChatMessage {
  if (!is_plain_object(raw)) {
    throw new XError("E_AZURE_BAD_PARAMS", "messages[] must be objects");
  }
  const role = ensure_optional_string(raw.role);
  if (role !== "system" && role !== "user" && role !== "assistant") {
    throw new XError("E_AZURE_BAD_PARAMS", "messages[].role must be system|user|assistant");
  }
  const content = ensure_optional_string(raw.content);
  if (!content) {
    throw new XError("E_AZURE_BAD_PARAMS", "messages[].content is required");
  }
  return { role, content };
}

function extract_chat_text(raw: unknown): string | undefined {
  if (!is_plain_object(raw)) return undefined;
  const choices = Array.isArray(raw.choices) ? raw.choices : [];
  if (choices.length === 0) return undefined;
  const first = is_plain_object(choices[0]) ? choices[0] : {};
  const message = is_plain_object(first.message) ? first.message : {};
  const content = message.content;
  if (typeof content === "string" && content.trim().length > 0) return content.trim();
  if (Array.isArray(content)) {
    const parts: string[] = [];
    for (const part of content) {
      if (!is_plain_object(part)) continue;
      if (typeof part.text === "string" && part.text.trim().length > 0) {
        parts.push(part.text.trim());
      }
    }
    if (parts.length > 0) return parts.join("\n");
  }
  return undefined;
}

async function read_skill_settings(ctx: XBotSkillContext): Promise<AzureSettings> {
  const out = await ctx.execute("settings", "get_skill", {
    skill_id: AZURE_SKILL_ID,
    include_schema: false,
    include_masked: false
  });
  const result = is_plain_object(out) && is_plain_object(out.result) ? out.result : {};
  const settings = Object.prototype.hasOwnProperty.call(result, "settings") ? result.settings : {};
  return sanitize_settings(settings);
}

class AzureModule extends XModule {
  static _name = "azure";

  private _ctx: XBotSkillContext;

  constructor(ctx: XBotSkillContext) {
    super({ _name: "azure" });
    this._ctx = ctx;
  }

  async _status(xcmd: XCommandData) {
    return this.status_impl(xcmd);
  }
  async _op_status(xcmd: XCommandData) {
    return this.status_impl(xcmd);
  }

  async _configure(xcmd: XCommandData) {
    return this.configure_impl(xcmd);
  }
  async _op_configure(xcmd: XCommandData) {
    return this.configure_impl(xcmd);
  }

  async _test_connection(xcmd: XCommandData) {
    return this.test_connection_impl(xcmd);
  }
  async _op_test_connection(xcmd: XCommandData) {
    return this.test_connection_impl(xcmd);
  }

  async _import_env(xcmd: XCommandData) {
    return this.import_env_impl(xcmd);
  }
  async _op_import_env(xcmd: XCommandData) {
    return this.import_env_impl(xcmd);
  }

  async _openai_chat(xcmd: XCommandData) {
    return this.openai_chat_impl(xcmd);
  }
  async _op_openai_chat(xcmd: XCommandData) {
    return this.openai_chat_impl(xcmd);
  }

  private async status_impl(_xcmd: XCommandData): Promise<AzureStatusResult> {
    const settings = await read_skill_settings(this._ctx);
    return status_from_settings(settings);
  }

  private async configure_impl(xcmd: XCommandData): Promise<AzureStatusResult> {
    const params = ensure_json_object(xcmd?._params ?? {}, "params");
    const patch = ensure_json_object(params.patch ?? {}, "patch");
    const normalized_patch: Partial<Record<AzureSettingsKey, string>> = {};

    for (const [key, raw_value] of Object.entries(patch)) {
      if (!is_azure_key(key)) {
        throw new XError("E_AZURE_BAD_PARAMS", `Unsupported key in patch: ${key}`);
      }
      if (typeof raw_value !== "string") {
        throw new XError("E_AZURE_BAD_PARAMS", `Patch value for ${key} must be a string`);
      }
      normalized_patch[key] = AZURE_SECRET_KEYS.includes(key) && raw_value === MASK_SENTINEL ? MASK_SENTINEL : raw_value.trim();
    }

    await this._ctx.execute("settings", "set_skill", {
      skill_id: AZURE_SKILL_ID,
      patch: normalized_patch
    });

    const settings = await read_skill_settings(this._ctx);
    return status_from_settings(settings);
  }

  private async test_connection_impl(_xcmd: XCommandData): Promise<{
    openai: AzureConnectionProbeResult;
    speech: AzureConnectionProbeResult;
    _ts: number;
  }> {
    const settings = await read_skill_settings(this._ctx);

    const openai = await this.probe_openai(settings);
    const speech = this.probe_speech(settings);
    return {
      openai,
      speech,
      _ts: Date.now()
    };
  }

  private async import_env_impl(xcmd: XCommandData): Promise<AzureImportEnvResult> {
    ensure_json_object(xcmd?._params ?? {}, "params");

    const patch: Partial<Record<AzureSettingsKey, string>> = {};
    const imported_keys: AzureSettingsKey[] = [];

    for (const key of AZURE_ALLOWED_KEYS) {
      const env_value = ensure_optional_string(process.env[key]);
      if (!env_value) continue;
      patch[key] = env_value;
      imported_keys.push(key);
    }

    if (imported_keys.length > 0) {
      await this._ctx.execute("settings", "set_skill", {
        skill_id: AZURE_SKILL_ID,
        patch
      });
    }

    const settings = await read_skill_settings(this._ctx);
    const status = status_from_settings(settings);
    return {
      ...status,
      imported_count: imported_keys.length,
      imported_keys,
      ...(imported_keys.length === 0 ? { detail: "no matching env vars found" } : {})
    };
  }

  private async openai_chat_impl(xcmd: XCommandData): Promise<{ text: string }> {
    const params = ensure_json_object(xcmd?._params ?? {}, "params");
    const raw_messages = Array.isArray(params.messages) ? params.messages : undefined;
    if (!raw_messages || raw_messages.length === 0) {
      throw new XError("E_AZURE_BAD_PARAMS", "messages is required and must be a non-empty array");
    }

    const messages: AzureOpenAIChatMessage[] = [];
    for (const raw_message of raw_messages) {
      messages.push(parse_openai_message(raw_message));
    }

    const temperature = ensure_optional_number(params.temperature);
    if (temperature !== undefined && (temperature < 0 || temperature > 2)) {
      throw new XError("E_AZURE_BAD_PARAMS", "temperature must be between 0 and 2");
    }

    const settings = await read_skill_settings(this._ctx);
    const missing: string[] = [];
    if (!settings.AZURE_OPENAI_API_KEY) missing.push("AZURE_OPENAI_API_KEY");
    if (!settings.AZURE_OPENAI_ENDPOINT) missing.push("AZURE_OPENAI_ENDPOINT");
    if (!settings.AZURE_OPENAI_DEPLOYMENT) missing.push("AZURE_OPENAI_DEPLOYMENT");
    if (missing.length > 0) {
      throw new XError("E_AZURE_NOT_CONFIGURED", `Missing Azure OpenAI settings: ${missing.join(", ")}`);
    }

    const url = `${settings.AZURE_OPENAI_ENDPOINT}/openai/deployments/${encodeURIComponent(
      settings.AZURE_OPENAI_DEPLOYMENT
    )}/chat/completions?api-version=${AZURE_OPENAI_API_VERSION}`;

    let response: Response;
    try {
      response = await fetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "api-key": settings.AZURE_OPENAI_API_KEY
        },
        body: JSON.stringify({
          messages,
          ...(temperature !== undefined ? { temperature } : {})
        })
      });
    } catch (err) {
      throw new XError("E_AZURE_OPENAI_NETWORK", `Azure OpenAI chat request failed: ${sanitize_connection_error(err)}`);
    }

    let body: unknown;
    try {
      body = await response.json();
    } catch {
      throw new XError("E_AZURE_OPENAI_BAD_RESPONSE", "Azure OpenAI chat returned non-JSON response");
    }

    if (!response.ok) {
      const detail =
        (is_plain_object(body) && is_plain_object(body.error) && ensure_optional_string(body.error.message)) ??
        (is_plain_object(body) && ensure_optional_string(body.message)) ??
        `http_${response.status}`;
      throw new XError("E_AZURE_OPENAI_HTTP", `Azure OpenAI chat failed (${detail})`);
    }

    const text = extract_chat_text(body);
    if (!text) {
      throw new XError("E_AZURE_OPENAI_BAD_RESPONSE", "Azure OpenAI chat response missing assistant text");
    }
    return { text };
  }

  private async probe_openai(settings: AzureSettings): Promise<AzureConnectionProbeResult> {
    if (!settings.AZURE_OPENAI_ENDPOINT || !settings.AZURE_OPENAI_API_KEY) {
      return { ok: false, detail: "skipped: missing AZURE_OPENAI_ENDPOINT or AZURE_OPENAI_API_KEY" };
    }

    try {
      const response = await fetch(settings.AZURE_OPENAI_ENDPOINT, {
        method: "HEAD",
        headers: {
          "api-key": settings.AZURE_OPENAI_API_KEY
        }
      });

      if (response.ok) {
        return { ok: true, detail: `endpoint reachable (http_${response.status})` };
      }

      return { ok: false, detail: `endpoint probe failed (http_${response.status})` };
    } catch (err) {
      return { ok: false, detail: `endpoint probe error: ${sanitize_connection_error(err)}` };
    }
  }

  private probe_speech(settings: AzureSettings): AzureConnectionProbeResult {
    if (!settings.AZURE_SPEECH_REGION || !settings.AZURE_SPEECH_KEY) {
      return { ok: false, detail: "skipped: missing AZURE_SPEECH_REGION or AZURE_SPEECH_KEY" };
    }
    return { ok: false, detail: "skipped: no deterministic speech probe endpoint is configured in this stage" };
  }
}

/*
Manual test steps:
1) Enable skill in agent.config.json allow/enabled.
2) In ACP set Azure settings fields for this skill.
3) Call azure.status and azure.test_connection via Wormholes.
4) Confirm secrets are masked in settings.get_skill and never returned from azure.status.
*/
export const skill: XBotSkill = {
  id: AZURE_SKILL_ID,
  version: "0.1.0-alpha.0",
  name: "Azure Integration",
  description: "Optional Azure integration for XBot (settings + connectivity checks).",
  settings: AZURE_SETTINGS_META,
  capabilities: {
    network: true,
    kernel_ops: ["settings.get_skill", "settings.set_skill"]
  },
  onEnable(ctx) {
    ctx.registerModule(new AzureModule(ctx));
    ctx.log("info", "azure module registered", { skill: ctx.skill.id });
  },
  onDisable(ctx) {
    ctx.log("info", "azure skill disabled", { skill: ctx.skill.id });
  }
};

export default skill;
