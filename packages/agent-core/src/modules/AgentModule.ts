import { randomUUID } from "node:crypto";

import { XError, XModule, _x, _xlog, type XCommandData } from "@xpell/node";

import { CHANNELS_MODULE_NAME } from "./ChannelsModule.js";
import { CONVERSATIONS_MODULE_NAME } from "./ConversationsModule.js";
import { KNOWLEDGE_MODULE_NAME } from "./KnowledgeModule.js";
import { SETTINGS_MODULE_NAME } from "./SettingsModule.js";
import { USERS_MODULE_NAME } from "./UsersModule.js";
import {
  readCommandCtx,
  requireKernelCap,
  requireKernelCapOrActorRole,
  type AgentActorRole,
  type AgentCommandCtx
} from "../runtime/guards.js";
import { add_task_xdb, init_agent_tasks_xdb, list_tasks_xdb, type AgentTaskXdbScope } from "./agent-xdb.js";
import type { ConversationMessage, ConversationThread } from "../types/conversations.js";

const MODULE_NAME = "agent";
const AZURE_MODULE_NAME = "azure";
const INBOUND_HISTORY_LIMIT = 20;
const AGENT_NAME_MAX_CHARS = 80;
const AGENT_ROLE_MAX_CHARS = 120;
const AGENT_SYSTEM_PROMPT_MAX_BYTES = 8000;
const DEFAULT_AGENT_NAME = "XBot";
const DEFAULT_AGENT_ROLE = "Assistant";
const DEFAULT_AGENT_SYSTEM_PROMPT =
  "You are {agent_name} ({agent_id}), a helpful {agent_role}. Reply in the user's language; Spanish if they write Spanish, English otherwise.";
const KB_EXPORT_DEFAULT_MAX_CHARS = 8000;
const KB_EXPORT_MAX_CHARS = 24000;
const KB_BLOCKED_CUSTOMER_TEXT =
  "[KB:blocked] I can’t share internal docs verbatim. Ask a question and I’ll answer from them.";
const KB_BLOCKED_DISABLED_TEXT = "[KB:blocked] KB export is disabled. Enable it in Settings.";
const SENSITIVE_VALUE_SENTINEL = "••••••••";
const DEFAULT_KB_ID = "ruta1";
const EXFILTRATION_INTENT_SUBSTRINGS = [
  "show kb",
  "dump kb",
  "print kb",
  "export kb",
  "show your kb",
  "dump your kb",
  "print your kb",
  "export your kb",
  "knowledge base",
  "system prompt",
  "hidden instructions",
  "context",
  "sources",
  "verbatim",
  "exact text",
  "entire document",
  "ruta1_kb.md"
];

type Dict = Record<string, unknown>;

type AgentModuleOptions = {
  _version: string;
  _started_at_ms: number;
  _app_id?: string;
  _env?: string;
};

type TaskRecord = {
  _task_id: string;
  _task_type: string;
  _payload: unknown;
  _created_at: number;
};

type KbContextResult = {
  context: string;
  sources: string[];
};

type AgentHandleInboundInput = {
  channel_id: string;
  thread_id: string;
  text: string;
  user_ref?: Record<string, unknown>;
};

type AgentAnswerInput = {
  channel_id: string;
  text: string;
  thread_id?: string;
  actor_role?: "owner" | "admin" | "customer";
  user_ref?: Record<string, unknown>;
};

type AgentLanguagePolicy = "auto" | "spanish" | "english";

type AgentProfileIdentity = {
  name: string;
  role: string;
  system_prompt: string;
  language_policy: AgentLanguagePolicy;
};

type OpenAIChatMessage = {
  role: "system" | "user" | "assistant";
  content: string;
};

type KbPolicySettings = {
  allow_export: boolean;
  export_roles: Array<"owner" | "admin">;
  max_export_chars: number;
};

type SystemPromptParams = {
  agent_id: string;
  agent_name: string;
  agent_role: string;
  channel: string;
  user_display_name: string;
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

function expand_system_prompt(template: string, params: SystemPromptParams): string {
  return template
    .split("{agent_id}")
    .join(params.agent_id)
    .split("{agent_name}")
    .join(params.agent_name)
    .split("{agent_role}")
    .join(params.agent_role)
    .split("{channel}")
    .join(params.channel)
    .split("{user_display_name}")
    .join(params.user_display_name);
}

export class AgentModule extends XModule {
  static _name = MODULE_NAME;

  private _agent_id: string;
  private _version: string;
  private _started_at_ms: number;
  private _task_seq = 0;
  private _tasks = new Map<string, TaskRecord>();
  private _xdb_scope: AgentTaskXdbScope;
  private _xdb_initialized = false;

  constructor(opts: AgentModuleOptions) {
    super({ _name: MODULE_NAME });
    this._agent_id = typeof opts._app_id === "string" && opts._app_id.trim() ? opts._app_id.trim() : "xbot";
    this._version = opts._version;
    this._started_at_ms = opts._started_at_ms;
    this._xdb_scope = {
      _app_id: this._agent_id,
      _env: typeof opts._env === "string" && opts._env.trim() ? opts._env.trim() : "default"
    };
  }

  async _ping(_xcmd: XCommandData) {
    return this.ping_impl();
  }
  async _op_ping(xcmd: XCommandData) {
    return this._ping(xcmd);
  }

  async _status(_xcmd: XCommandData) {
    return this.status_impl();
  }
  async _op_status(xcmd: XCommandData) {
    return this._status(xcmd);
  }

  async _run_task(xcmd: XCommandData) {
    return this.run_task_impl(xcmd);
  }
  async _op_run_task(xcmd: XCommandData) {
    return this._run_task(xcmd);
  }

  async _handle_inbound(xcmd: XCommandData) {
    return this.handle_inbound_impl(xcmd);
  }
  async _op_handle_inbound(xcmd: XCommandData) {
    return this.handle_inbound_impl(xcmd);
  }

  async _answer(xcmd: XCommandData) {
    return this.answer_impl(xcmd);
  }
  async _op_answer(xcmd: XCommandData) {
    return this.answer_impl(xcmd);
  }

  async _get_profile(xcmd: XCommandData) {
    return this.get_profile_impl(xcmd);
  }
  async _op_get_profile(xcmd: XCommandData) {
    return this.get_profile_impl(xcmd);
  }

  async _set_profile(xcmd: XCommandData) {
    return this.set_profile_impl(xcmd);
  }
  async _op_set_profile(xcmd: XCommandData) {
    return this.set_profile_impl(xcmd);
  }

  async _reset_db(xcmd: XCommandData) {
    return this.reset_db_impl(xcmd);
  }
  async _op_reset_db(xcmd: XCommandData) {
    return this.reset_db_impl(xcmd);
  }

  async _init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }
  async _op_init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }

  private ping_impl() {
    return { ok: true, ts: Date.now() };
  }

  private status_impl() {
    return {
      status: "running",
      version: this._version,
      uptime: Date.now() - this._started_at_ms
    };
  }

  private async get_profile_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    requireKernelCapOrActorRole(ctx, "admin");

    const identity = await this.exec_read_agent_identity_settings(ctx);
    return {
      agent_id: this._agent_id,
      env: this._xdb_scope._env,
      agent_runtime_version: this._version,
      xpell_version: this._version,
      connected: true,
      name: identity.name,
      role: identity.role,
      system_prompt: identity.system_prompt,
      identity
    };
  }

  private async set_profile_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    requireKernelCapOrActorRole(ctx, "admin");

    const params = this.ensure_params(xcmd?._params);
    const patch = this.parse_agent_profile_patch(params);
    const current = await this.exec_read_agent_identity_settings(ctx);
    const next: AgentProfileIdentity = {
      ...current,
      ...patch
    };

    await this.exec_write_agent_identity_settings(ctx, next);
    return this.get_profile_impl(xcmd);
  }

  private async reset_db_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    requireKernelCapOrActorRole(ctx, "admin");

    const conv_out = await _x.execute({
      _module: CONVERSATIONS_MODULE_NAME,
      _op: "reset_storage",
      _params: { _ctx: this.forward_ctx(ctx) }
    });
    const users_out = await _x.execute({
      _module: USERS_MODULE_NAME,
      _op: "reset_storage",
      _params: { _ctx: this.forward_ctx(ctx) }
    });

    const threads_deleted = this.read_optional_count(is_plain_object(conv_out) ? conv_out.threads_deleted : undefined);
    const messages_deleted = this.read_optional_count(is_plain_object(conv_out) ? conv_out.messages_deleted : undefined);
    const users_deleted = this.read_optional_count(is_plain_object(users_out) ? users_out.users_deleted : undefined);
    const sessions_deleted = this.read_optional_count(
      is_plain_object(users_out) ? users_out.sessions_deleted : undefined
    );

    return {
      ok: true,
      threads_deleted,
      messages_deleted,
      users_deleted,
      sessions_deleted
    };
  }

  private async init_on_boot_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    await init_agent_tasks_xdb(this._xdb_scope);
    await this.hydrate_from_xdb();
    this._xdb_initialized = true;
    return {
      ok: true,
      tasks: this._tasks.size
    };
  }

  private async run_task_impl(xcmd: XCommandData) {
    const params = is_plain_object(xcmd?._params) ? xcmd._params : {};

    const task_type = typeof params.task_type === "string" ? params.task_type.trim() : "";
    if (!task_type) {
      throw new XError("E_AGENT_BAD_PARAMS", "Missing required param: task_type");
    }

    const payload = params.payload;
    if (has_function(payload)) {
      throw new XError("E_AGENT_BAD_PARAMS", "payload must be JSON-safe");
    }

    const payload_with_context = await this.inject_kb_context_if_reply(task_type, payload);

    const task_id = this.next_task_id();
    const record: TaskRecord = {
      _task_id: task_id,
      _task_type: task_type,
      _payload: payload_with_context.payload,
      _created_at: Date.now()
    };
    this._tasks.set(task_id, record);

    try {
      await this.persist_task(record);
    } catch {
      this._tasks.delete(task_id);
      throw new XError("E_AGENT_PERSIST_FAILED", "Failed to persist task");
    }

    return {
      task_id,
      accepted: true,
      ...(payload_with_context.kb ? { kb: payload_with_context.kb } : {})
    };
  }

  private async handle_inbound_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    requireKernelCap(ctx);
    const params = this.ensure_params(xcmd?._params);
    const inbound = this.parse_handle_inbound_input(params);
    const answer = await this.generate_answer(ctx, inbound);

    await this.exec_channels_send_message({
      channel_id: inbound.channel_id,
      thread_id: inbound.thread_id,
      msg: { text: answer.reply_text }
    });

    return { reply_text: answer.reply_text };
  }

  private async answer_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    requireKernelCapOrActorRole(ctx, "admin");
    const params = this.ensure_params(xcmd?._params);
    const inbound = this.parse_answer_input(params);
    return this.generate_answer(ctx, inbound);
  }

  private async generate_answer(ctx: AgentCommandCtx, inbound: AgentAnswerInput): Promise<{ reply_text: string }> {
    let thread_state: { thread: ConversationThread; messages: ConversationMessage[] } | undefined;
    let actor_role: "owner" | "admin" | "customer" =
      inbound.actor_role ??
      (ctx.actor?.role === "owner" || ctx.actor?.role === "admin" || ctx.actor?.role === "customer"
        ? ctx.actor.role
        : "customer");

    if (inbound.thread_id) {
      thread_state = await this.exec_conv_get_thread({
        thread_id: inbound.thread_id,
        limit_messages: INBOUND_HISTORY_LIMIT
      });
      if (thread_state.thread.channel.toLowerCase() !== inbound.channel_id) {
        throw new XError("E_AGENT_BAD_PARAMS", "channel_id does not match thread channel");
      }
      actor_role =
        (await this.resolve_effective_actor_role(ctx, inbound.actor_role ?? ctx.actor?.role, thread_state.thread.user_id)) ?? "customer";
    }

    if (this.is_exfiltration_intent(inbound.text)) {
      const policy = await this.exec_read_kb_policy_settings(ctx);

      if (!actor_role || !this.is_export_role(actor_role) || !policy.export_roles.includes(actor_role)) {
        return { reply_text: KB_BLOCKED_CUSTOMER_TEXT };
      }

      if (!policy.allow_export) {
        return { reply_text: KB_BLOCKED_DISABLED_TEXT };
      }

      const export_context = await this.exec_kb_build_context({ max_chars: policy.max_export_chars });
      const sensitive_values = await this.exec_collect_sensitive_values(ctx);
      const raw_text = ensure_optional_string(export_context.context) ?? "";
      const bounded_text = this.truncate_text(raw_text, policy.max_export_chars);
      const redacted_text = this.redact_text_by_values(bounded_text, sensitive_values);
      const export_payload = redacted_text.length > 0 ? redacted_text : "No KB content available.";
      const export_reply = `[KB:export] ${export_payload}`;
      return { reply_text: export_reply };
    }

    const profile = await this.exec_read_agent_identity_settings(ctx);
    const user_display_name =
      (is_plain_object(inbound.user_ref) ? ensure_optional_string(inbound.user_ref.name) : undefined) ??
      (is_plain_object(inbound.user_ref) ? ensure_optional_string(inbound.user_ref.username) : undefined) ??
      "";
    const system_prompt = expand_system_prompt(profile.system_prompt, {
      agent_id: this._agent_id,
      agent_name: profile.name,
      agent_role: profile.role,
      channel: inbound.channel_id,
      user_display_name
    });

    const kb_lang = this.detect_kb_lang(inbound.text);
    const kb_doc = await this.exec_kb_show({
      _kb_id: DEFAULT_KB_ID,
      _lang: kb_lang,
      _ctx: this.forward_ctx(ctx)
    });
    const kb_context_text = this.truncate_text(kb_doc.content, 12000);
    const has_kb_context = kb_context_text.length > 0;
    _xlog.log("[agent-core] kb context prepared for inbound reply", {
      channel_id: inbound.channel_id,
      thread_id: inbound.thread_id,
      kb_id: DEFAULT_KB_ID,
      kb_lang: kb_doc.lang,
      kb_context_chars: kb_context_text.length,
      history_messages: thread_state?.messages.length ?? 0
    });

    const messages = this.build_openai_messages({
      system_prompt,
      kb_context: has_kb_context ? kb_context_text : "",
      history: thread_state?.messages ?? [],
      latest_text: inbound.text
    });

    const chat = await this.exec_azure_openai_chat({
      messages,
      temperature: 0.2
    });
    const reply_text_raw = ensure_optional_string(chat.text);
    const reply_text = reply_text_raw;
    if (!reply_text) {
      throw new XError("E_AGENT_UPSTREAM", "azure.openai_chat returned empty text");
    }
    return { reply_text };
  }

  private async inject_kb_context_if_reply(
    task_type: string,
    payload: unknown
  ): Promise<{ payload: unknown; kb?: { sources: string[]; context_chars: number } }> {
    if (task_type !== "reply") return { payload };
    if (!is_plain_object(payload)) return { payload };

    const user_text = ensure_optional_string(payload.user_text);
    if (!user_text) return { payload };

    const kb_context = await this.exec_kb_build_context({ query: user_text, max_chars: 6000 });
    const prompt = `Knowledge Base (static docs):\n${kb_context.context}\n---\nUser: ${user_text}`;

    _xlog.log("[agent-core] kb context prepared for reply task", {
      task_type,
      kb_sources: kb_context.sources,
      kb_source_count: kb_context.sources.length,
      kb_context_chars: kb_context.context.length
    });

    return {
      payload: {
        ...payload,
        prompt,
        kb_sources: [...kb_context.sources]
      },
      kb: {
        sources: [...kb_context.sources],
        context_chars: kb_context.context.length
      }
    };
  }

  private async exec_kb_build_context(params: { query?: string; max_chars: number }): Promise<KbContextResult> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "build_context",
      _params: {
        ...(params.query ? { query: params.query } : {}),
        max_chars: params.max_chars
      }
    });
    if (!is_plain_object(out)) {
      throw new XError("E_AGENT_UPSTREAM", "kb.build_context returned invalid payload");
    }

    const context = ensure_optional_string(out.context) ?? "";
    const raw_sources = Array.isArray(out.sources) ? out.sources : [];
    const sources: string[] = [];
    for (const raw_source of raw_sources) {
      if (typeof raw_source === "string" && raw_source.trim().length > 0) {
        sources.push(raw_source.trim());
        continue;
      }
      if (is_plain_object(raw_source) && typeof raw_source.relpath === "string" && raw_source.relpath.trim().length > 0) {
        sources.push(raw_source.relpath.trim());
      }
    }

    return {
      context,
      sources
    };
  }

  private async exec_read_kb_policy_settings(ctx: AgentCommandCtx): Promise<KbPolicySettings> {
    const out = await _x.execute({
      _module: SETTINGS_MODULE_NAME,
      _op: "get",
      _params: { key: "kb", _ctx: this.forward_ctx(ctx) }
    });
    const value = is_plain_object(out) ? out.value : undefined;
    const allow_export = is_plain_object(value) && typeof value.allow_export === "boolean" ? value.allow_export : false;
    const export_roles_raw = is_plain_object(value) && Array.isArray(value.export_roles) ? value.export_roles : [];
    const export_roles = export_roles_raw
      .map((entry) => (typeof entry === "string" ? entry.trim().toLowerCase() : ""))
      .filter((entry): entry is "owner" | "admin" => entry === "owner" || entry === "admin");
    const max_export_chars_raw = is_plain_object(value) ? this.ensure_optional_positive_int(value.max_export_chars) : undefined;
    return {
      allow_export,
      export_roles: export_roles.length > 0 ? export_roles : ["owner", "admin"],
      max_export_chars: max_export_chars_raw ?? KB_EXPORT_DEFAULT_MAX_CHARS
    };
  }

  private async resolve_effective_actor_role(
    ctx: AgentCommandCtx,
    actor_role: AgentActorRole | undefined,
    thread_user_id: string
  ): Promise<"owner" | "admin" | "customer" | undefined> {
    if (actor_role === "owner" || actor_role === "admin" || actor_role === "customer") {
      return actor_role;
    }

    const out = await _x.execute({
      _module: USERS_MODULE_NAME,
      _op: "list",
      _params: {
        _limit: 500,
        _ctx: this.forward_ctx(ctx)
      }
    });
    const users = is_plain_object(out) && Array.isArray(out.items) ? out.items : [];
    for (const raw_user of users) {
      if (!is_plain_object(raw_user)) continue;
      const user_id = ensure_optional_string(raw_user._id) ?? ensure_optional_string(raw_user.user_id);
      if (user_id !== thread_user_id) continue;
      const role = ensure_optional_string(raw_user._role) ?? ensure_optional_string(raw_user.role);
      if (role === "owner" || role === "admin" || role === "customer") {
        return role;
      }
    }
    return undefined;
  }

  private async exec_collect_sensitive_values(ctx: AgentCommandCtx): Promise<string[]> {
    const out = await _x.execute({
      _module: SETTINGS_MODULE_NAME,
      _op: "get",
      _params: { key: "skills", _ctx: this.forward_ctx(ctx) }
    });
    const value = is_plain_object(out) ? out.value : undefined;
    if (!is_plain_object(value)) return [];

    const values = new Set<string>();
    this.collect_sensitive_values_recursive(value, false, values);
    return Array.from(values.values()).sort((left, right) => right.length - left.length);
  }

  private collect_sensitive_values_recursive(value: unknown, path_sensitive: boolean, out: Set<string>): void {
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (
        path_sensitive &&
        trimmed.length >= 4 &&
        trimmed !== SENSITIVE_VALUE_SENTINEL &&
        trimmed.toLowerCase() !== "null"
      ) {
        out.add(trimmed);
      }
      return;
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        this.collect_sensitive_values_recursive(item, path_sensitive, out);
      }
      return;
    }

    if (!is_plain_object(value)) return;
    for (const [key, child] of Object.entries(value)) {
      const next_sensitive = path_sensitive || this.is_sensitive_key_name(key);
      this.collect_sensitive_values_recursive(child, next_sensitive, out);
    }
  }

  private is_sensitive_key_name(key: string): boolean {
    const lower = key.trim().toLowerCase();
    if (!lower) return false;
    return (
      lower.includes("password") ||
      lower.includes("secret") ||
      lower.includes("token") ||
      lower.includes("api_key") ||
      lower.endsWith("_key") ||
      lower === "key"
    );
  }

  private redact_text_by_values(text: string, sensitive_values: string[]): string {
    let out = text;
    for (const sensitive of sensitive_values) {
      if (!sensitive || sensitive.length < 4) continue;
      out = out.split(sensitive).join("[REDACTED]");
    }
    return out;
  }

  private is_exfiltration_intent(text: string): boolean {
    const normalized = text.trim().toLowerCase();
    if (!normalized) return false;
    for (const marker of EXFILTRATION_INTENT_SUBSTRINGS) {
      if (normalized.includes(marker)) return true;
    }
    return false;
  }

  private ensure_optional_positive_int(value: unknown): number | undefined {
    if (value === undefined || value === null) return undefined;
    const parsed = Number.parseInt(String(value), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) return undefined;
    return Math.min(parsed, KB_EXPORT_MAX_CHARS);
  }

  private read_optional_count(value: unknown): number {
    const parsed = Number.parseInt(String(value ?? ""), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) return 0;
    return parsed;
  }

  private truncate_text(text: string, max_chars: number): string {
    if (!Number.isFinite(max_chars) || max_chars <= 0) return "";
    if (text.length <= max_chars) return text;
    return text.slice(0, Math.max(0, max_chars - 3)) + "...";
  }

  private is_export_role(role: "owner" | "admin" | "customer"): role is "owner" | "admin" {
    return role === "owner" || role === "admin";
  }

  private detect_kb_lang(text: string): "es" | "en" {
    const normalized = text.toLowerCase();
    const spanish_markers = [" el ", " la ", " los ", " las ", " para ", " horario", " horas", " hola", "gracias", " por "];
    let score = 0;
    for (const marker of spanish_markers) {
      if (normalized.includes(marker)) score += 1;
    }
    if (/[áéíóúñ¿¡]/.test(normalized)) score += 2;
    return score >= 2 ? "es" : "en";
  }

  private parse_agent_profile_patch(params: Dict): Partial<AgentProfileIdentity> {
    const root = is_plain_object(params.profile) ? this.ensure_params(params.profile) : params;
    const patch: Partial<AgentProfileIdentity> = {};

    if (Object.prototype.hasOwnProperty.call(root, "name")) {
      patch.name = this.ensure_bounded_non_empty_string(root.name, "name", AGENT_NAME_MAX_CHARS);
    }
    if (Object.prototype.hasOwnProperty.call(root, "role")) {
      patch.role = this.ensure_bounded_non_empty_string(root.role, "role", AGENT_ROLE_MAX_CHARS);
    }
    if (Object.prototype.hasOwnProperty.call(root, "system_prompt")) {
      const system_prompt = this.ensure_bounded_non_empty_string(root.system_prompt, "system_prompt", AGENT_SYSTEM_PROMPT_MAX_BYTES);
      if (Buffer.byteLength(system_prompt, "utf8") > AGENT_SYSTEM_PROMPT_MAX_BYTES) {
        throw new XError(
          "E_AGENT_BAD_PARAMS",
          `system_prompt exceeds ${AGENT_SYSTEM_PROMPT_MAX_BYTES} bytes`
        );
      }
      patch.system_prompt = system_prompt;
    }
    if (Object.prototype.hasOwnProperty.call(root, "language_policy")) {
      patch.language_policy = this.ensure_language_policy(root.language_policy);
    }

    return patch;
  }

  private ensure_bounded_non_empty_string(value: unknown, field_name: string, max_bytes: number): string {
    if (typeof value !== "string") {
      throw new XError("E_AGENT_BAD_PARAMS", `${field_name} must be a string`);
    }
    const trimmed = value.trim();
    if (!trimmed) {
      throw new XError("E_AGENT_BAD_PARAMS", `${field_name} must not be empty`);
    }
    if (Buffer.byteLength(trimmed, "utf8") > max_bytes) {
      throw new XError("E_AGENT_BAD_PARAMS", `${field_name} exceeds ${max_bytes} bytes`);
    }
    return trimmed;
  }

  private ensure_language_policy(value: unknown): AgentLanguagePolicy {
    if (typeof value !== "string") {
      throw new XError("E_AGENT_BAD_PARAMS", "language_policy must be a string");
    }
    const normalized = value.trim().toLowerCase();
    if (normalized === "auto" || normalized === "spanish" || normalized === "english") {
      return normalized;
    }
    throw new XError("E_AGENT_BAD_PARAMS", "language_policy must be one of: auto, spanish, english");
  }

  private async exec_read_agent_identity_settings(ctx: AgentCommandCtx): Promise<AgentProfileIdentity> {
    const out = await _x.execute({
      _module: SETTINGS_MODULE_NAME,
      _op: "get",
      _params: {
        key: "agent",
        _ctx: this.forward_ctx(ctx)
      }
    });
    const agent_value = is_plain_object(out) && is_plain_object(out.value) ? out.value : {};
    const identity_value = is_plain_object(agent_value.identity) ? agent_value.identity : {};

    const name = ensure_optional_string(agent_value.name) ?? ensure_optional_string(identity_value.name) ?? DEFAULT_AGENT_NAME;
    const role = ensure_optional_string(agent_value.role) ?? ensure_optional_string(identity_value.role) ?? DEFAULT_AGENT_ROLE;
    const system_prompt =
      ensure_optional_string(agent_value.system_prompt) ??
      ensure_optional_string(identity_value.system_prompt) ??
      DEFAULT_AGENT_SYSTEM_PROMPT;
    const language_policy_raw = ensure_optional_string(identity_value.language_policy) ?? "auto";
    const language_policy = this.ensure_language_policy(language_policy_raw);

    return {
      name,
      role,
      system_prompt,
      language_policy
    };
  }

  private async exec_write_agent_identity_settings(ctx: AgentCommandCtx, identity: AgentProfileIdentity): Promise<void> {
    const current_out = await _x.execute({
      _module: SETTINGS_MODULE_NAME,
      _op: "get",
      _params: {
        key: "agent",
        _ctx: this.forward_ctx(ctx)
      }
    });
    const current_agent = is_plain_object(current_out) && is_plain_object(current_out.value) ? current_out.value : {};
    const current_identity = is_plain_object(current_agent.identity) ? current_agent.identity : {};
    const next_agent = {
      ...current_agent,
      name: identity.name,
      role: identity.role,
      system_prompt: identity.system_prompt,
      identity: {
        ...current_identity,
        name: identity.name,
        role: identity.role,
        system_prompt: identity.system_prompt,
        language_policy: identity.language_policy
      }
    };
    await _x.execute({
      _module: SETTINGS_MODULE_NAME,
      _op: "set",
      _params: {
        key: "agent",
        value: next_agent,
        _ctx: this.forward_ctx(ctx)
      }
    });
    _xlog.log("[agent-core] agent profile updated", {
      name_chars: identity.name.length,
      role_chars: identity.role.length,
      system_prompt_chars: identity.system_prompt.length
    });
  }

  private forward_ctx(ctx: AgentCommandCtx): Dict {
    const out: Dict = {};
    if (typeof ctx.kernel_cap === "string" && ctx.kernel_cap.trim().length > 0) {
      out.kernel_cap = ctx.kernel_cap;
    }
    if (ctx.actor && is_plain_object(ctx.actor)) {
      out.actor = ctx.actor;
    }
    return out;
  }

  private ensure_params(value: unknown): Dict {
    if (value === undefined || value === null) return {};
    if (!is_plain_object(value)) {
      throw new XError("E_AGENT_BAD_PARAMS", "Expected params to be an object");
    }
    if (has_function(value)) {
      throw new XError("E_AGENT_BAD_PARAMS", "params must be JSON-safe");
    }
    return value;
  }

  private parse_handle_inbound_input(params: Dict): AgentHandleInboundInput {
    const channel_id = ensure_optional_string(params.channel_id);
    if (!channel_id) {
      throw new XError("E_AGENT_BAD_PARAMS", "Missing required param: channel_id");
    }
    const thread_id = ensure_optional_string(params.thread_id);
    if (!thread_id) {
      throw new XError("E_AGENT_BAD_PARAMS", "Missing required param: thread_id");
    }
    const text = ensure_optional_string(params.text);
    if (!text) {
      throw new XError("E_AGENT_BAD_PARAMS", "Missing required param: text");
    }
    const user_ref =
      params.user_ref !== undefined && params.user_ref !== null ? this.ensure_params(params.user_ref) : undefined;

    return {
      channel_id: channel_id.toLowerCase(),
      thread_id,
      text,
      ...(user_ref ? { user_ref } : {})
    };
  }

  private parse_answer_input(params: Dict): AgentAnswerInput {
    const channel_id = ensure_optional_string(params.channel_id);
    if (!channel_id) {
      throw new XError("E_AGENT_BAD_PARAMS", "Missing required param: channel_id");
    }
    const text = ensure_optional_string(params.text);
    if (!text) {
      throw new XError("E_AGENT_BAD_PARAMS", "Missing required param: text");
    }
    const thread_id = ensure_optional_string(params.thread_id);
    const actor_role_raw = ensure_optional_string(params.actor_role);
    const actor_role =
      actor_role_raw === "owner" || actor_role_raw === "admin" || actor_role_raw === "customer" ? actor_role_raw : undefined;
    const user_ref =
      params.user_ref !== undefined && params.user_ref !== null ? this.ensure_params(params.user_ref) : undefined;

    return {
      channel_id: channel_id.toLowerCase(),
      text,
      ...(thread_id ? { thread_id } : {}),
      ...(actor_role ? { actor_role } : {}),
      ...(user_ref ? { user_ref } : {})
    };
  }

  private build_openai_messages(params: {
    system_prompt: string;
    kb_context: string;
    history: ConversationMessage[];
    latest_text: string;
  }): OpenAIChatMessage[] {
    const system_messages: OpenAIChatMessage[] = [
      {
        role: "system",
        content: params.system_prompt
      }
    ];
    if (params.kb_context.trim().length > 0) {
      system_messages.push({
        role: "system",
        content: `Knowledge Base (read-only):\n${params.kb_context}`
      });
    }

    const history_messages: OpenAIChatMessage[] = [];
    for (const message of params.history) {
      const content = ensure_optional_string(message.text);
      if (!content) continue;
      history_messages.push({
        role: message.direction === "in" ? "user" : "assistant",
        content
      });
    }

    if (history_messages.length > 0) {
      const last = history_messages[history_messages.length - 1];
      if (last.role === "user" && last.content === params.latest_text) {
        history_messages.pop();
      }
    }

    return [...system_messages, ...history_messages, { role: "user", content: params.latest_text }];
  }

  private async exec_conv_get_thread(params: {
    thread_id: string;
    limit_messages: number;
  }): Promise<{ thread: ConversationThread; messages: ConversationMessage[] }> {
    const out = await _x.execute({
      _module: CONVERSATIONS_MODULE_NAME,
      _op: "get_thread",
      _params: params
    });
    if (!is_plain_object(out) || !is_plain_object(out.thread)) {
      throw new XError("E_AGENT_UPSTREAM", "conv.get_thread returned invalid payload");
    }
    const thread = this.parse_thread(out.thread);
    const raw_messages = Array.isArray(out.messages) ? out.messages : [];
    const messages: ConversationMessage[] = [];
    for (const raw_message of raw_messages) {
      if (!is_plain_object(raw_message)) continue;
      messages.push(this.parse_message(raw_message));
    }
    return { thread, messages };
  }

  private async exec_azure_openai_chat(params: {
    messages: OpenAIChatMessage[];
    temperature?: number;
  }): Promise<{ text: string }> {
    const out = await _x.execute({
      _module: AZURE_MODULE_NAME,
      _op: "openai_chat",
      _params: {
        messages: params.messages,
        ...(typeof params.temperature === "number" ? { temperature: params.temperature } : {})
      }
    });
    if (!is_plain_object(out)) {
      throw new XError("E_AGENT_UPSTREAM", "azure.openai_chat returned invalid payload");
    }
    const text = ensure_optional_string(out.text);
    if (!text) {
      throw new XError("E_AGENT_UPSTREAM", "azure.openai_chat.text is required");
    }
    return { text };
  }

  private async exec_kb_show(params: {
    _kb_id: string;
    _lang: "es" | "en";
    _ctx: Dict;
  }): Promise<{ lang: "es" | "en"; content: string }> {
    try {
      const out = await _x.execute({
        _module: KNOWLEDGE_MODULE_NAME,
        _op: "show",
        _params: params
      });
      if (!is_plain_object(out)) {
        throw new Error("invalid");
      }
      const lang = ensure_optional_string(out.lang) === "es" ? "es" : "en";
      return {
        lang,
        content: typeof out.content === "string" ? out.content : ""
      };
    } catch {
      return {
        lang: params._lang,
        content: ""
      };
    }
  }

  private async exec_channels_send_message(params: {
    channel_id: string;
    thread_id: string;
    msg: { text: string };
  }): Promise<void> {
    const out = await _x.execute({
      _module: CHANNELS_MODULE_NAME,
      _op: "send_message",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_AGENT_UPSTREAM", "channels.send_message returned invalid payload");
    }
  }

  private parse_thread(value: Dict): ConversationThread {
    const thread_id = ensure_optional_string(value.thread_id);
    const channel = ensure_optional_string(value.channel);
    const channel_thread_id = ensure_optional_string(value.channel_thread_id);
    const user_id = ensure_optional_string(value.user_id);
    const status = ensure_optional_string(value.status);
    const created_at = this.ensure_number(value.created_at, "thread.created_at");
    const updated_at = this.ensure_number(value.updated_at, "thread.updated_at");
    const tags = Array.isArray(value.tags) ? value.tags.filter((entry): entry is string => typeof entry === "string") : [];

    if (!thread_id || !channel || !channel_thread_id || !user_id || !status) {
      throw new XError("E_AGENT_UPSTREAM", "conv.get_thread.thread is missing required fields");
    }

    return {
      thread_id,
      channel,
      channel_thread_id,
      user_id,
      status,
      created_at,
      updated_at,
      tags
    };
  }

  private parse_message(value: Dict): ConversationMessage {
    const message_id = ensure_optional_string(value.message_id);
    const thread_id = ensure_optional_string(value.thread_id);
    const direction = ensure_optional_string(value.direction);
    const sender = ensure_optional_string(value.sender);
    const text = ensure_optional_string(value.text);
    const ts = this.ensure_number(value.ts, "message.ts");

    if (!message_id || !thread_id || !text) {
      throw new XError("E_AGENT_UPSTREAM", "conv.get_thread.messages contains invalid message");
    }
    if (direction !== "in" && direction !== "out") {
      throw new XError("E_AGENT_UPSTREAM", "conv.get_thread.messages[].direction invalid");
    }
    if (sender !== "customer" && sender !== "agent" && sender !== "admin" && sender !== "system") {
      throw new XError("E_AGENT_UPSTREAM", "conv.get_thread.messages[].sender invalid");
    }

    return {
      message_id,
      thread_id,
      direction,
      sender,
      text,
      ts
    };
  }

  private ensure_number(value: unknown, field_name: string): number {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      throw new XError("E_AGENT_UPSTREAM", `Invalid ${field_name}`);
    }
    return value;
  }

  private next_task_id(): string {
    return randomUUID();
  }

  private read_task_seq_from_id(task_id: string): number {
    const matched = /^task_(\d+)$/.exec(task_id);
    if (!matched) return 0;
    const parsed = Number.parseInt(matched[1], 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  private async hydrate_from_xdb(): Promise<void> {
    const tasks = await list_tasks_xdb(this._xdb_scope);
    this._tasks.clear();
    this._task_seq = 0;

    for (const task of tasks) {
      const normalized: TaskRecord = {
        _task_id: task._task_id,
        _task_type: task._task_type,
        _payload: task._payload,
        _created_at: task._created_at
      };
      this._tasks.set(normalized._task_id, normalized);
      this._task_seq = Math.max(this._task_seq, this.read_task_seq_from_id(normalized._task_id));
    }
  }

  private async persist_task(task: TaskRecord): Promise<void> {
    if (!this._xdb_initialized) return;
    await add_task_xdb(this._xdb_scope, {
      _task_id: task._task_id,
      _task_type: task._task_type,
      _payload: task._payload,
      _created_at: task._created_at
    });
  }
}

export default AgentModule;
