import { _xd, _xem, _xlog, XUI, XVM } from "@xpell/ui";

import {
  clear_session,
  load_session,
  save_session,
  type ACPAuthSession
} from "../services/auth.js";
import {
  AgentApiError,
  type ACPSkillAction,
  type ACPSkillConfig,
  type ACPSkillField,
  type ACPAgentProfile,
  type ACPAbout,
  type ACPAdminUser,
  type ACPAuthUser,
  type ACPConversationMessage,
  type ACPConversationSummary,
  type ACPIntentRecord,
  type ACPQAgentCase,
  type ACPQAgentLastRun,
  type ACPQAgentRun,
  type ACPUserSummary,
  type ACPSkillRuntimeStatus,
  type ACPSkill,
  type AgentApi
} from "../services/api.js";
import {
  normalize_skill_mode,
  parse_admin_chat_ids,
  validate_agent_language_policy,
  validate_admin_create_input,
  validate_admin_delete_input,
  validate_admin_update_input,
  validate_login_form,
  validate_skill_id,
  validate_telegram_settings
} from "../services/validators.js";
import {
  ACTION_ADMIN_CREATE,
  ACTION_ADMIN_DELETE,
  ACTION_ADMIN_UPDATE,
  ACTION_AGENT_RESET_DEFAULTS,
  ACTION_AGENT_SAVE,
  ACTION_QAGENT_REFRESH,
  ACTION_QAGENT_RUN_QUICK,
  ACTION_CONVERSATIONS_OPEN,
  ACTION_CONVERSATIONS_REFRESH,
  ACTION_INTENTS_EDIT,
  ACTION_INTENTS_REFRESH,
  ACTION_INTENTS_SAVE,
  ACTION_USERS_DEMOTE_CUSTOMER,
  ACTION_USERS_PROMOTE_ADMIN,
  ACTION_USERS_REFRESH,
  ACTION_USERS_SEARCH,
  ACTION_DRAWER_CLOSE,
  ACTION_DRAWER_TOGGLE,
  ACTION_LOGIN,
  ACTION_SETUP_ADMIN_CREATE,
  ACTION_LOGOUT,
  ACTION_NAVIGATE,
  ACTION_REFRESH_ABOUT,
  ACTION_SETTINGS_SERVER_URL_SAVE,
  ACTION_SETTINGS_RESET_DB,
  ACTION_SETTINGS_THEME_TOGGLE,
  ACTION_SKILLS_DISABLE,
  ACTION_SKILLS_ENABLE,
  ACTION_SKILLS_OPEN_SETTINGS,
  ACTION_SKILL_MODE_POLLING,
  ACTION_SKILL_MODE_WEBHOOK,
  ACTION_SKILL_IMPORT_ENV,
  ACTION_SKILL_POLLING_START,
  ACTION_SKILL_POLLING_STOP,
  ACTION_SKILL_SERVICE_START,
  ACTION_SKILL_SERVICE_STOP,
  ACTION_SKILL_SETTINGS_SAVE,
  ACTION_SKILL_STATUS_REFRESH,
  ACTION_SKILL_VERIFY_AZURE,
  ACTION_SKILL_VERIFY_TOKEN,
  ID_DRAWER,
  ID_DRAWER_BACKDROP,
  ID_SHELL,
  ID_TOAST,
  ID_TOPBAR,
  KEY_ABOUT,
  KEY_AGENT_PROFILE,
  KEY_QAGENT,
  KEY_CONVERSATIONS,
  KEY_INTENTS,
  KEY_USERS,
  KEY_ADMIN_USERS,
  KEY_AUTH_IS_AUTHENTICATED,
  KEY_AUTH_USER,
  KEY_UI_BOOTSTRAP_MODE,
  KEY_SKILLS,
  KEY_SKILL_SETTINGS,
  KEY_UI_ABOUT_AGENT_VERSION_TEXT,
  KEY_UI_ABOUT_CONNECTION_TEXT,
  KEY_UI_ABOUT_SERVER_URL_TEXT,
  KEY_UI_ABOUT_XPELL_VERSION_TEXT,
  KEY_UI_AGENT_CONNECTED_TEXT,
  KEY_UI_AGENT_ENV_TEXT,
  KEY_UI_AGENT_FORM_SUMMARY_TEXT,
  KEY_UI_QAGENT_FAILURES_TABLE_ROWS,
  KEY_UI_QAGENT_RUN_TABLE_ROWS,
  KEY_UI_QAGENT_STATUS_TEXT,
  KEY_UI_CONVERSATION_DETAIL_HEADER_TEXT,
  KEY_UI_CONVERSATION_DETAIL_STATUS_TEXT,
  KEY_UI_CONVERSATION_MESSAGES_TABLE_ROWS,
  KEY_UI_CONVERSATION_ROUTE_ARGS,
  KEY_UI_CONVERSATIONS_STATUS_TEXT,
  KEY_UI_CONVERSATIONS_TABLE_ROWS,
  KEY_UI_INTENT_FORM_SUMMARY_TEXT,
  KEY_UI_INTENT_SELECTED_ID,
  KEY_UI_INTENT_SELECTED_TEXT,
  KEY_UI_INTENTS_STATUS_TEXT,
  KEY_UI_INTENTS_TABLE_ROWS,
  KEY_UI_AGENT_ID_TEXT,
  KEY_UI_AGENT_RUNTIME_VERSION_TEXT,
  KEY_UI_AGENT_XPELL_VERSION_TEXT,
  KEY_UI_ADMIN_USERS_LIST_TEXT,
  KEY_UI_ADMIN_USERS_TABLE_ROWS,
  KEY_UI_DRAWER_OPEN,
  KEY_UI_LOGIN_MESSAGE,
  KEY_UI_ROUTE,
  KEY_UI_SETTINGS_SERVER_URL_TEXT,
  KEY_UI_SETTINGS_NOTICE,
  KEY_UI_SETTINGS_THEME_TEXT,
  KEY_UI_SKILL_ACTION_RESULT_TEXT,
  KEY_UI_SKILLS_LIST_TEXT,
  KEY_UI_SKILLS_TABLE_ROWS,
  KEY_UI_SKILL_ADMIN_CHAT_IDS,
  KEY_UI_SKILL_BOT_TOKEN,
  KEY_UI_SKILL_MODE,
  KEY_UI_SKILL_RUNTIME_STATUS_TEXT,
  KEY_UI_SKILL_SELECTED_ID,
  KEY_UI_SKILL_ROUTE_ARGS,
  KEY_UI_SKILL_SELECTED_TEXT,
  KEY_UI_SKILL_SETTINGS_SUMMARY_TEXT,
  KEY_UI_STATUS_MESSAGE,
  KEY_UI_THEME,
  KEY_UI_USERS_LIST_TEXT,
  KEY_UI_USERS_STATUS_TEXT,
  KEY_UI_USERS_TABLE_ROWS,
  REGION_MAIN,
  ROUTE_ABOUT,
  ROUTE_AGENT,
  ROUTE_QAGENT,
  ROUTE_CONVERSATION_DETAILS,
  ROUTE_CONVERSATIONS,
  ROUTE_INTENTS,
  ROUTE_LOGIN,
  ROUTE_SETUP_ADMIN,
  ROUTE_SETTINGS,
  ROUTE_SKILLS,
  ROUTE_SKILL_SETTINGS,
  ROUTE_USERS,
  SOURCE_API,
  SOURCE_AUTH,
  SOURCE_BOOT,
  SOURCE_DRAWER,
  SOURCE_NAV,
  SOURCE_SETTINGS,
  SOURCE_UI,
  UI_ACTION_COMMAND,
  UI_COMMAND_OBJECT_IDS
} from "../state/xd_keys.js";

type ActionParams = Record<string, unknown>;

type UICommandsOptions = {
  api: AgentApi;
};

type UICommandsRuntime = {
  register: () => void;
  bootstrap: () => Promise<void>;
};

const THEME_STORAGE_KEY = "acp.theme";
const MASK_SENTINEL = "••••••••";
const AZURE_SKILL_ID = "@xpell/agent-skill-azure";
const DEFAULT_AGENT_LANGUAGE_POLICY = "auto";
const DEFAULT_AGENT_NAME = "XBot";
const DEFAULT_AGENT_ROLE = "Assistant";
const DEFAULT_AGENT_SYSTEM_PROMPT =
  "You are {agent_name} ({agent_id}), a helpful {agent_role}. Reply in the user's language; Spanish if they write Spanish, English otherwise.";
type AgentLanguagePolicy = "auto" | "spanish" | "english";
type ToastVariant = "default" | "success" | "error" | "warn" | "info";

function as_text(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function as_error_text(value: unknown): string {
  if (value instanceof Error) return value.message;
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function deep_clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function split_path(path_value: string): string[] {
  return path_value
    .split(".")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

function get_by_path(obj: unknown, dotted_path: string): unknown {
  const parts = split_path(dotted_path);
  if (parts.length === 0) return obj;
  let cursor: unknown = obj;
  for (const key of parts) {
    if (!cursor || typeof cursor !== "object" || Array.isArray(cursor)) return undefined;
    cursor = (cursor as Record<string, unknown>)[key];
  }
  return cursor;
}

function set_by_path(obj: Record<string, unknown>, dotted_path: string, value: unknown): void {
  const parts = split_path(dotted_path);
  if (parts.length === 0) return;
  let cursor: Record<string, unknown> = obj;
  for (let idx = 0; idx < parts.length - 1; idx += 1) {
    const key = parts[idx];
    const current = cursor[key];
    if (!current || typeof current !== "object" || Array.isArray(current)) {
      cursor[key] = {};
    }
    cursor = cursor[key] as Record<string, unknown>;
  }
  cursor[parts[parts.length - 1]] = deep_clone(value);
}

function values_equal(left: unknown, right: unknown): boolean {
  return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
}

function parse_string_list(text: string): string[] {
  return text
    .split(/\r?\n|,/)
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

function mask_secret_display(masked: boolean): string {
  return masked ? MASK_SENTINEL : "";
}

function read_storage(): Storage | null {
  if (typeof window === "undefined") return null;
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

function read_stored_theme(): "dark" | "light" {
  const storage = read_storage();
  if (!storage) return "dark";
  const raw = storage.getItem(THEME_STORAGE_KEY);
  return raw === "light" ? "light" : "dark";
}

function save_theme(theme: "dark" | "light"): void {
  const storage = read_storage();
  if (!storage) return;
  storage.setItem(THEME_STORAGE_KEY, theme);
}

function set_xdata(key: string, value: unknown, source: string): void {
  _xd.set(key, value, { source });
}

function read_input_value(object_id: string, trim = true): string {
  const object_ref = XUI.getObject(object_id) as { dom?: { value?: unknown } } | undefined;
  const raw_value = object_ref?.dom?.value;
  if (typeof raw_value !== "string") return "";
  return trim ? raw_value.trim() : raw_value;
}

function write_input_value(object_id: string, value: string): void {
  const object_ref = XUI.getObject(object_id) as { dom?: { value?: unknown } } | undefined;
  if (!object_ref?.dom) return;
  object_ref.dom.value = value;
}

function set_login_message(message: string, source: string): void {
  set_xdata(KEY_UI_LOGIN_MESSAGE, message, source);
}

function show_toast(message: string, variant: ToastVariant): void {
  const toast = XUI.getObject(ID_TOAST) as
    | {
        _text?: string;
        _variant?: ToastVariant;
        _auto_close_ms?: number;
        _open?: boolean;
        open?: () => void;
        close?: () => void;
        setOpen?: (open: boolean) => void;
      }
    | undefined;

  if (!toast) return;

  if (!message.trim()) {
    if (typeof toast.close === "function") {
      toast.close();
      return;
    }
    if (typeof toast.setOpen === "function") {
      toast.setOpen(false);
      return;
    }
    toast._open = false;
    return;
  }

  toast._text = message;
  toast._variant = variant;
  toast._auto_close_ms = 3200;

  if (typeof toast.open === "function") {
    toast.open();
    return;
  }
  if (typeof toast.setOpen === "function") {
    toast.setOpen(true);
    return;
  }
  toast._open = true;
}

function set_status_message(message: string, source: string, variant: ToastVariant = "info"): void {
  set_xdata(KEY_UI_STATUS_MESSAGE, message, source);
  show_toast(message, variant);
}

function render_about(about: ACPAbout, source: string): void {
  set_xdata(KEY_ABOUT, deep_clone(about), source);
  set_xdata(KEY_UI_ABOUT_AGENT_VERSION_TEXT, `Agent version: ${about.agent_version}`, source);
  set_xdata(KEY_UI_ABOUT_XPELL_VERSION_TEXT, `Xpell version: ${about.xpell_version}`, source);
  set_xdata(
    KEY_UI_ABOUT_CONNECTION_TEXT,
    `Running status: ${about.connected ? "connected" : "disconnected"}`,
    source
  );

  const server_url = typeof about.server_url === "string" && about.server_url.trim() ? about.server_url : "n/a";
  set_xdata(KEY_UI_ABOUT_SERVER_URL_TEXT, `Server URL: ${server_url}`, source);
}

function normalize_agent_language_policy(value: unknown): AgentLanguagePolicy {
  const normalized = as_text(value).toLowerCase();
  if (normalized === "spanish") return "spanish";
  if (normalized === "english") return "english";
  return "auto";
}

function render_agent_profile(profile: ACPAgentProfile, source: string): void {
  const identity = profile.identity ?? {
    name: DEFAULT_AGENT_NAME,
    role: DEFAULT_AGENT_ROLE,
    system_prompt: DEFAULT_AGENT_SYSTEM_PROMPT,
    language_policy: DEFAULT_AGENT_LANGUAGE_POLICY
  };
  const language_policy = normalize_agent_language_policy(identity.language_policy);
  const normalized_profile: ACPAgentProfile = {
    ...profile,
    identity: {
      name: identity.name ?? DEFAULT_AGENT_NAME,
      role: identity.role ?? DEFAULT_AGENT_ROLE,
      system_prompt: identity.system_prompt ?? DEFAULT_AGENT_SYSTEM_PROMPT,
      language_policy
    }
  };

  set_xdata(KEY_AGENT_PROFILE, deep_clone(normalized_profile), source);
  set_xdata(KEY_UI_AGENT_ID_TEXT, `Agent ID: ${profile.agent_id || "unknown"}`, source);
  set_xdata(KEY_UI_AGENT_ENV_TEXT, `Env: ${profile.env || "default"}`, source);
  set_xdata(KEY_UI_AGENT_RUNTIME_VERSION_TEXT, `Agent runtime version: ${profile.agent_runtime_version || "unknown"}`, source);
  set_xdata(KEY_UI_AGENT_XPELL_VERSION_TEXT, `Xpell version: ${profile.xpell_version || "unknown"}`, source);
  set_xdata(KEY_UI_AGENT_CONNECTED_TEXT, `Connected status: ${profile.connected ? "connected" : "disconnected"}`, source);

  write_input_value("agent-name-input", normalized_profile.identity.name);
  write_input_value("agent-role-input", normalized_profile.identity.role);
  write_input_value("agent-system-prompt-input", normalized_profile.identity.system_prompt);
  write_input_value("agent-language-policy-select", normalized_profile.identity.language_policy);

  const summary = [
    `name: ${normalized_profile.identity.name || "(empty)"}`,
    `role: ${normalized_profile.identity.role || "(empty)"}`,
    `language_policy: ${normalized_profile.identity.language_policy}`,
    `system_prompt_chars: ${normalized_profile.identity.system_prompt.length}`
  ].join("\n");

  set_xdata(KEY_UI_AGENT_FORM_SUMMARY_TEXT, summary, source);
}

function render_admin_users(users: ACPAdminUser[], source: string): void {
  set_xdata(KEY_ADMIN_USERS, deep_clone(users), source);

  const rows = users.map((user) => ({
    id: user.id,
    name: user.name,
    username: user.username,
    role: user.role
  }));

  set_xdata(KEY_UI_ADMIN_USERS_TABLE_ROWS, rows, source);
  set_xdata(KEY_UI_ADMIN_USERS_LIST_TEXT, users.length > 0 ? `rows=${users.length}` : "(no admin users)", source);
}

function format_ts(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return "n/a";
  try {
    return new Date(value).toISOString();
  } catch {
    return "n/a";
  }
}

function short_id(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) return "";
  if (trimmed.length <= 12) return trimmed;
  return `${trimmed.slice(0, 8)}...${trimmed.slice(-4)}`;
}

function map_users_table_rows(items: ACPUserSummary[]): Array<Record<string, string>> {
  return items.map((user) => ({
    raw_user_id: user.user_id,
    display: user.display_id ? `${user.display_name} (${user.display_id})` : `${user.display_name} (${short_id(user.user_id)})`,
    role: user.role,
    channels_text: user.channels.length > 0 ? user.channels.join(", ") : "(none)",
    updated_text: format_ts(user.updated_at),
    action: ""
  }));
}

function map_conversations_table_rows(items: ACPConversationSummary[]): Array<Record<string, string>> {
  return items.map((thread) => ({
    raw_thread_id: thread.thread_id,
    thread_id: short_id(thread.thread_id),
    channel: thread.channel,
    channel_thread_id: short_id(thread.channel_thread_id),
    user_id: short_id(thread.user_id),
    status: thread.status,
    updated_text: format_ts(thread.updated_at),
    action: ""
  }));
}

function truncate_cell_text(value: string, max_chars = 140): string {
  const trimmed = value.trim();
  if (!trimmed) return "";
  if (trimmed.length <= max_chars) return trimmed;
  return `${trimmed.slice(0, Math.max(0, max_chars - 3))}...`;
}

function map_conversation_messages_table_rows(items: ACPConversationMessage[]): Array<Record<string, string>> {
  return items.map((message) => ({
    raw_message_id: message.message_id,
    time_text: format_ts(message.ts),
    direction: message.direction,
    from: message.direction === "out" ? "agent" : message.sender || "customer",
    text: truncate_cell_text(message.text)
  }));
}

function format_percent(value: number): string {
  if (!Number.isFinite(value)) return "0%";
  return `${Math.round(Math.min(1, Math.max(0, value)) * 100)}%`;
}

function map_qagent_run_table_rows(run: ACPQAgentRun | null): Array<Record<string, string>> {
  if (!run) return [];
  return [
    {
      raw_run_id: run.run_id,
      run_id: run.run_id,
      created_text: format_ts(run.created_at),
      cases_text: String(run.totals.cases_total),
      pass_rate_text: format_percent(run.totals.pass_rate),
      avg_score_text: run.totals.avg_score.toFixed(2)
    }
  ];
}

function map_qagent_failures_table_rows(items: ACPQAgentCase[]): Array<Record<string, string>> {
  return items.slice(0, 10).map((item) => ({
    raw_case_id: item.case_id,
    case_id: item.case_id,
    question: truncate_cell_text(item.question, 96),
    expected_text:
      item.expected_facts.length > 0 ? truncate_cell_text(item.expected_facts.join(" | "), 96) : "(none)",
    score_text: item.score.toFixed(2)
  }));
}

function map_intents_table_rows(items: ACPIntentRecord[]): Array<Record<string, string>> {
  return items.map((intent) => ({
    raw_intent_id: intent.intent_id,
    enabled: intent.enabled ? "yes" : "no",
    intent_id: intent.intent_id,
    title: intent.title,
    roles_text: intent.roles_allowed.length > 0 ? intent.roles_allowed.join(", ") : "(none)",
    channels_text: intent.channels_allowed.length > 0 ? intent.channels_allowed.join(", ") : "(all)",
    priority: String(intent.priority),
    skill_id: intent.skill_id,
    action: ""
  }));
}

function render_conversations_page(items: ACPConversationSummary[], source: string, status_text: string): void {
  set_xdata(KEY_CONVERSATIONS, deep_clone(items), source);
  set_xdata(KEY_UI_CONVERSATIONS_STATUS_TEXT, status_text, source);
  set_xdata(KEY_UI_CONVERSATIONS_TABLE_ROWS, map_conversations_table_rows(items), source);
}

function render_qagent_page(last_run: ACPQAgentLastRun, source: string, status_text: string): void {
  set_xdata(KEY_QAGENT, deep_clone(last_run), source);
  set_xdata(KEY_UI_QAGENT_STATUS_TEXT, status_text, source);
  set_xdata(KEY_UI_QAGENT_RUN_TABLE_ROWS, map_qagent_run_table_rows(last_run.run), source);
  set_xdata(KEY_UI_QAGENT_FAILURES_TABLE_ROWS, map_qagent_failures_table_rows(last_run.top_failures), source);
}

function render_conversation_detail(
  thread: ACPConversationSummary | null,
  messages: ACPConversationMessage[],
  source: string,
  status_text: string
): void {
  const header = thread
    ? [
        `Channel: ${thread.channel}`,
        `Channel Thread: ${thread.channel_thread_id}`,
        `User: ${thread.user_id}`
      ].join(" | ")
    : "Conversation not found.";
  const summary = thread
    ? [
        `Status: ${thread.status}`,
        `Updated: ${format_ts(thread.updated_at)}`,
        `Messages: ${messages.length}`
      ].join(" | ")
    : status_text;

  set_xdata(KEY_UI_CONVERSATION_DETAIL_HEADER_TEXT, header, source);
  set_xdata(KEY_UI_CONVERSATION_DETAIL_STATUS_TEXT, thread ? summary : status_text, source);
  set_xdata(KEY_UI_CONVERSATION_MESSAGES_TABLE_ROWS, map_conversation_messages_table_rows(messages), source);
}

function render_intents_page(items: ACPIntentRecord[], source: string, status_text: string): void {
  set_xdata(KEY_INTENTS, deep_clone(items), source);
  set_xdata(KEY_UI_INTENTS_STATUS_TEXT, status_text, source);
  set_xdata(KEY_UI_INTENTS_TABLE_ROWS, map_intents_table_rows(items), source);
}

function apply_users_filter(source: string, status_text: string, query?: string): void {
  const all_users = ((_xd.get(KEY_USERS) as ACPUserSummary[] | undefined) ?? []).map((user) => deep_clone(user));
  const needle = (query ?? "").trim().toLowerCase();
  const filtered = needle
    ? all_users.filter((user) => {
        if (user.user_id.toLowerCase().includes(needle)) return true;
        if (user.display_id && user.display_id.toLowerCase().includes(needle)) return true;
        if (user.display_name.toLowerCase().includes(needle)) return true;
        if (user.role.toLowerCase().includes(needle)) return true;
        for (const channel of user.channels) {
          if (channel.toLowerCase().includes(needle)) return true;
        }
        return false;
      })
    : all_users;

  set_xdata(KEY_UI_USERS_STATUS_TEXT, status_text, source);
  set_xdata(KEY_UI_USERS_TABLE_ROWS, map_users_table_rows(filtered), source);
  set_xdata(KEY_UI_USERS_LIST_TEXT, filtered.length > 0 ? `rows=${filtered.length}` : "(no users)", source);
}

function render_users_page(items: ACPUserSummary[], source: string, status_text: string, query?: string): void {
  set_xdata(KEY_USERS, deep_clone(items), source);
  apply_users_filter(source, status_text, query);
}

function render_skills(skills: ACPSkill[], source: string): void {
  set_xdata(KEY_SKILLS, deep_clone(skills), source);

  const rows = skills.map((skill) => ({
    id: skill.id,
    version: skill.version ? `v${skill.version}` : "n/a",
    enabled: skill.enabled ? "yes" : "no",
    status: skill.status,
    error: skill.error?.trim() ? skill.error.trim() : "(none)"
  }));

  const text =
    skills.length > 0
      ? skills
          .map(
            (skill) =>
              `- ${skill.id} | v${skill.version} | enabled:${skill.enabled ? "yes" : "no"} | status:${skill.status}`
          )
          .join("\n")
      : "(no skills)";

  set_xdata(KEY_UI_SKILLS_TABLE_ROWS, rows, source);
  set_xdata(KEY_UI_SKILLS_LIST_TEXT, text, source);
}

function apply_dom_theme(theme: string): void {
  if (typeof document === "undefined") return;
  const normalized = theme === "light" ? "light" : "dark";
  document.documentElement.setAttribute("data-theme", normalized);
  document.body.setAttribute("data-theme", normalized);
}

function render_theme(theme: string, source: string): void {
  const normalized = theme === "light" ? "light" : "dark";
  set_xdata(KEY_UI_THEME, normalized, source);
  set_xdata(KEY_UI_SETTINGS_THEME_TEXT, `Theme: ${normalized}`, source);
  apply_dom_theme(normalized);
  save_theme(normalized);
}

function render_settings_server_url(server_url: string, source: string): void {
  const normalized = server_url.trim() || "n/a";
  set_xdata(KEY_UI_SETTINGS_SERVER_URL_TEXT, `ACP Server URL: ${normalized}`, source);
}

function render_skill_settings(skill: ACPSkill | undefined, config: ACPSkillConfig, source: string): void {
  const skill_id = config.skill_id;
  const current = (_xd.get(KEY_SKILL_SETTINGS) as Record<string, Record<string, unknown>> | undefined) ?? {};
  const next = {
    ...current,
    [skill_id]: deep_clone(config.settings)
  };

  set_xdata(KEY_SKILL_SETTINGS, next, source);
  set_xdata(KEY_UI_SKILL_SELECTED_ID, skill_id, source);
  set_xdata(
    KEY_UI_SKILL_SELECTED_TEXT,
    `Selected skill: ${skill?.name?.trim() || skill_id}`,
    source
  );

  const schema_title = config.schema?.title?.trim() || "General";
  const summary = [
    `skill_id: ${skill_id}`,
    `version: ${skill?.version || "unknown"}`,
    `status: ${skill?.status || "unknown"}`,
    `enabled: ${skill?.enabled === true ? "yes" : "no"}`,
    `schema: ${schema_title} (${config.schema?.fields.length ?? 0} fields)`,
    `actions: ${config.actions.length}`
  ].join("\n");

  set_xdata(KEY_UI_SKILL_SETTINGS_SUMMARY_TEXT, summary, source);
  set_xdata(KEY_UI_SKILL_ACTION_RESULT_TEXT, "Action result: (none)", source);
}

function render_skill_runtime_status(status: ACPSkillRuntimeStatus, source: string): void {
  if (!status.available) {
    const reason = status.error ? ` (${status.error})` : "";
    set_xdata(KEY_UI_SKILL_RUNTIME_STATUS_TEXT, `Polling: unavailable${reason}`, source);
    return;
  }
  const mode = as_text(status.mode) || "polling";
  set_xdata(KEY_UI_SKILL_RUNTIME_STATUS_TEXT, `Polling: ${status.running ? "running" : "stopped"} (mode: ${mode})`, source);
}

function set_auth_state(is_authenticated: boolean, user: ACPAuthUser | null, source: string): void {
  set_xdata(KEY_AUTH_IS_AUTHENTICATED, is_authenticated, source);
  set_xdata(KEY_AUTH_USER, user ? deep_clone(user) : null, source);
}

function apply_drawer_state(open: boolean): void {
  const drawer = XUI.getObject(ID_DRAWER) as { addClass: (c: string) => void; removeClass: (c: string) => void } | undefined;
  const backdrop = XUI.getObject(ID_DRAWER_BACKDROP) as
    | { addClass: (c: string) => void; removeClass: (c: string) => void }
    | undefined;

  if (open) {
    drawer?.addClass("is-open");
    backdrop?.addClass("is-open");
  } else {
    drawer?.removeClass("is-open");
    backdrop?.removeClass("is-open");
  }
}

function set_drawer_open(open: boolean, source: string): void {
  set_xdata(KEY_UI_DRAWER_OPEN, open, source);
  apply_drawer_state(open);
}

function apply_shell_state(): void {
  const shell = XUI.getObject(ID_SHELL) as { addClass: (c: string) => void; removeClass: (c: string) => void } | undefined;
  const topbar = XUI.getObject(ID_TOPBAR) as { show: () => void; hide: () => void } | undefined;

  const is_authenticated = _xd.get(KEY_AUTH_IS_AUTHENTICATED) === true;

  if (is_authenticated) {
    shell?.removeClass("acp-shell-no-auth");
    topbar?.show();
  } else {
    shell?.addClass("acp-shell-no-auth");
    topbar?.hide();
    set_drawer_open(false, SOURCE_UI);
  }
}

export function create_ui_commands(opts: UICommandsOptions): UICommandsRuntime {
  const { api } = opts;
  let events_bound = false;
  const skill_config_cache = new Map<string, { skill?: ACPSkill; config: ACPSkillConfig }>();

  const set_bootstrap_mode = (enabled: boolean, source: string): void => {
    set_xdata(KEY_UI_BOOTSTRAP_MODE, enabled, source);
  };

  const is_skill_config_route = (route: string): boolean => {
    const trimmed = route.trim();
    return trimmed === ROUTE_SKILL_SETTINGS || trimmed.startsWith(`${ROUTE_SKILLS}/`);
  };

  const decode_skill_value = (value: unknown): string => {
    if (typeof value !== "string") return "";
    const trimmed = value.trim();
    if (!trimmed) return "";
    try {
      return decodeURIComponent(trimmed).trim();
    } catch {
      return trimmed;
    }
  };

  const read_skill_id_from_route_args = (): string => {
    const args = _xd.get(KEY_UI_SKILL_ROUTE_ARGS);
    if (!args || typeof args !== "object") return "";
    return decode_skill_value((args as { skill_id?: unknown }).skill_id);
  };

  const is_conversation_detail_route = (route: string): boolean => {
    const trimmed = route.trim();
    return trimmed === ROUTE_CONVERSATION_DETAILS || trimmed.startsWith(`${ROUTE_CONVERSATIONS}/`);
  };

  const decode_thread_value = (value: unknown): string => {
    if (typeof value !== "string") return "";
    const trimmed = value.trim();
    if (!trimmed) return "";
    try {
      return decodeURIComponent(trimmed).trim();
    } catch {
      return trimmed;
    }
  };

  const read_thread_id_from_route_args = (): string => {
    const args = _xd.get(KEY_UI_CONVERSATION_ROUTE_ARGS);
    if (!args || typeof args !== "object") return "";
    return decode_thread_value((args as { thread_id?: unknown }).thread_id);
  };

  const decode_thread_id_from_route = (route: string): string => {
    const trimmed = route.trim();
    if (trimmed === ROUTE_CONVERSATION_DETAILS) {
      return read_thread_id_from_route_args();
    }
    if (!trimmed.startsWith(`${ROUTE_CONVERSATIONS}/`)) return "";
    return decode_thread_value(trimmed.slice(`${ROUTE_CONVERSATIONS}/`.length));
  };

  const decode_skill_id_from_route = (route: string): string => {
    const trimmed = route.trim();
    if (trimmed === ROUTE_SKILL_SETTINGS) {
      return read_skill_id_from_route_args();
    }
    if (!trimmed.startsWith(`${ROUTE_SKILLS}/`)) return "";
    const raw = trimmed.slice(`${ROUTE_SKILLS}/`.length);
    return decode_skill_value(raw);
  };

  const find_skill_record = (skill_id: string): ACPSkill | undefined => {
    const skills = (_xd.get(KEY_SKILLS) as ACPSkill[] | undefined) ?? [];
    return skills.find((skill) => skill.id === skill_id);
  };

  const field_dom_id = (skill_id: string, field: ACPSkillField): string => {
    const safe_skill = skill_id.replace(/[^a-zA-Z0-9_-]+/g, "_");
    const safe_key = field.key.replace(/[^a-zA-Z0-9_-]+/g, "_");
    return `skillcfg-${safe_skill}-${safe_key}`;
  };

  const resolve_skill_host = (object_id: string): HTMLElement | null => {
    const object_ref = XUI.getObject(object_id) as { dom?: unknown } | undefined;
    return object_ref?.dom instanceof HTMLElement ? object_ref.dom : null;
  };

  const render_skill_fields_dom = (skill_id: string, config: ACPSkillConfig): void => {
    const host = resolve_skill_host("skill-config-fields-host");
    if (!host) return;
    host.replaceChildren();

    const fields = config.schema?.fields ?? [];
    if (fields.length === 0) {
      host.textContent = "This skill does not declare configurable fields.";
      return;
    }

    const form = document.createElement("div");
    form.className = "acp-dynamic-form";

    for (const field of fields) {
      const row = document.createElement("div");
      row.className = "acp-dynamic-field";

      const label = document.createElement("label");
      label.className = "acp-form-title";
      label.htmlFor = field_dom_id(skill_id, field);
      label.textContent = field.label;
      row.append(label);

      const current_value = get_by_path(config.settings, field.key);
      const masked = config.masked[field.key] === true;
      const control_id = field_dom_id(skill_id, field);

      if (field.type === "boolean") {
        const checkbox = document.createElement("input");
        checkbox.id = control_id;
        checkbox.type = "checkbox";
        checkbox.className = "acp-checkbox";
        checkbox.checked = current_value === true;
        row.append(checkbox);
      } else if (field.type === "select") {
        const select = document.createElement("select");
        select.id = control_id;
        select.className = "acp-input";
        for (const option of field.options ?? []) {
          const option_el = document.createElement("option");
          option_el.value = typeof option.value === "string" ? option.value : JSON.stringify(option.value);
          option_el.textContent = option.label;
          select.append(option_el);
        }
        const next_value =
          typeof current_value === "string"
            ? current_value
            : current_value === undefined
              ? ""
              : JSON.stringify(current_value);
        if (next_value) select.value = next_value;
        row.append(select);
      } else if (field.type === "string_list") {
        const textarea = document.createElement("textarea");
        textarea.id = control_id;
        textarea.className = "acp-input acp-textarea";
        textarea.placeholder = field.placeholder || "one value per line";
        textarea.value = Array.isArray(current_value)
          ? current_value.map((entry) => String(entry)).join("\n")
          : typeof current_value === "string"
            ? current_value
            : "";
        row.append(textarea);
      } else {
        const input = document.createElement("input");
        input.id = control_id;
        input.className = "acp-input";
        input.type = field.secret === true || masked ? "password" : field.type === "number" ? "number" : "text";
        input.placeholder =
          field.secret === true || masked
            ? mask_secret_display(masked)
            : field.placeholder || "";
        if (!(field.secret === true || masked)) {
          input.value =
            typeof current_value === "string" || typeof current_value === "number"
              ? String(current_value)
              : "";
        }
        row.append(input);
      }

      if (field.help && field.help.trim()) {
        const help = document.createElement("div");
        help.className = "acp-note";
        help.textContent = field.help;
        row.append(help);
      }

      form.append(row);
    }

    host.append(form);
  };

  const render_skill_actions_dom = (skill_id: string, actions: ACPSkillAction[]): void => {
    const host = resolve_skill_host("skill-config-actions-host");
    if (!host) return;
    host.replaceChildren();

    if (actions.length === 0) {
      host.textContent = "No actions available for this skill.";
      return;
    }

    const wrap = document.createElement("div");
    wrap.className = "acp-inline-actions";

    const read_dynamic_skill_input_value = (field_key: string): string => {
      const safe_skill = skill_id.replace(/[^a-zA-Z0-9_-]+/g, "_");
      const control = document.getElementById(`skillcfg-${safe_skill}-${field_key}`) as HTMLInputElement | null;
      return typeof control?.value === "string" ? control.value.trim() : "";
    };

    const run_action = async (action: ACPSkillAction): Promise<void> => {
      if (action.confirm && typeof window !== "undefined") {
        const confirmed = window.confirm(`${action.confirm.title}\n\n${action.confirm.body}`);
        if (!confirmed) return;
      }

      if (skill_id === "@xpell/agent-skill-telegram" && action.id === "verify_token") {
        const input_token = read_dynamic_skill_input_value("bot_token");
        const token_to_verify =
          input_token.length > 0 && input_token !== MASK_SENTINEL ? input_token : undefined;
        const result = await api.skills.verifyToken(skill_id, token_to_verify);
        set_xdata(
          KEY_UI_SKILL_ACTION_RESULT_TEXT,
          `Action result:\n${JSON.stringify(result, null, 2)}`,
          SOURCE_SETTINGS
        );

        if (result.valid) {
          const username = as_text(result.bot?.username);
          const bot_id = as_text(result.bot?.id);
          const bot_text =
            username.length > 0
              ? `@${username}${bot_id ? ` (id:${bot_id})` : ""}`
              : bot_id
                ? `id:${bot_id}`
                : "bot";
          const source = result.source === "input" ? "input token" : "configured token";
          set_status_message(`Token valid (${source}): ${bot_text}.`, SOURCE_SETTINGS, "success");
        } else {
          const source = result.source === "input" ? "input token" : "configured token";
          const reason = as_text(result.error) || "invalid token";
          set_status_message(`Token invalid (${source}): ${reason}.`, SOURCE_SETTINGS, "error");
        }
        return;
      }

      const params: Record<string, unknown> = {};
      for (const field of action.params_schema?.fields ?? []) {
        const label = `${action.label}: ${field.label}`;
        const default_value =
          field.type === "boolean"
            ? "false"
            : field.type === "select"
              ? typeof field.options?.[0]?.value === "string"
                ? String(field.options?.[0]?.value ?? "")
                : ""
              : "";
        const raw =
          typeof window === "undefined" ? default_value : window.prompt(label, default_value);
        if (raw === null) return;

        if (field.type === "boolean") {
          set_by_path(params, field.key, raw.trim().toLowerCase() === "true");
          continue;
        }
        if (field.type === "number") {
          const parsed = Number(raw);
          if (!Number.isFinite(parsed)) {
            set_status_message(`Invalid number for ${field.label}.`, SOURCE_SETTINGS, "error");
            return;
          }
          set_by_path(params, field.key, parsed);
          continue;
        }
        if (field.type === "string_list") {
          set_by_path(params, field.key, parse_string_list(raw));
          continue;
        }
        if (field.type === "select") {
          set_by_path(params, field.key, raw);
          continue;
        }
        set_by_path(params, field.key, raw);
      }

      const result = await api.exec(action.op.module, action.op.op, params);
      set_xdata(
        KEY_UI_SKILL_ACTION_RESULT_TEXT,
        `Action result:\n${JSON.stringify(result, null, 2)}`,
        SOURCE_SETTINGS
      );

      if (skill_id === "@xpell/agent-skill-telegram") {
        await refresh_skill_runtime_status(skill_id, SOURCE_SETTINGS);
      }
      set_status_message(`Action '${action.label}' executed.`, SOURCE_SETTINGS, "success");
    };

    for (const action of actions) {
      const button = document.createElement("button");
      const kind_class =
        action.kind === "danger" ? "acp-danger-btn" : action.kind === "primary" ? "acp-primary-btn" : "acp-secondary-btn";
      button.className = kind_class;
      button.type = "button";
      button.textContent = action.label;
      button.addEventListener("click", () => {
        void run_action(action).catch((error) => {
          const text = as_error_text(error);
          set_xdata(KEY_UI_SKILL_ACTION_RESULT_TEXT, `Action error:\n${text}`, SOURCE_SETTINGS);
          set_status_message(text, SOURCE_SETTINGS, "error");
        });
      });
      wrap.append(button);
    }

    host.append(wrap);
  };

  const load_skill_config = async (skill_id: string): Promise<{ skill?: ACPSkill; config: ACPSkillConfig }> => {
    const skill = find_skill_record(skill_id);
    const config = await api.skills.getConfig(skill_id);
    const declared_actions = skill?.actions ?? [];
    const fallback_actions = config.actions;
    const seen_action_ids = new Set<string>();
    const actions: ACPSkillAction[] = [];
    for (const action of [...declared_actions, ...fallback_actions]) {
      if (!action || seen_action_ids.has(action.id)) continue;
      seen_action_ids.add(action.id);
      actions.push(action);
    }
    const merged: ACPSkillConfig = {
      ...config,
      actions
    };
    const next = { skill, config: merged };
    skill_config_cache.set(skill_id, next);
    return next;
  };

  const refresh_skill_config_page = async (source: string, skill_id_override?: string): Promise<void> => {
    const skill_id = (skill_id_override ?? "").trim() || read_skill_id_from_route_args() || resolve_skill_id_from_ui();
    const validation = validate_skill_id(skill_id);
    if (!validation.ok) {
      set_status_message(validation.error, source, "error");
      return;
    }

    const loaded = await load_skill_config(skill_id);
    render_skill_settings(loaded.skill, loaded.config, source);
    render_skill_fields_dom(skill_id, loaded.config);
    render_skill_actions_dom(skill_id, loaded.config.actions);

    if (skill_id === "@xpell/agent-skill-telegram") {
      await refresh_skill_runtime_status(skill_id, source);
    } else {
      set_xdata(KEY_UI_SKILL_RUNTIME_STATUS_TEXT, "Runtime: action-driven", source);
    }
  };

  const detect_bootstrap_mode = async (): Promise<boolean> => {
    try {
      const admins = await api.users.listAdmins();
      return admins.length === 0;
    } catch (error) {
      if (error instanceof AgentApiError && error.auth_required) {
        return false;
      }
      return false;
    }
  };

  const refresh_about = async (): Promise<void> => {
    const about = await api.system.getAbout();
    render_about(about, SOURCE_API);
    render_settings_server_url(api.system.getServerUrl(), SOURCE_API);
  };

  const refresh_agent_profile = async (source: string = SOURCE_API): Promise<void> => {
    const profile = await api.agent.getProfile();
    render_agent_profile(profile, source);
  };

  const refresh_qagent_page = async (source: string): Promise<void> => {
    const auth_user = _xd.get(KEY_AUTH_USER) as { role?: unknown } | null;
    const role = auth_user && typeof auth_user.role === "string" ? auth_user.role.trim().toLowerCase() : "";
    if (role !== "admin" && role !== "owner") {
      render_qagent_page({ run: null, top_failures: [] }, source, "Forbidden");
      return;
    }

    const last_run = await api.qagent_get_last_run();
    const status_text = last_run.run
      ? `Last run ${last_run.run.run_id} | cases=${last_run.run.totals.cases_total} | pass=${format_percent(last_run.run.totals.pass_rate)} | avg=${last_run.run.totals.avg_score.toFixed(2)}`
      : "No QAgent runs yet.";
    render_qagent_page(last_run, source, status_text);
  };

  const refresh_users_page = async (source: string, q?: string): Promise<void> => {
    const auth_user = _xd.get(KEY_AUTH_USER) as { role?: unknown } | null;
    const role = auth_user && typeof auth_user.role === "string" ? auth_user.role.trim().toLowerCase() : "";
    if (role !== "admin" && role !== "owner") {
      render_users_page([], source, "Forbidden");
      return;
    }

    const result = await api.users_list({ limit: 200 });
    const query_text = q && q.trim() ? ` for "${q.trim()}"` : "";
    render_users_page(result.items, source, `Loaded ${result.items.length} user(s)${query_text}.`, q);
  };

  const refresh_conversations_page = async (source: string): Promise<void> => {
    const auth_user = _xd.get(KEY_AUTH_USER) as { role?: unknown } | null;
    const role = auth_user && typeof auth_user.role === "string" ? auth_user.role.trim().toLowerCase() : "";
    if (role !== "admin" && role !== "owner") {
      render_conversations_page([], source, "Forbidden");
      return;
    }

    const items = await api.conversations_list(200);
    render_conversations_page(items, source, `Loaded ${items.length} conversation(s).`);
  };

  const refresh_conversation_detail_page = async (source: string, thread_id_override?: string): Promise<void> => {
    const auth_user = _xd.get(KEY_AUTH_USER) as { role?: unknown } | null;
    const role = auth_user && typeof auth_user.role === "string" ? auth_user.role.trim().toLowerCase() : "";
    if (role !== "admin" && role !== "owner") {
      render_conversation_detail(null, [], source, "Forbidden");
      return;
    }

    const thread_id =
      (thread_id_override ?? "").trim() ||
      read_input_value("conversations-action-thread-id-input", false) ||
      read_thread_id_from_route_args();
    if (!thread_id) {
      render_conversation_detail(null, [], source, "Select a conversation first.");
      return;
    }

    write_input_value("conversations-action-thread-id-input", thread_id);
    const thread = await api.conversations_get_thread(thread_id);
    if (!thread) {
      render_conversation_detail(null, [], source, `Conversation not found: ${thread_id}`);
      return;
    }

    const messages = await api.conversations_list_messages({ thread_id, limit: 200 });
    render_conversation_detail(thread, messages, source, `Loaded ${messages.length} message(s).`);
  };

  const render_intent_form = (intent: ACPIntentRecord | undefined, source: string): void => {
    const selected = intent ?? null;
    set_xdata(KEY_UI_INTENT_SELECTED_ID, selected?.intent_id ?? "", source);
    set_xdata(
      KEY_UI_INTENT_SELECTED_TEXT,
      selected ? `Selected intent: ${selected.title} (${selected.intent_id})` : "Selected intent: (none)",
      source
    );
    set_xdata(
      KEY_UI_INTENT_FORM_SUMMARY_TEXT,
      selected
        ? [
            `skill: ${selected.skill_id}`,
            `enabled: ${selected.enabled ? "yes" : "no"}`,
            `priority: ${selected.priority}`,
            `roles: ${selected.roles_allowed.join(", ") || "(none)"}`,
            `channels: ${selected.channels_allowed.join(", ") || "(all)"}`
          ].join("\n")
        : "(no intent selected)",
      source
    );

    write_input_value("intents-enabled-select", selected ? (selected.enabled ? "true" : "false") : "false");
    write_input_value("intents-priority-input", selected ? String(selected.priority) : "100");
    write_input_value("intents-roles-input", selected ? selected.roles_allowed.join("\n") : "");
    write_input_value("intents-channels-input", selected ? selected.channels_allowed.join("\n") : "");
    write_input_value("intents-synonyms-input", selected ? selected.synonyms.join("\n") : "");
    write_input_value("intents-examples-input", selected ? selected.examples.join("\n") : "");
    write_input_value("intents-default-params-input", selected ? selected.default_params_json : "");
  };

  const find_intent_record = (intent_id: string): ACPIntentRecord | undefined => {
    const items = (_xd.get(KEY_INTENTS) as ACPIntentRecord[] | undefined) ?? [];
    return items.find((item) => item.intent_id === intent_id);
  };

  const refresh_intents_page = async (source: string): Promise<void> => {
    const auth_user = _xd.get(KEY_AUTH_USER) as { role?: unknown } | null;
    const role = auth_user && typeof auth_user.role === "string" ? auth_user.role.trim().toLowerCase() : "";
    if (role !== "admin" && role !== "owner") {
      render_intents_page([], source, "Forbidden");
      render_intent_form(undefined, source);
      return;
    }

    const items = await api.intents.list();
    render_intents_page(items, source, `Loaded ${items.length} intent(s).`);
    const selected_id = as_text(_xd.get(KEY_UI_INTENT_SELECTED_ID));
    const selected = (selected_id && items.find((item) => item.intent_id === selected_id)) || items[0];
    render_intent_form(selected, source);
  };

  const intents_edit = async (intent_id_override?: string): Promise<void> => {
    const intent_id = (intent_id_override ?? "").trim() || read_input_value("intents-action-intent-id-input");
    const selected = find_intent_record(intent_id);
    if (!selected) {
      set_status_message("Intent not found.", SOURCE_UI, "error");
      return;
    }
    render_intent_form(selected, SOURCE_UI);
    set_status_message(`Loaded intent '${intent_id}'.`, SOURCE_UI, "info");
  };

  const intents_save = async (): Promise<void> => {
    const intent_id = as_text(_xd.get(KEY_UI_INTENT_SELECTED_ID));
    if (!intent_id) {
      set_status_message("Select an intent first.", SOURCE_UI, "error");
      return;
    }

    const enabled = read_input_value("intents-enabled-select", false).toLowerCase() === "true";
    const priority_raw = read_input_value("intents-priority-input", false);
    const priority = Number(priority_raw);
    if (!Number.isFinite(priority)) {
      set_status_message("Priority must be a number.", SOURCE_UI, "error");
      return;
    }

    const default_params_json = read_input_value("intents-default-params-input", false).trim();
    if (default_params_json) {
      try {
        JSON.parse(default_params_json);
      } catch {
        set_status_message("Default params must be valid JSON.", SOURCE_UI, "error");
        return;
      }
    }

    await api.intents.setEnabled(intent_id, enabled);
    await api.intents.updateConfig({
      intent_id,
      priority: Math.floor(priority),
      roles_allowed: parse_string_list(read_input_value("intents-roles-input", false)),
      channels_allowed: parse_string_list(read_input_value("intents-channels-input", false)),
      synonyms: parse_string_list(read_input_value("intents-synonyms-input", false)),
      examples: parse_string_list(read_input_value("intents-examples-input", false)),
      default_params_json
    });
    await refresh_intents_page(SOURCE_UI);
    set_status_message(`Intent '${intent_id}' saved.`, SOURCE_UI, "success");
  };

  const refresh_admin_users = async (): Promise<void> => {
    const users = await api.users.listAdmins();
    render_admin_users(users, SOURCE_API);
  };

  const refresh_skills = async (): Promise<void> => {
    const skills = await api.skills.list();
    render_skills(skills, SOURCE_API);

    if (skills.length === 0) {
      set_xdata(KEY_UI_SKILL_SELECTED_ID, "", SOURCE_API);
      set_xdata(KEY_UI_SKILL_SELECTED_TEXT, "Selected skill: (none)", SOURCE_API);
      set_xdata(KEY_UI_SKILL_SETTINGS_SUMMARY_TEXT, "(no skill selected)", SOURCE_API);
      set_xdata(KEY_UI_SKILL_ACTION_RESULT_TEXT, "Action result: (none)", SOURCE_API);
      return;
    }

    const selected = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const fallback = skills.some((skill) => skill.id === selected) ? selected : skills[0].id;
    set_xdata(KEY_UI_SKILL_SELECTED_ID, fallback, SOURCE_API);
  };

  const refresh_skill_runtime_status = async (skill_id: string, source: string): Promise<void> => {
    const status = await api.skills.getRuntimeStatus(skill_id);
    render_skill_runtime_status(status, source);
  };

  const navigate_to = async (route: string, route_args?: Record<string, unknown>): Promise<void> => {
    const requested = route.trim() || ROUTE_LOGIN;
    const is_authenticated = _xd.get(KEY_AUTH_IS_AUTHENTICATED) === true;
    const bootstrap_mode = _xd.get(KEY_UI_BOOTSTRAP_MODE) === true;

    let target = requested;

    if (!is_authenticated) {
      const public_route = bootstrap_mode ? ROUTE_SETUP_ADMIN : ROUTE_LOGIN;
      if (target !== public_route) {
        target = public_route;
      }
    }

    if (is_authenticated && (target === ROUTE_LOGIN || target === ROUTE_SETUP_ADMIN)) {
      target = ROUTE_ABOUT;
    }

    if (is_conversation_detail_route(target)) {
      const selected_thread = decode_thread_value(route_args?.thread_id) || decode_thread_id_from_route(target);
      if (!selected_thread) {
        set_status_message("Select a conversation first from Conversations page.", SOURCE_NAV);
        target = ROUTE_CONVERSATIONS;
      } else {
        set_xdata(KEY_UI_CONVERSATION_ROUTE_ARGS, { thread_id: selected_thread }, SOURCE_NAV);
        target = ROUTE_CONVERSATION_DETAILS;
      }
    } else {
      set_xdata(KEY_UI_CONVERSATION_ROUTE_ARGS, {}, SOURCE_NAV);
    }

    if (is_skill_config_route(target)) {
      const selected_skill = decode_skill_value(route_args?.skill_id) || decode_skill_id_from_route(target);
      if (!selected_skill) {
        set_status_message("Select a skill first from Skills page.", SOURCE_NAV);
        target = ROUTE_SKILLS;
      } else {
        set_xdata(KEY_UI_SKILL_ROUTE_ARGS, { skill_id: selected_skill }, SOURCE_NAV);
        set_xdata(KEY_UI_SKILL_SELECTED_ID, selected_skill, SOURCE_NAV);
        target = ROUTE_SKILL_SETTINGS;
      }
    } else {
      set_xdata(KEY_UI_SKILL_ROUTE_ARGS, {}, SOURCE_NAV);
    }

    set_xdata(KEY_UI_ROUTE, target, SOURCE_NAV);
    await XVM.navigate(target, { region: REGION_MAIN });
    set_drawer_open(false, SOURCE_DRAWER);
    apply_shell_state();
    bind_action_commands();

    if (target === ROUTE_SETTINGS) {
      write_input_value("settings-server-url-input", api.system.getServerUrl());
    }

    if (target === ROUTE_AGENT) {
      await refresh_agent_profile(SOURCE_API);
    }

    if (target === ROUTE_QAGENT) {
      await refresh_qagent_page(SOURCE_API);
    }

    if (target === ROUTE_CONVERSATIONS) {
      await refresh_conversations_page(SOURCE_API);
    }

    if (target === ROUTE_CONVERSATION_DETAILS) {
      await refresh_conversation_detail_page(SOURCE_API, read_thread_id_from_route_args());
    }

    if (target === ROUTE_INTENTS) {
      await refresh_intents_page(SOURCE_API);
    }

    if (target === ROUTE_USERS) {
      await refresh_users_page(SOURCE_API, read_input_value("users-search-input", false));
    }

    if (is_skill_config_route(target)) {
      await refresh_skill_config_page(SOURCE_API, decode_skill_id_from_route(target));
    }
  };

  const resolve_skill_id_from_ui = (): string => {
    const from_input = read_input_value("skills-skill-id-input");
    if (from_input) return from_input;

    const from_selected = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    if (from_selected) return from_selected;

    const skills = (_xd.get(KEY_SKILLS) as ACPSkill[] | undefined) ?? [];
    if (skills.length > 0) return skills[0].id;
    return "";
  };

  const login = async (): Promise<void> => {
    const identifier = read_input_value("acp-login-username");
    const password = read_input_value("acp-login-password", false);

    const validation = validate_login_form(identifier, password);
    if (!validation.ok) {
      set_login_message(validation.error, SOURCE_AUTH);
      return;
    }

    const session = await api.auth.login(identifier, password);
    save_session(session as ACPAuthSession);
    set_auth_state(true, session.user, SOURCE_AUTH);
    set_bootstrap_mode(false, SOURCE_AUTH);

    set_login_message("", SOURCE_AUTH);
    set_status_message(`Logged in as ${session.user.name}.`, SOURCE_AUTH);

    await refresh_about();
    await refresh_admin_users();
    await refresh_skills();
    await navigate_to(ROUTE_ABOUT);
  };

  const setup_admin_create = async (): Promise<void> => {
    const name = read_input_value("setup-admin-name-input");
    const username = read_input_value("setup-admin-username-input");
    const password = read_input_value("setup-admin-password-input", false);

    const validation = validate_admin_create_input(name, username, password);
    if (!validation.ok) {
      set_login_message(validation.error, SOURCE_AUTH);
      return;
    }

    await api.users.createAdmin({ name, username, password });

    const session = await api.auth.login(username, password);
    save_session(session as ACPAuthSession);
    set_auth_state(true, session.user, SOURCE_AUTH);
    set_bootstrap_mode(false, SOURCE_AUTH);

    write_input_value("setup-admin-name-input", "");
    write_input_value("setup-admin-username-input", "");
    write_input_value("setup-admin-password-input", "");

    set_login_message("", SOURCE_AUTH);
    set_status_message(`First admin created: ${session.user.name}.`, SOURCE_AUTH, "success");

    await refresh_about();
    await refresh_admin_users();
    await refresh_skills();
    await navigate_to(ROUTE_ABOUT);
  };

  const logout = async (): Promise<void> => {
    const session = load_session();
    await api.auth.logout(session?.token);
    clear_session();

    set_auth_state(false, null, SOURCE_AUTH);
    set_login_message("Session ended.", SOURCE_AUTH);
    set_status_message("Logged out.", SOURCE_AUTH);

    await navigate_to(ROUTE_LOGIN);
  };

  const admin_create = async (): Promise<void> => {
    const name = read_input_value("admin-create-name-input");
    const username = read_input_value("admin-create-username-input");
    const password = read_input_value("admin-create-password-input", false);

    const validation = validate_admin_create_input(name, username, password);
    if (!validation.ok) {
      set_status_message(validation.error, SOURCE_UI, "error");
      return;
    }

    await api.users.createAdmin({ name, username, password });
    await refresh_admin_users();

    write_input_value("admin-create-name-input", "");
    write_input_value("admin-create-username-input", "");
    write_input_value("admin-create-password-input", "");

    set_status_message("Admin created.", SOURCE_UI);
  };

  const admin_update = async (): Promise<void> => {
    const id = read_input_value("admin-edit-id-input");
    const name = read_input_value("admin-edit-name-input");
    const username = read_input_value("admin-edit-username-input");
    const password = read_input_value("admin-edit-password-input", false);

    const validation = validate_admin_update_input(id, name, username, password);
    if (!validation.ok) {
      set_status_message(validation.error, SOURCE_UI, "error");
      return;
    }

    await api.users.updateAdmin({
      id,
      ...(name ? { name } : {}),
      ...(username ? { username } : {}),
      ...(password ? { password } : {})
    });

    await refresh_admin_users();
    set_status_message(`Admin '${id}' updated.`, SOURCE_UI);
  };

  const admin_delete = async (): Promise<void> => {
    const id = read_input_value("admin-edit-id-input");
    const validation = validate_admin_delete_input(id);
    if (!validation.ok) {
      set_status_message(validation.error, SOURCE_UI, "error");
      return;
    }

    await api.users.deleteAdmin(id);
    await refresh_admin_users();

    write_input_value("admin-edit-id-input", "");
    set_status_message(`Admin '${id}' deleted.`, SOURCE_UI);
  };

  const skills_enable = async (skill_id_override?: string): Promise<void> => {
    const skill_id = (skill_id_override ?? "").trim() || resolve_skill_id_from_ui();
    _xlog.log(`[acp-ui] skills enable click skill_id=${skill_id || "(empty)"}`);
    const validation = validate_skill_id(skill_id);
    if (!validation.ok) {
      set_status_message(validation.error, SOURCE_UI, "error");
      return;
    }

    await api.skills.enable(skill_id);
    await refresh_skills();
    set_status_message(`Skill '${skill_id}' enabled.`, SOURCE_UI);
  };

  const skills_disable = async (skill_id_override?: string): Promise<void> => {
    const skill_id = (skill_id_override ?? "").trim() || resolve_skill_id_from_ui();
    _xlog.log(`[acp-ui] skills disable click skill_id=${skill_id || "(empty)"}`);
    const validation = validate_skill_id(skill_id);
    if (!validation.ok) {
      set_status_message(validation.error, SOURCE_UI, "error");
      return;
    }

    await api.skills.disable(skill_id);
    await refresh_skills();
    set_status_message(`Skill '${skill_id}' disabled.`, SOURCE_UI);
  };

  const skills_open_settings = async (skill_id_override?: string): Promise<void> => {
    const skill_id = (skill_id_override ?? "").trim() || resolve_skill_id_from_ui();
    const validation = validate_skill_id(skill_id);
    if (!validation.ok) {
      set_status_message(validation.error, SOURCE_UI, "error");
      return;
    }

    set_status_message("", SOURCE_UI);
    await navigate_to(ROUTE_SKILL_SETTINGS, { skill_id });
  };

  const skill_mode_polling = (): void => {
    set_xdata(KEY_UI_SKILL_MODE, "polling", SOURCE_SETTINGS);
    write_input_value("skill-settings-mode-input", "polling");
    set_status_message("Telegram mode set to polling.", SOURCE_SETTINGS);
  };

  const skill_mode_webhook = (): void => {
    set_xdata(KEY_UI_SKILL_MODE, "webhook", SOURCE_SETTINGS);
    write_input_value("skill-settings-mode-input", "webhook");
    set_status_message("Webhook mode is marked coming in v1.", SOURCE_SETTINGS);
  };

  const skill_settings_save = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    const cached = skill_config_cache.get(skill_id);
    if (!cached) {
      set_status_message("Skill config is not loaded.", SOURCE_SETTINGS, "error");
      return;
    }

    const patch: Record<string, unknown> = {};

    for (const field of cached.config.schema?.fields ?? []) {
      const control = document.getElementById(field_dom_id(skill_id, field)) as HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement | null;
      if (!control) continue;

      const original = get_by_path(cached.config.settings, field.key);
      let next_value: unknown = original;
      let should_compare = true;

      if (field.type === "boolean") {
        next_value = (control as HTMLInputElement).checked;
      } else {
        const raw = control.value;

        if ((field.secret === true || cached.config.masked[field.key] === true) && raw.trim().length === 0) {
          should_compare = false;
        } else if (field.type === "number") {
          if (raw.trim().length === 0) {
            should_compare = false;
          } else {
            const parsed = Number(raw);
            if (!Number.isFinite(parsed)) {
              set_status_message(`Invalid number for ${field.label}.`, SOURCE_SETTINGS, "error");
              return;
            }
            next_value = parsed;
          }
        } else if (field.type === "string_list") {
          next_value = parse_string_list(raw);
        } else {
          next_value = raw;
        }
      }

      if (!should_compare) continue;
      if (!values_equal(next_value, original)) {
        set_by_path(patch, field.key, next_value);
      }
    }

    if (Object.keys(patch).length === 0) {
      set_status_message("No setting changes to save.", SOURCE_SETTINGS, "warn");
      return;
    }

    await api.skills.updateSettings(skill_id, patch);
    await refresh_skill_config_page(SOURCE_SETTINGS, skill_id);
    set_status_message("Skill settings saved.", SOURCE_SETTINGS, "success");
  };

  const skill_status_refresh = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }
    await refresh_skill_runtime_status(skill_id, SOURCE_SETTINGS);
    set_status_message("Polling status refreshed.", SOURCE_SETTINGS);
  };

  const skill_service_start = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    await api.skills.enable(skill_id);
    await refresh_skills();
    await refresh_skill_runtime_status(skill_id, SOURCE_SETTINGS);
    set_status_message(`Skill service '${skill_id}' started (enabled).`, SOURCE_SETTINGS);
  };

  const skill_service_stop = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    await api.skills.disable(skill_id);
    await refresh_skills();
    await refresh_skill_runtime_status(skill_id, SOURCE_SETTINGS);
    set_status_message(`Skill service '${skill_id}' stopped (disabled).`, SOURCE_SETTINGS);
  };

  const skill_polling_start = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    const status = await api.skills.startPolling(skill_id);
    render_skill_runtime_status(status, SOURCE_SETTINGS);
    set_status_message("Polling start requested.", SOURCE_SETTINGS);
  };

  const skill_polling_stop = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    const status = await api.skills.stopPolling(skill_id);
    render_skill_runtime_status(status, SOURCE_SETTINGS);
    set_status_message("Polling stop requested.", SOURCE_SETTINGS);
  };

  const skill_verify_token = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    const input_token = read_input_value("skill-settings-bot-token-input", false).trim();
    const token_to_verify =
      input_token.length > 0 && input_token !== MASK_SENTINEL ? input_token : undefined;
    const result = await api.skills.verifyToken(skill_id, token_to_verify);

    if (result.valid) {
      const username = as_text(result.bot?.username);
      const bot_id = as_text(result.bot?.id);
      const bot_text =
        username.length > 0
          ? `@${username}${bot_id ? ` (id:${bot_id})` : ""}`
          : bot_id
            ? `id:${bot_id}`
            : "bot";
      const source = result.source === "input" ? "input token" : "configured token";
      set_status_message(`Token valid (${source}): ${bot_text}.`, SOURCE_SETTINGS);
      return;
    }

    const source = result.source === "input" ? "input token" : "configured token";
    const reason = as_text(result.error) || "invalid token";
    set_status_message(`Token invalid (${source}): ${reason}.`, SOURCE_SETTINGS, "error");
  };

  const skill_import_env = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    if (skill_id !== AZURE_SKILL_ID) {
      set_status_message("Env import is available only for Azure skill.", SOURCE_SETTINGS, "warn");
      return;
    }

    const result = await api.skills.importEnv(skill_id);
    await refresh_skill_config_page(SOURCE_SETTINGS, skill_id);

    const imported_count =
      typeof result.imported_count === "number" && Number.isFinite(result.imported_count)
        ? Math.max(0, Math.floor(result.imported_count))
        : 0;

    if (imported_count > 0) {
      const keys_text = result.imported_keys.length > 0 ? ` (${result.imported_keys.join(", ")})` : "";
      set_status_message(`Imported ${imported_count} env key(s)${keys_text}.`, SOURCE_SETTINGS, "success");
      return;
    }

    const detail = as_text(result.detail);
    set_status_message(
      detail ? `No env vars imported: ${detail}.` : "No env vars imported.",
      SOURCE_SETTINGS,
      "warn"
    );
  };

  const skill_verify_azure = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    if (skill_id !== AZURE_SKILL_ID) {
      set_status_message("Azure key verification is available only for Azure skill.", SOURCE_SETTINGS, "warn");
      return;
    }

    const result = await api.skills.testAzureConnection(skill_id);
    const openai_text = `openai=${result.openai.ok ? "ok" : "fail"}${result.openai.detail ? ` (${result.openai.detail})` : ""}`;
    const speech_text = `speech=${result.speech.ok ? "ok" : "fail"}${result.speech.detail ? ` (${result.speech.detail})` : ""}`;
    const variant: ToastVariant = result.openai.ok || result.speech.ok ? "success" : "warn";
    set_status_message(`Azure verify: ${openai_text}; ${speech_text}`, SOURCE_SETTINGS, variant);
  };

  const agent_save = async (): Promise<void> => {
    const name = read_input_value("agent-name-input");
    const role = read_input_value("agent-role-input");
    const system_prompt = read_input_value("agent-system-prompt-input", false).trim();
    const language_policy = normalize_agent_language_policy(read_input_value("agent-language-policy-select", false));

    const validation = validate_agent_language_policy(language_policy);
    if (!validation.ok) {
      set_status_message(validation.error, SOURCE_SETTINGS, "error");
      return;
    }

    const profile = await api.agent.setProfile({
      name,
      role,
      system_prompt,
      language_policy
    });
    render_agent_profile(profile, SOURCE_SETTINGS);
    set_status_message("Agent profile saved.", SOURCE_SETTINGS, "success");
  };

  const agent_reset_defaults = async (): Promise<void> => {
    write_input_value("agent-name-input", DEFAULT_AGENT_NAME);
    write_input_value("agent-role-input", DEFAULT_AGENT_ROLE);
    write_input_value("agent-system-prompt-input", DEFAULT_AGENT_SYSTEM_PROMPT);
    write_input_value("agent-language-policy-select", DEFAULT_AGENT_LANGUAGE_POLICY);
    await agent_save();
  };

  const qagent_run_quick = async (): Promise<void> => {
    const auth_user = _xd.get(KEY_AUTH_USER) as { role?: unknown } | null;
    const role = auth_user && typeof auth_user.role === "string" ? auth_user.role.trim().toLowerCase() : "";
    if (role !== "admin" && role !== "owner") {
      render_qagent_page({ run: null, top_failures: [] }, SOURCE_UI, "Forbidden");
      return;
    }

    set_status_message("QAgent quick test started...", SOURCE_UI);
    const last_run = await api.qagent_run_quick(8);
    const run_id = last_run.run?.run_id ?? "(unknown)";
    render_qagent_page(last_run, SOURCE_UI, `QAgent quick test completed (${run_id}).`);
    set_status_message(`QAgent quick test completed (${run_id}).`, SOURCE_UI, "success");
  };

  const qagent_refresh = async (): Promise<void> => {
    await refresh_qagent_page(SOURCE_UI);
    set_status_message("QAgent refreshed.", SOURCE_UI);
  };

  const users_search = async (): Promise<void> => {
    const auth_user = _xd.get(KEY_AUTH_USER) as { role?: unknown } | null;
    const role = auth_user && typeof auth_user.role === "string" ? auth_user.role.trim().toLowerCase() : "";
    if (role !== "admin" && role !== "owner") {
      render_users_page([], SOURCE_UI, "Forbidden");
      return;
    }

    const q = read_input_value("users-search-input", false);
    const all_users = ((_xd.get(KEY_USERS) as ACPUserSummary[] | undefined) ?? []).length;
    const query_text = q && q.trim() ? ` for "${q.trim()}"` : "";
    apply_users_filter(SOURCE_UI, `Showing filtered users (${all_users} loaded)${query_text}.`, q);
  };

  const users_refresh = async (): Promise<void> => {
    const q = read_input_value("users-search-input", false);
    await refresh_users_page(SOURCE_UI, q);
  };

  const users_set_role = async (next_role: "admin" | "customer", user_id_override?: string): Promise<void> => {
    const user_id = (user_id_override ?? "").trim() || read_input_value("users-action-user-id-input");
    if (!user_id) {
      set_status_message("User id is required.", SOURCE_UI, "error");
      return;
    }

    await api.exec("users", "set_role", {
      _user_id: user_id,
      _role: next_role
    });
    await refresh_users_page(SOURCE_UI, read_input_value("users-search-input", false));
    set_status_message(
      next_role === "admin" ? `User '${user_id}' promoted to admin.` : `User '${user_id}' changed to customer.`,
      SOURCE_UI,
      "success"
    );
  };

  const users_promote_admin = async (user_id_override?: string): Promise<void> => {
    await users_set_role("admin", user_id_override);
  };

  const users_demote_customer = async (user_id_override?: string): Promise<void> => {
    await users_set_role("customer", user_id_override);
  };

  const conversations_refresh = async (): Promise<void> => {
    await refresh_conversations_page(SOURCE_UI);
  };

  const conversations_open = async (thread_id_override?: string): Promise<void> => {
    const thread_id =
      (thread_id_override ?? "").trim() || read_input_value("conversations-action-thread-id-input", false) || read_thread_id_from_route_args();
    if (!thread_id) {
      set_status_message("Select a conversation first.", SOURCE_UI, "error");
      return;
    }
    await navigate_to(`${ROUTE_CONVERSATIONS}/${encodeURIComponent(thread_id)}`, { thread_id });
  };

  const settings_theme_toggle = (): void => {
    const current = as_text(_xd.get(KEY_UI_THEME)) === "light" ? "light" : "dark";
    const next = current === "dark" ? "light" : "dark";
    render_theme(next, SOURCE_SETTINGS);
    set_xdata(KEY_UI_SETTINGS_NOTICE, `Theme changed to ${next}.`, SOURCE_SETTINGS);
  };

  const settings_server_url_save = async (): Promise<void> => {
    const raw_input = read_input_value("settings-server-url-input", false);
    const next_url = api.system.setServerUrl(raw_input);
    render_settings_server_url(next_url, SOURCE_SETTINGS);
    write_input_value("settings-server-url-input", next_url);
    set_xdata(KEY_UI_SETTINGS_NOTICE, "Server URL saved.", SOURCE_SETTINGS);
    await refresh_about();
  };

  const settings_reset_db = async (): Promise<void> => {
    const auth_user = _xd.get(KEY_AUTH_USER) as { role?: unknown } | null;
    const role = auth_user && typeof auth_user.role === "string" ? auth_user.role.trim().toLowerCase() : "";
    if (role !== "admin" && role !== "owner") {
      set_xdata(KEY_UI_SETTINGS_NOTICE, "Forbidden", SOURCE_SETTINGS);
      return;
    }

    const confirmed =
      typeof window === "undefined"
        ? true
        : window.confirm("Reset DB? This deletes all conversations, users, and sessions and signs you out.");
    if (!confirmed) return;

    const result = await api.system.resetDb();
    const summary = `Reset complete. Threads:${result.threads_deleted} Messages:${result.messages_deleted} Users:${result.users_deleted} Sessions:${result.sessions_deleted}`;
    set_xdata(KEY_UI_SETTINGS_NOTICE, summary, SOURCE_SETTINGS);

    clear_session();
    set_auth_state(false, null, SOURCE_AUTH);
    set_bootstrap_mode(true, SOURCE_SETTINGS);
    set_drawer_open(false, SOURCE_DRAWER);
    set_login_message("Database reset. Create the first admin to continue.", SOURCE_SETTINGS);
    set_status_message(summary, SOURCE_SETTINGS, "success");
    await navigate_to(ROUTE_SETUP_ADMIN);
  };

  const dispatch_action = async (params: ActionParams): Promise<void> => {
    const action = as_text(params._action);

    switch (action) {
      case ACTION_NAVIGATE:
        await navigate_to(as_text(params._route));
        return;

      case ACTION_LOGIN:
        await login();
        return;

      case ACTION_SETUP_ADMIN_CREATE:
        await setup_admin_create();
        return;

      case ACTION_LOGOUT:
        await logout();
        return;

      case ACTION_DRAWER_TOGGLE: {
        const is_open = _xd.get(KEY_UI_DRAWER_OPEN) === true;
        set_drawer_open(!is_open, SOURCE_DRAWER);
        return;
      }

      case ACTION_DRAWER_CLOSE:
        set_drawer_open(false, SOURCE_DRAWER);
        return;

      case ACTION_REFRESH_ABOUT:
        await refresh_about();
        set_status_message("About refreshed.", SOURCE_API);
        return;

      case ACTION_AGENT_SAVE:
        await agent_save();
        return;

      case ACTION_AGENT_RESET_DEFAULTS:
        await agent_reset_defaults();
        return;

      case ACTION_QAGENT_RUN_QUICK:
        await qagent_run_quick();
        return;

      case ACTION_QAGENT_REFRESH:
        await qagent_refresh();
        return;

      case ACTION_CONVERSATIONS_REFRESH:
        await conversations_refresh();
        return;

      case ACTION_CONVERSATIONS_OPEN:
        await conversations_open(as_text(params.thread_id));
        return;

      case ACTION_INTENTS_REFRESH:
        await refresh_intents_page(SOURCE_UI);
        return;

      case ACTION_INTENTS_EDIT:
        await intents_edit(as_text(params.intent_id));
        return;

      case ACTION_INTENTS_SAVE:
        await intents_save();
        return;

      case ACTION_USERS_SEARCH:
        await users_search();
        return;

      case ACTION_USERS_REFRESH:
        await users_refresh();
        return;

      case ACTION_USERS_PROMOTE_ADMIN:
        await users_promote_admin(as_text(params.user_id));
        return;

      case ACTION_USERS_DEMOTE_CUSTOMER:
        await users_demote_customer(as_text(params.user_id));
        return;

      case ACTION_ADMIN_CREATE:
        await admin_create();
        return;

      case ACTION_ADMIN_UPDATE:
        await admin_update();
        return;

      case ACTION_ADMIN_DELETE:
        await admin_delete();
        return;

      case ACTION_SKILLS_ENABLE:
        await skills_enable(as_text(params.skill_id));
        return;

      case ACTION_SKILLS_DISABLE:
        await skills_disable(as_text(params.skill_id));
        return;

      case ACTION_SKILLS_OPEN_SETTINGS:
        await skills_open_settings(as_text(params.skill_id));
        return;

      case ACTION_SKILL_MODE_POLLING:
        skill_mode_polling();
        return;

      case ACTION_SKILL_MODE_WEBHOOK:
        skill_mode_webhook();
        return;

      case ACTION_SKILL_SETTINGS_SAVE:
        await skill_settings_save();
        return;

      case ACTION_SKILL_STATUS_REFRESH:
        await skill_status_refresh();
        return;

      case ACTION_SKILL_SERVICE_START:
        await skill_service_start();
        return;

      case ACTION_SKILL_SERVICE_STOP:
        await skill_service_stop();
        return;

      case ACTION_SKILL_POLLING_START:
        await skill_polling_start();
        return;

      case ACTION_SKILL_POLLING_STOP:
        await skill_polling_stop();
        return;

      case ACTION_SKILL_VERIFY_TOKEN:
        await skill_verify_token();
        return;

      case ACTION_SKILL_IMPORT_ENV:
        await skill_import_env();
        return;

      case ACTION_SKILL_VERIFY_AZURE:
        await skill_verify_azure();
        return;

      case ACTION_SETTINGS_THEME_TOGGLE:
        settings_theme_toggle();
        return;

      case ACTION_SETTINGS_SERVER_URL_SAVE:
        await settings_server_url_save();
        return;

      case ACTION_SETTINGS_RESET_DB:
        await settings_reset_db();
        return;

      default:
        set_status_message(`Unknown action '${action}'.`, SOURCE_UI, "warn");
    }
  };

  const force_login = async (reason: string): Promise<void> => {
    clear_session();
    set_auth_state(false, null, SOURCE_AUTH);
    set_drawer_open(false, SOURCE_DRAWER);
    set_login_message(reason, SOURCE_AUTH);
    set_status_message(reason, SOURCE_AUTH, "warn");
    await navigate_to(ROUTE_LOGIN);
  };

  const bind_action_commands = (): void => {
    for (const object_id of UI_COMMAND_OBJECT_IDS) {
      const object_ref = XUI.getObject(object_id) as
        | {
            addNanoCommand: (name: string, handler: (cmd: unknown) => Promise<void>) => void;
            __acp_action_bound?: boolean;
          }
        | undefined;

      if (!object_ref) continue;
      if (object_ref.__acp_action_bound === true) continue;

      object_ref.addNanoCommand(UI_ACTION_COMMAND, async (xcmd: unknown) => {
        const params =
          xcmd && typeof xcmd === "object" && (xcmd as { _params?: unknown })._params
            ? ((xcmd as { _params: ActionParams })._params as ActionParams)
            : {};
        const action = as_text(params._action);

        try {
          await dispatch_action(params);
        } catch (error) {
          if (error instanceof AgentApiError && error.code === "E_USERS_AUTH_FAILED" && action === ACTION_LOGIN) {
            const msg = error.message || "Invalid username or password.";
            set_login_message(msg, SOURCE_AUTH);
            set_status_message(msg, SOURCE_AUTH, "error");
            return;
          }

          if (error instanceof AgentApiError && error.auth_required && action !== ACTION_LOGIN) {
            await force_login("Session expired. Please login again.");
            return;
          }

          const text = as_error_text(error);
          set_status_message(text, SOURCE_UI, "error");
          set_login_message(text, SOURCE_UI);
        }
      });

      object_ref.__acp_action_bound = true;
    }
  };

  const bind_events_once = (): void => {
    if (events_bound) return;
    events_bound = true;

    _xem.on("xvm:view-rendered", () => {
      bind_action_commands();
      apply_shell_state();
    });
  };

  const seed_state = (): void => {
    set_auth_state(false, null, SOURCE_BOOT);
    set_bootstrap_mode(false, SOURCE_BOOT);
    set_xdata(KEY_UI_DRAWER_OPEN, false, SOURCE_BOOT);
    set_xdata(KEY_UI_ROUTE, ROUTE_LOGIN, SOURCE_BOOT);

    render_theme(read_stored_theme(), SOURCE_BOOT);
    render_settings_server_url(api.system.getServerUrl(), SOURCE_BOOT);
    set_xdata(KEY_UI_SETTINGS_NOTICE, "", SOURCE_BOOT);

    render_about(
      {
        agent_version: "unknown",
        xpell_version: "unknown",
        connected: false,
        server_url: "n/a"
      },
      SOURCE_BOOT
    );

    render_agent_profile(
      {
        agent_id: "unknown",
        env: "default",
        agent_runtime_version: "unknown",
        xpell_version: "unknown",
        connected: false,
        identity: {
          name: DEFAULT_AGENT_NAME,
          role: "",
          system_prompt: "",
          language_policy: DEFAULT_AGENT_LANGUAGE_POLICY
        }
      },
      SOURCE_BOOT
    );

    render_conversations_page([], SOURCE_BOOT, "Conversations not loaded.");
    render_conversation_detail(null, [], SOURCE_BOOT, "Conversation not loaded.");
    render_qagent_page({ run: null, top_failures: [] }, SOURCE_BOOT, "QAgent not loaded.");
    render_intents_page([], SOURCE_BOOT, "Intents not loaded.");
    render_users_page([], SOURCE_BOOT, "Users not loaded.");
    render_intent_form(undefined, SOURCE_BOOT);

    render_admin_users([], SOURCE_BOOT);
    render_skills([], SOURCE_BOOT);

    skill_config_cache.clear();
    set_xdata(KEY_SKILL_SETTINGS, {}, SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_SELECTED_ID, "", SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_SELECTED_TEXT, "Selected skill: (none)", SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_BOT_TOKEN, "", SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_ADMIN_CHAT_IDS, "", SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_MODE, "polling", SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_SETTINGS_SUMMARY_TEXT, "(no skill selected)", SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_RUNTIME_STATUS_TEXT, "Runtime: unknown", SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_ACTION_RESULT_TEXT, "Action result: (none)", SOURCE_BOOT);

    set_login_message("", SOURCE_BOOT);
    set_status_message("", SOURCE_BOOT);
  };

  const restore_session = async (): Promise<boolean> => {
    const session = load_session();
    if (!session || !session.token) return false;

    try {
      const current = await api.auth.session(session.token);
      if (!current.is_authenticated || !current.user) {
        clear_session();
        set_auth_state(false, null, SOURCE_AUTH);
        return false;
      }

      const next_session: ACPAuthSession = { token: session.token, user: current.user };
      save_session(next_session);
      set_auth_state(true, current.user, SOURCE_AUTH);
      return true;
    } catch {
      clear_session();
      set_auth_state(false, null, SOURCE_AUTH);
      return false;
    }
  };

  const bootstrap = async (): Promise<void> => {
    seed_state();
    await restore_session();

    bind_action_commands();
    bind_events_once();

    await refresh_about();

    if (_xd.get(KEY_AUTH_IS_AUTHENTICATED) === true) {
      try {
        await refresh_admin_users();
        await refresh_skills();
        await refresh_agent_profile(SOURCE_API);
        await navigate_to(ROUTE_ABOUT);
      } catch (error) {
        if (error instanceof AgentApiError && error.auth_required) {
          await force_login("Session expired. Please login again.");
        } else {
          throw error;
        }
      }
    } else {
      const bootstrap_mode = await detect_bootstrap_mode();
      set_bootstrap_mode(bootstrap_mode, SOURCE_BOOT);
      if (bootstrap_mode) {
        await navigate_to(ROUTE_SETUP_ADMIN);
        set_login_message("No admins found. Create the first admin to continue.", SOURCE_BOOT);
      } else {
        await navigate_to(ROUTE_LOGIN);
        set_login_message("Use agent admin credentials to login.", SOURCE_BOOT);
      }
    }

    apply_shell_state();
  };

  return {
    register: bind_action_commands,
    bootstrap
  };
}

export default create_ui_commands;
