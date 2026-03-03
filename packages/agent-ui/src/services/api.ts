import type { ACPSkillMode } from "../state/xd_keys.js";
import { clear_token, load_token, save_token } from "./auth.js";

export type ACPAuthUser = {
  user_id: string;
  role: "admin" | "owner";
  name: string;
};

export type ACPAdminUser = {
  id: string;
  name: string;
  username: string;
  role: "admin" | "owner";
};

export type ACPSkillStatus = "loaded" | "error" | "disabled";

export type ACPSkillFieldType = "string" | "number" | "boolean" | "select" | "string_list";

export type ACPSkillFieldOption = {
  label: string;
  value: unknown;
};

export type ACPSkillField = {
  key: string;
  label: string;
  type: ACPSkillFieldType;
  help?: string;
  secret?: boolean;
  options?: ACPSkillFieldOption[];
  placeholder?: string;
};

export type ACPSkillSchema = {
  title?: string;
  fields: ACPSkillField[];
};

export type ACPSkillAction = {
  id: string;
  label: string;
  kind?: "primary" | "danger" | "secondary";
  op: {
    module: string;
    op: string;
  };
  params_schema?: {
    fields: ACPSkillField[];
  };
  confirm?: {
    title: string;
    body: string;
  };
};

export type ACPSkill = {
  id: string;
  version: string;
  enabled: boolean;
  status: ACPSkillStatus;
  name?: string;
  description?: string;
  settings_meta?: {
    schema?: ACPSkillSchema;
    sensitive?: string[];
  };
  actions?: ACPSkillAction[];
  error?: string;
};

export type ACPSkillConfig = {
  skill_id: string;
  settings: Record<string, unknown>;
  masked: Record<string, boolean>;
  schema?: ACPSkillSchema;
  actions: ACPSkillAction[];
};

export type ACPIntentRole = "admin" | "owner" | "customer";

export type ACPIntentRecord = {
  intent_id: string;
  title: string;
  description?: string;
  skill_id: string;
  enabled: boolean;
  priority: number;
  roles_allowed: ACPIntentRole[];
  channels_allowed: string[];
  synonyms: string[];
  examples: string[];
  default_params_json: string;
};

export type ACPIntentUpdateInput = {
  intent_id: string;
  priority?: number;
  roles_allowed?: string[];
  channels_allowed?: string[];
  synonyms?: string[];
  examples?: string[];
  default_params_json?: string;
};

export type ACPSkillRuntimeStatus = {
  available: boolean;
  running: boolean;
  mode?: string;
  error?: string;
};

export type ACPTelegramTokenVerify = {
  valid: boolean;
  source: "input" | "configured";
  bot?: {
    id: string;
    username?: string;
    first_name?: string;
  };
  error?: string;
};

export type ACPSkillImportEnvResult = {
  imported_count: number;
  imported_keys: string[];
  detail?: string;
};

export type ACPSkillProbeResult = {
  ok: boolean;
  detail?: string;
};

export type ACPAzureConnectionTestResult = {
  openai: ACPSkillProbeResult;
  speech: ACPSkillProbeResult;
  _ts: number;
};

export type ACPAbout = {
  agent_version: string;
  xpell_version: string;
  connected: boolean;
  server_url?: string;
};

export type ACPAgentLanguagePolicy = "auto" | "spanish" | "english";

export type ACPAgentIdentity = {
  name: string;
  role: string;
  system_prompt: string;
  language_policy: ACPAgentLanguagePolicy;
};

export type ACPAgentProfile = {
  agent_id: string;
  env: string;
  agent_runtime_version: string;
  xpell_version: string;
  connected: boolean;
  identity: ACPAgentIdentity;
};

export type ACPAgentProfilePatch = Partial<ACPAgentIdentity>;

export type ACPUserSummary = {
  user_id: string;
  display_id?: string;
  display_name: string;
  role: "admin" | "owner" | "customer";
  channels: string[];
  created_at: number;
  updated_at: number;
};

export type ACPUsersListInput = {
  q?: string;
  limit?: number;
  cursor?: string;
};

export type ACPUsersListResult = {
  items: ACPUserSummary[];
  next_cursor?: string;
};

export type ACPConversationSummary = {
  thread_id: string;
  channel: string;
  channel_thread_id: string;
  user_id: string;
  status: string;
  created_at: number;
  updated_at: number;
  tags: string[];
};

export type ACPConversationMessage = {
  message_id: string;
  thread_id: string;
  direction: "in" | "out";
  sender: string;
  text: string;
  ts: number;
  channel_message_id?: string;
};

export type ACPQAgentTotals = {
  cases_total: number;
  cases_passed: number;
  avg_score: number;
  pass_rate: number;
};

export type ACPQAgentRun = {
  run_id: string;
  status: string;
  created_at: number;
  updated_at: number;
  agent_name: string;
  agent_role: string;
  summary: string;
  kb_files: string[];
  totals: ACPQAgentTotals;
};

export type ACPQAgentCase = {
  case_id: string;
  case_idx: number;
  audience: "admin" | "customer";
  intent_id: string;
  question: string;
  expected_facts: string[];
  answer: string;
  score: number;
  judge_notes: string;
};

export type ACPQAgentLastRun = {
  run: ACPQAgentRun | null;
  top_failures: ACPQAgentCase[];
};

export type ACPResetDbResult = {
  ok: boolean;
  threads_deleted: number;
  messages_deleted: number;
  users_deleted: number;
  sessions_deleted: number;
};

export type ACPTelegramSkillSettings = {
  bot_token: string;
  admin_chat_ids: string[];
  mode: ACPSkillMode;
  auto_start?: boolean;
  polling?: {
    timeout_sec?: number;
  };
  webhook?: {
    url?: string;
    secret_token?: string;
  };
};

export type ACPLoginResponse = {
  token: string;
  user: ACPAuthUser;
};

export type ACPCreateAdminInput = {
  name: string;
  username: string;
  password: string;
};

export type ACPUpdateAdminInput = {
  id: string;
  name?: string;
  username?: string;
  password?: string;
};

export type AuthSessionResponse = {
  is_authenticated: boolean;
  user: ACPAuthUser | null;
};

export interface AgentApi {
  exec(module_name: string, op: string, params?: Record<string, unknown>): Promise<unknown>;
  auth: {
    login(identifier: string, password: string): Promise<ACPLoginResponse>;
    logout(token?: string): Promise<void>;
    session(token?: string): Promise<AuthSessionResponse>;
  };
  users: {
    listAdmins(): Promise<ACPAdminUser[]>;
    createAdmin(input: ACPCreateAdminInput): Promise<ACPAdminUser>;
    updateAdmin(input: ACPUpdateAdminInput): Promise<ACPAdminUser>;
    deleteAdmin(id: string): Promise<void>;
  };
  skills: {
    list(): Promise<ACPSkill[]>;
    getConfig(skill_id: string): Promise<ACPSkillConfig>;
    enable(skill_id: string): Promise<ACPSkill>;
    disable(skill_id: string): Promise<ACPSkill>;
    getSettings(skill_id: string): Promise<Record<string, unknown>>;
    updateSettings(skill_id: string, settings: Record<string, unknown>): Promise<Record<string, unknown>>;
    getRuntimeStatus(skill_id: string): Promise<ACPSkillRuntimeStatus>;
    startPolling(skill_id: string): Promise<ACPSkillRuntimeStatus>;
    stopPolling(skill_id: string): Promise<ACPSkillRuntimeStatus>;
    verifyToken(skill_id: string, bot_token?: string): Promise<ACPTelegramTokenVerify>;
    importEnv(skill_id: string): Promise<ACPSkillImportEnvResult>;
    testAzureConnection(skill_id: string): Promise<ACPAzureConnectionTestResult>;
  };
  intents: {
    list(): Promise<ACPIntentRecord[]>;
    setEnabled(intent_id: string, enabled: boolean): Promise<void>;
    updateConfig(input: ACPIntentUpdateInput): Promise<void>;
  };
  system: {
    getAbout(): Promise<ACPAbout>;
    getServerUrl(): string;
    setServerUrl(next_url: string): string;
    resetDb(): Promise<ACPResetDbResult>;
  };
  agent: {
    getProfile(): Promise<ACPAgentProfile>;
    setProfile(patch: ACPAgentProfilePatch): Promise<ACPAgentProfile>;
  };
  conversations_list(limit?: number): Promise<ACPConversationSummary[]>;
  conversations_get_thread(thread_id: string): Promise<ACPConversationSummary | null>;
  conversations_list_messages(input: {
    thread_id: string;
    limit?: number;
    before_ts?: number;
  }): Promise<ACPConversationMessage[]>;
  qagent_run_quick(max_cases?: number): Promise<ACPQAgentLastRun>;
  qagent_get_last_run(): Promise<ACPQAgentLastRun>;
  qagent_list_runs(limit?: number): Promise<ACPQAgentRun[]>;
  qagent_get_run(run_id: string): Promise<{ run: ACPQAgentRun | null; cases: ACPQAgentCase[] }>;
  users_list(input?: ACPUsersListInput): Promise<ACPUsersListResult>;
}

type ACPAdminRecord = ACPAdminUser & { password: string };

type CreateAgentApiOptions = {
  mode?: "mock" | "wormholes";
  dev_mode?: boolean;
  server_url?: string;
};

type WHReqEnvelope = {
  _v: 2;
  _id: string;
  _kind: "REQ";
  _sid?: string;
  _payload: {
    _module: string;
    _op: string;
    _params: Record<string, unknown>;
  };
};

type WHResEnvelope = {
  _v: number;
  _id: string;
  _kind: "RES";
  _rid: string;
  _ts: number;
  _sid?: string;
  _payload: {
    _ok: boolean;
    _ts: number;
    _pt: number;
    _result: unknown;
  };
};

const STORAGE_SERVER_URL_KEY = "acp.server_url";
const DEFAULT_SERVER_URL = "http://127.0.0.1:3090";
const MASK_SENTINEL = "••••••••";
const AZURE_SKILL_ID = "@xpell/agent-skill-azure";

function is_plain_object(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function as_text(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function normalize_string_array(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const entry of value) {
    if (typeof entry !== "string") continue;
    const trimmed = entry.trim();
    if (!trimmed || seen.has(trimmed)) continue;
    seen.add(trimmed);
    out.push(trimmed);
  }
  return out;
}

function normalize_language_policy(value: unknown): ACPAgentLanguagePolicy {
  const normalized = as_text(value).toLowerCase();
  if (normalized === "spanish") return "spanish";
  if (normalized === "english") return "english";
  return "auto";
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
  let cursor: unknown = obj;
  for (const key of parts) {
    if (!is_plain_object(cursor)) return undefined;
    cursor = cursor[key];
  }
  return cursor;
}

function set_by_path(obj: Record<string, unknown>, dotted_path: string, value: unknown): void {
  const parts = split_path(dotted_path);
  if (parts.length === 0) return;
  let cursor: Record<string, unknown> = obj;
  for (let idx = 0; idx < parts.length - 1; idx += 1) {
    const key = parts[idx];
    const next = cursor[key];
    if (!is_plain_object(next)) {
      cursor[key] = {};
    }
    cursor = cursor[key] as Record<string, unknown>;
  }
  cursor[parts[parts.length - 1]] = value;
}

function delete_by_path(obj: Record<string, unknown>, dotted_path: string): void {
  const parts = split_path(dotted_path);
  if (parts.length === 0) return;
  let cursor: unknown = obj;
  for (let idx = 0; idx < parts.length - 1; idx += 1) {
    if (!is_plain_object(cursor)) return;
    cursor = cursor[parts[idx]];
  }
  if (!is_plain_object(cursor)) return;
  delete cursor[parts[parts.length - 1]];
}

function deep_merge(base: Record<string, unknown>, patch: Record<string, unknown>): Record<string, unknown> {
  const out = deep_clone(base);
  for (const [key, value] of Object.entries(patch)) {
    if (is_plain_object(out[key]) && is_plain_object(value)) {
      out[key] = deep_merge(out[key] as Record<string, unknown>, value);
      continue;
    }
    out[key] = deep_clone(value);
  }
  return out;
}

function apply_sensitive_patch(
  existing: Record<string, unknown>,
  patch: Record<string, unknown>,
  sensitive_paths: string[]
): Record<string, unknown> {
  const patch_copy = deep_clone(patch);
  for (const path_value of sensitive_paths) {
    const incoming = get_by_path(patch_copy, path_value);
    if (incoming !== MASK_SENTINEL) continue;
    const current = get_by_path(existing, path_value);
    if (current === undefined) {
      delete_by_path(patch_copy, path_value);
    } else {
      set_by_path(patch_copy, path_value, current);
    }
  }
  return deep_merge(existing, patch_copy);
}

function mask_sensitive(settings: Record<string, unknown>, sensitive_paths: string[]): Record<string, unknown> {
  const out = deep_clone(settings);
  for (const path_value of sensitive_paths) {
    const current = get_by_path(out, path_value);
    if (current === undefined || current === null) continue;
    if (typeof current === "string" && current.length === 0) continue;
    if (Array.isArray(current) && current.length === 0) continue;
    set_by_path(out, path_value, MASK_SENTINEL);
  }
  return out;
}

function normalize_server_url(input: string): string {
  const trimmed = input.trim();
  if (!trimmed) throw new Error("Server URL is required.");

  let parsed: URL;
  try {
    parsed = new URL(trimmed);
  } catch {
    throw new Error(`Invalid server URL: ${trimmed}`);
  }

  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error("Server URL must start with http:// or https://");
  }

  parsed.hash = "";
  parsed.search = "";

  const normalized = parsed.toString().replace(/\/+$/, "");
  return normalized;
}

function read_storage(): Storage | null {
  if (typeof window === "undefined") return null;
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

function read_query_server_url(): string | undefined {
  if (typeof window === "undefined") return undefined;
  try {
    const url = new URL(window.location.href);
    const candidate = url.searchParams.get("server");
    if (!candidate || !candidate.trim()) return undefined;
    return normalize_server_url(candidate);
  } catch {
    return undefined;
  }
}

function read_stored_server_url(): string | undefined {
  const storage = read_storage();
  if (!storage) return undefined;
  const raw = storage.getItem(STORAGE_SERVER_URL_KEY);
  if (!raw || !raw.trim()) return undefined;
  try {
    return normalize_server_url(raw);
  } catch {
    return undefined;
  }
}

function persist_server_url(server_url: string): void {
  const storage = read_storage();
  if (!storage) return;
  storage.setItem(STORAGE_SERVER_URL_KEY, server_url);
}

function resolve_server_url(opts: CreateAgentApiOptions): string {
  if (opts.server_url && opts.server_url.trim()) {
    const normalized = normalize_server_url(opts.server_url);
    persist_server_url(normalized);
    return normalized;
  }

  const query_server_url = read_query_server_url();
  if (query_server_url) {
    persist_server_url(query_server_url);
    return query_server_url;
  }

  const stored_server_url = read_stored_server_url();
  if (stored_server_url) return stored_server_url;

  persist_server_url(DEFAULT_SERVER_URL);
  return DEFAULT_SERVER_URL;
}

function create_req_id(): string {
  const rand = Math.random().toString(36).slice(2, 10);
  return `acp_req_${Date.now()}_${rand}`;
}

function create_req_envelope(
  module_name: string,
  op: string,
  params: Record<string, unknown>,
  sid?: string
): WHReqEnvelope {
  return {
    _v: 2,
    _id: create_req_id(),
    _kind: "REQ",
    ...(sid && sid.trim() ? { _sid: sid.trim() } : {}),
    _payload: {
      _module: module_name,
      _op: op,
      _params: params
    }
  };
}

function ensure_res_envelope(value: unknown): WHResEnvelope {
  if (!is_plain_object(value)) {
    throw new Error("Invalid Wormholes response envelope (expected object).");
  }
  if (value._kind !== "RES") {
    throw new Error("Invalid Wormholes response envelope kind.");
  }
  if (!is_plain_object(value._payload)) {
    throw new Error("Invalid Wormholes response payload.");
  }
  const payload = value._payload;
  if (typeof payload._ok !== "boolean") {
    throw new Error("Invalid Wormholes response _ok flag.");
  }
  return value as WHResEnvelope;
}

function read_error_message(result: unknown): string {
  if (!is_plain_object(result)) return String(result);
  const msg = as_text(result.message) || as_text(result._message) || as_text(result.error);
  if (msg) return msg;
  return JSON.stringify(result);
}

function read_error_code(result: unknown): string {
  if (!is_plain_object(result)) return "E_AGENT_REMOTE";
  return as_text(result._code) || as_text(result.code) || "E_AGENT_REMOTE";
}

export class AgentApiError extends Error {
  code: string;
  auth_required: boolean;
  network: boolean;

  constructor(args: { message: string; code: string; auth_required?: boolean; network?: boolean }) {
    super(args.message);
    this.name = "AgentApiError";
    this.code = args.code;
    this.auth_required = args.auth_required === true;
    this.network = args.network === true;
  }
}

function to_agent_profile(payload: unknown): ACPAgentProfile {
  const value = is_plain_object(payload) ? payload : {};
  const identity_obj = is_plain_object(value.identity) ? value.identity : {};
  const name = as_text(value.name) || as_text(identity_obj.name);
  const role = as_text(value.role) || as_text(identity_obj.role);
  const system_prompt =
    typeof value.system_prompt === "string"
      ? value.system_prompt
      : (typeof identity_obj.system_prompt === "string" ? identity_obj.system_prompt : "");
  const language_policy = normalize_language_policy(value.language_policy ?? identity_obj.language_policy);
  return {
    agent_id: as_text(value.agent_id) || "unknown",
    env: as_text(value.env) || "default",
    agent_runtime_version: as_text(value.agent_runtime_version) || as_text(value.version) || "unknown",
    xpell_version: as_text(value.xpell_version) || "unknown",
    connected: value.connected !== false,
    identity: {
      name,
      role,
      system_prompt,
      language_policy
    }
  };
}

function to_user_summary(payload: unknown): ACPUserSummary | null {
  const value = is_plain_object(payload) ? payload : {};
  const user_id = as_text(value._id) || as_text(value.user_id);
  if (!user_id) return null;
  const role_raw = (as_text(value._role) || as_text(value.role)).toLowerCase();
  const role: "admin" | "owner" | "customer" =
    role_raw === "owner" ? "owner" : role_raw === "admin" ? "admin" : "customer";
  const channels_value = Array.isArray(value._channels) ? value._channels : value.channels;
  const channels = Array.isArray(channels_value)
    ? channels_value.map((entry) => as_text(entry)).filter((entry) => entry.length > 0)
    : [];
  const created_at = Number(value._created_at ?? value.created_at);
  const updated_at = Number(value._updated_at ?? value.updated_at);
  const display_id = as_text(value._display_id) || as_text(value.display_id);
  const display_name = as_text(value._display_name) || as_text(value.display_name) || as_text(value.name) || user_id;

  return {
    user_id,
    ...(display_id ? { display_id } : {}),
    display_name,
    role,
    channels,
    created_at: Number.isFinite(created_at) ? Math.floor(created_at) : 0,
    updated_at: Number.isFinite(updated_at) ? Math.floor(updated_at) : 0
  };
}

function to_conversation_summary(payload: unknown): ACPConversationSummary | null {
  const value = is_plain_object(payload) ? payload : {};
  const thread_id = as_text(value.thread_id);
  const channel = as_text(value.channel);
  const channel_thread_id = as_text(value.channel_thread_id);
  const user_id = as_text(value.user_id);
  const status = as_text(value.status) || "open";
  if (!thread_id || !channel || !channel_thread_id || !user_id) return null;

  const created_at = Number(value.created_at);
  const updated_at = Number(value.updated_at);
  const tags = Array.isArray(value.tags) ? value.tags.map((entry) => as_text(entry)).filter((entry) => entry.length > 0) : [];

  return {
    thread_id,
    channel,
    channel_thread_id,
    user_id,
    status,
    created_at: Number.isFinite(created_at) ? Math.floor(created_at) : 0,
    updated_at: Number.isFinite(updated_at) ? Math.floor(updated_at) : 0,
    tags
  };
}

function to_conversation_message(payload: unknown): ACPConversationMessage | null {
  const value = is_plain_object(payload) ? payload : {};
  const message_id = as_text(value.message_id);
  const thread_id = as_text(value.thread_id);
  const direction = as_text(value.direction).toLowerCase();
  const sender = as_text(value.sender) || "agent";
  const text = typeof value.text === "string" ? value.text.trim() : "";
  const ts = Number(value.ts);
  const channel_message_id = as_text(value.channel_message_id);
  if (!message_id || !thread_id || (direction !== "in" && direction !== "out")) return null;

  return {
    message_id,
    thread_id,
    direction: direction as "in" | "out",
    sender,
    text,
    ts: Number.isFinite(ts) ? Math.floor(ts) : 0,
    ...(channel_message_id ? { channel_message_id } : {})
  };
}

function read_qagent_totals_from_string(value: unknown): ACPQAgentTotals {
  if (typeof value !== "string" || !value.trim()) {
    return {
      cases_total: 0,
      cases_passed: 0,
      avg_score: 0,
      pass_rate: 0
    };
  }
  try {
    return to_qagent_totals(JSON.parse(value));
  } catch {
    return {
      cases_total: 0,
      cases_passed: 0,
      avg_score: 0,
      pass_rate: 0
    };
  }
}

function to_qagent_totals(payload: unknown): ACPQAgentTotals {
  const value = is_plain_object(payload) ? payload : {};
  const cases_total = Number(value.cases_total);
  const cases_passed = Number(value.cases_passed);
  const avg_score = Number(value.avg_score);
  const pass_rate = Number(value.pass_rate);
  return {
    cases_total: Number.isFinite(cases_total) ? Math.max(0, Math.floor(cases_total)) : 0,
    cases_passed: Number.isFinite(cases_passed) ? Math.max(0, Math.floor(cases_passed)) : 0,
    avg_score: Number.isFinite(avg_score) ? Math.min(1, Math.max(0, avg_score)) : 0,
    pass_rate: Number.isFinite(pass_rate) ? Math.min(1, Math.max(0, pass_rate)) : 0
  };
}

function to_qagent_run(payload: unknown, totals_override?: unknown): ACPQAgentRun | null {
  const value = is_plain_object(payload) ? payload : {};
  const run_id = as_text(value._id) || as_text(value.run_id);
  if (!run_id) return null;

  const kb_files_source = Array.isArray(value._kb_files) ? value._kb_files : value.kb_files;
  const kb_files = Array.isArray(kb_files_source)
    ? kb_files_source.map((entry) => as_text(entry)).filter((entry) => entry.length > 0)
    : [];

  const created_at = Number(value._created_at ?? value.created_at);
  const updated_at = Number(value._updated_at ?? value.updated_at);
  const totals = typeof totals_override !== "undefined"
    ? to_qagent_totals(totals_override)
    : read_qagent_totals_from_string(value._totals_json);

  return {
    run_id,
    status: as_text(value._status) || as_text(value.status) || "created",
    created_at: Number.isFinite(created_at) ? Math.floor(created_at) : 0,
    updated_at: Number.isFinite(updated_at) ? Math.floor(updated_at) : 0,
    agent_name: as_text(value._agent_name) || as_text(value.agent_name) || "XBot",
    agent_role: as_text(value._agent_role) || as_text(value.agent_role) || "Assistant",
    summary: as_text(value._summary) || as_text(value.summary),
    kb_files,
    totals
  };
}

function to_qagent_case(payload: unknown): ACPQAgentCase | null {
  const value = is_plain_object(payload) ? payload : {};
  const case_id = as_text(value._id) || as_text(value.case_id);
  if (!case_id) return null;

  const expected_source = Array.isArray(value._expected_facts)
    ? value._expected_facts
    : (Array.isArray(value.expected_facts) ? value.expected_facts : undefined);
  const expected_facts = Array.isArray(expected_source)
    ? expected_source.map((entry) => as_text(entry)).filter((entry) => entry.length > 0)
    : [];

  const case_idx = Number(value._case_idx ?? value.case_idx);
  const score = Number(value._score ?? value.score);
  const audience = (as_text(value._audience) || as_text(value.audience)).toLowerCase() === "admin" ? "admin" : "customer";

  return {
    case_id,
    case_idx: Number.isFinite(case_idx) ? Math.max(0, Math.floor(case_idx)) : 0,
    audience,
    intent_id: as_text(value._intent_id) || as_text(value.intent_id),
    question: as_text(value._question) || as_text(value.question),
    expected_facts,
    answer: typeof value._answer === "string" ? value._answer : (typeof value.answer === "string" ? value.answer : ""),
    score: Number.isFinite(score) ? Math.min(1, Math.max(0, score)) : 0,
    judge_notes: typeof value._judge_notes === "string" ? value._judge_notes : (typeof value.judge_notes === "string" ? value.judge_notes : "")
  };
}

function to_qagent_last_run(payload: unknown): ACPQAgentLastRun {
  const value = is_plain_object(payload) ? payload : {};
  const run = to_qagent_run(value.run, value.totals);
  const top_failures_source = Array.isArray(value.top_failures) ? value.top_failures : [];
  return {
    run,
    top_failures: top_failures_source.map((entry) => to_qagent_case(entry)).filter((entry): entry is ACPQAgentCase => entry !== null)
  };
}

function is_skill_field_type(value: unknown): value is ACPSkillFieldType {
  return value === "string" || value === "number" || value === "boolean" || value === "select" || value === "string_list";
}

function to_skill_field(value: unknown): ACPSkillField | null {
  const entry = is_plain_object(value) ? value : {};
  const key = as_text(entry.key);
  const label = as_text(entry.label);
  const type = entry.type;
  if (!key || !label || !is_skill_field_type(type)) return null;

  const options = Array.isArray(entry.options)
    ? entry.options
        .map((option) => {
          const option_obj = is_plain_object(option) ? option : {};
          const option_label = as_text(option_obj.label);
          if (!option_label) return null;
          return {
            label: option_label,
            value: deep_clone(option_obj.value)
          } satisfies ACPSkillFieldOption;
        })
        .filter((option): option is ACPSkillFieldOption => option !== null)
    : [];

  return {
    key,
    label,
    type,
    ...(typeof entry.help === "string" ? { help: entry.help } : {}),
    ...(typeof entry.secret === "boolean" ? { secret: entry.secret } : {}),
    ...(options.length > 0 ? { options } : {}),
    ...(typeof entry.placeholder === "string" ? { placeholder: entry.placeholder } : {})
  };
}

function to_skill_schema(value: unknown): ACPSkillSchema | undefined {
  const schema = is_plain_object(value) ? value : {};
  const fields = Array.isArray(schema.fields)
    ? schema.fields.map((field) => to_skill_field(field)).filter((field): field is ACPSkillField => field !== null)
    : [];
  if (fields.length === 0) return undefined;
  return {
    ...(typeof schema.title === "string" ? { title: schema.title } : {}),
    fields
  };
}

function to_skill_action(value: unknown): ACPSkillAction | null {
  const entry = is_plain_object(value) ? value : {};
  const id = as_text(entry.id);
  const label = as_text(entry.label);
  const op_obj = is_plain_object(entry.op) ? entry.op : {};
  const module_name = as_text(op_obj.module);
  const op_name = as_text(op_obj.op);
  if (!id || !label || !module_name || !op_name) return null;
  const kind_raw = as_text(entry.kind).toLowerCase();
  const kind = kind_raw === "primary" || kind_raw === "danger" || kind_raw === "secondary" ? kind_raw : undefined;
  const params_schema = is_plain_object(entry.params_schema)
    ? to_skill_schema(entry.params_schema)
    : undefined;
  const confirm_obj = is_plain_object(entry.confirm) ? entry.confirm : {};
  const confirm_title = as_text(confirm_obj.title);
  const confirm_body = as_text(confirm_obj.body);
  return {
    id,
    label,
    ...(kind ? { kind } : {}),
    op: {
      module: module_name,
      op: op_name
    },
    ...(params_schema ? { params_schema: { fields: params_schema.fields } } : {}),
    ...(confirm_title && confirm_body ? { confirm: { title: confirm_title, body: confirm_body } } : {})
  };
}

function build_fallback_skill_actions(skill_id: string): ACPSkillAction[] {
  if (skill_id === "@xpell/agent-skill-telegram") {
    return [
      {
        id: "verify_token",
        label: "Verify Token",
        kind: "secondary",
        op: { module: "telegram", op: "verify_token" }
      },
      {
        id: "status",
        label: "Status",
        kind: "secondary",
        op: { module: "telegram", op: "status" }
      },
      {
        id: "start",
        label: "Start",
        kind: "primary",
        op: { module: "telegram", op: "start" },
        params_schema: {
          fields: [
            {
              key: "mode",
              label: "Mode",
              type: "select",
              options: [
                { label: "Polling", value: "polling" },
                { label: "Webhook", value: "webhook" }
              ]
            }
          ]
        }
      },
      {
        id: "stop",
        label: "Stop",
        kind: "danger",
        op: { module: "telegram", op: "stop" },
        confirm: {
          title: "Stop Telegram",
          body: "Stop the Telegram connector?"
        }
      }
    ];
  }

  if (skill_id === AZURE_SKILL_ID) {
    return [
      {
        id: "status",
        label: "Status",
        kind: "secondary",
        op: { module: "azure", op: "status" }
      },
      {
        id: "test_connection",
        label: "Test Connection",
        kind: "primary",
        op: { module: "azure", op: "test_connection" }
      }
    ];
  }

  return [];
}

function to_skill_config_result(skill_id: string, payload: unknown): ACPSkillConfig {
  const result = is_plain_object(payload) ? payload : {};
  const settings = is_plain_object(result.settings) ? deep_clone(result.settings) : {};
  const masked = is_plain_object(result.masked)
    ? Object.fromEntries(
        Object.entries(result.masked).map(([key, value]) => [key, value === true])
      )
    : {};
  const schema = to_skill_schema(result.schema);
  return {
    skill_id,
    settings,
    masked,
    ...(schema ? { schema } : {}),
    actions: build_fallback_skill_actions(skill_id)
  };
}

function to_intent_record(payload: unknown): ACPIntentRecord | null {
  const value = is_plain_object(payload) ? payload : {};
  const intent_id = as_text(value.intent_id);
  const title = as_text(value.title);
  const skill_id = as_text(value.skill_id);
  if (!intent_id || !title || !skill_id) return null;
  const roles_allowed = normalize_string_array(value.roles_allowed).filter(
    (entry): entry is ACPIntentRole => entry === "owner" || entry === "admin" || entry === "customer"
  );
  return {
    intent_id,
    title,
    ...(as_text(value.description) ? { description: as_text(value.description) } : {}),
    skill_id,
    enabled: value.enabled === true,
    priority: Number.isFinite(Number(value.priority)) ? Math.floor(Number(value.priority)) : 100,
    roles_allowed,
    channels_allowed: normalize_string_array(value.channels_allowed),
    synonyms: normalize_string_array(value.synonyms),
    examples: normalize_string_array(value.examples),
    default_params_json: typeof value.default_params_json === "string" ? value.default_params_json : ""
  };
}

function create_mock_agent_api(opts: CreateAgentApiOptions): AgentApi {
  const dev_mode = opts.dev_mode === true;
  let server_url = resolve_server_url(opts);
  const admins: ACPAdminRecord[] = dev_mode
    ? [
        {
          id: "admin-1",
          name: "Admin",
          username: "admin",
          password: "admin",
          role: "admin"
        }
      ]
    : [];

  const skills: ACPSkill[] = [
    { id: "@xpell/agent-skill-telegram", version: "0.1.0-alpha.0", enabled: true, status: "loaded" },
    { id: "xpell-agent-skill-echo", version: "0.1.0-alpha.0", enabled: true, status: "loaded" }
  ];

  const skill_settings: Record<string, Record<string, unknown>> = {
    "@xpell/agent-skill-telegram": {
      bot_token: "",
      admin_chat_ids: [],
      mode: "polling",
      auto_start: false,
      polling: { timeout_sec: 30 },
      webhook: { url: "", secret_token: "" }
    }
  };
  const skill_sensitive_paths: Record<string, string[]> = {
    "@xpell/agent-skill-telegram": ["bot_token", "webhook.secret_token"]
  };
  const skill_runtime: Record<string, { running: boolean; mode: string }> = {
    "@xpell/agent-skill-telegram": {
      running: false,
      mode: "polling"
    }
  };

  const about: ACPAbout = {
    agent_version: "0.1.0-alpha.0-mock",
    xpell_version: "2.0.0-alpha.5",
    connected: false,
    server_url
  };
  let agent_profile: ACPAgentProfile = {
    agent_id: "xbot-mock",
    env: "default",
    agent_runtime_version: about.agent_version,
    xpell_version: about.xpell_version,
      connected: false,
      identity: {
        name: "XBot",
        role: "Assistant",
        system_prompt:
          "You are {agent_name} ({agent_id}), a helpful {agent_role}. Reply in the user's language; Spanish if they write Spanish, English otherwise.",
        language_policy: "auto"
      }
    };

  let admin_counter = admins.length + 1;
  let session_counter = 0;

  function as_public_admin(record: ACPAdminRecord): ACPAdminUser {
    return {
      id: record.id,
      name: record.name,
      username: record.username,
      role: record.role
    };
  }

  function find_admin_by_identifier(identifier: string): ACPAdminRecord | undefined {
    const needle = identifier.trim().toLowerCase();
    return admins.find((admin) => admin.username.trim().toLowerCase() === needle);
  }

  function find_admin_by_id(id: string): ACPAdminRecord {
    const found = admins.find((admin) => admin.id === id);
    if (!found) throw new Error(`Admin user '${id}' was not found.`);
    return found;
  }

  function find_skill(skill_id: string): ACPSkill {
    const found = skills.find((skill) => skill.id === skill_id);
    if (!found) throw new Error(`Skill '${skill_id}' was not found.`);
    return found;
  }

  return {
    async exec(module_name: string, op: string, params: Record<string, unknown> = {}): Promise<unknown> {
      if (module_name === "telegram" && op === "status") {
        const runtime = skill_runtime["@xpell/agent-skill-telegram"] ?? { running: false, mode: "polling" };
        return {
          available: true,
          running: runtime.running,
          mode: runtime.mode
        } satisfies ACPSkillRuntimeStatus;
      }
      if (module_name === "telegram" && op === "start") {
        const mode = as_text(params.mode) || "polling";
        if (mode === "polling") {
          const runtime = skill_runtime["@xpell/agent-skill-telegram"] ?? { running: false, mode: "polling" };
          runtime.running = true;
          runtime.mode = "polling";
          skill_runtime["@xpell/agent-skill-telegram"] = runtime;
          return {
            available: true,
            running: true,
            mode: "polling"
          } satisfies ACPSkillRuntimeStatus;
        }
        return {
          available: true,
          running: true,
          mode
        } satisfies ACPSkillRuntimeStatus;
      }
      if (module_name === "telegram" && op === "stop") {
        const runtime = skill_runtime["@xpell/agent-skill-telegram"] ?? { running: false, mode: "polling" };
        runtime.running = false;
        skill_runtime["@xpell/agent-skill-telegram"] = runtime;
        return {
          available: true,
          running: false,
          mode: runtime.mode
        } satisfies ACPSkillRuntimeStatus;
      }
      if (module_name === "azure" && op === "status") {
        return {
          configured: true,
          has_openai: true,
          has_speech: true
        };
      }
      if (module_name === "azure" && op === "test_connection") {
        return {
          openai: { ok: true },
          speech: { ok: true },
          _ts: Date.now()
        } satisfies ACPAzureConnectionTestResult;
      }
      throw new Error(`Unsupported mock exec: ${module_name}.${op}`);
    },

    auth: {
      async login(identifier: string, password: string): Promise<ACPLoginResponse> {
        const admin = find_admin_by_identifier(identifier);
        if (!admin || admin.password !== password) {
          throw new AgentApiError({ message: "Invalid admin credentials.", code: "E_USERS_AUTH_FAILED" });
        }

        session_counter += 1;
        return {
          token: `mock-session-${String(session_counter).padStart(6, "0")}`,
          user: {
            user_id: admin.id,
            role: admin.role,
            name: admin.name
          }
        };
      },

      async logout(): Promise<void> {},

      async session(token?: string): Promise<AuthSessionResponse> {
        if (!token) return { is_authenticated: false, user: null };
        const admin = admins[0];
        if (!admin) return { is_authenticated: false, user: null };
        return {
          is_authenticated: true,
          user: {
            user_id: admin.id,
            role: admin.role,
            name: admin.name
          }
        };
      }
    },

    users: {
      async listAdmins(): Promise<ACPAdminUser[]> {
        return admins.map(as_public_admin);
      },

      async createAdmin(input: ACPCreateAdminInput): Promise<ACPAdminUser> {
        const created: ACPAdminRecord = {
          id: `admin-${String(admin_counter).padStart(3, "0")}`,
          name: input.name.trim(),
          username: input.username.trim(),
          password: input.password,
          role: "admin"
        };
        admin_counter += 1;
        admins.push(created);
        return as_public_admin(created);
      },

      async updateAdmin(input: ACPUpdateAdminInput): Promise<ACPAdminUser> {
        const target = find_admin_by_id(input.id);
        if (input.name && input.name.trim()) target.name = input.name.trim();
        if (input.username && input.username.trim()) target.username = input.username.trim();
        if (input.password && input.password.length > 0) target.password = input.password;
        return as_public_admin(target);
      },

      async deleteAdmin(id: string): Promise<void> {
        const index = admins.findIndex((entry) => entry.id === id);
        if (index < 0) throw new Error(`Admin user '${id}' was not found.`);
        admins.splice(index, 1);
      }
    },

    skills: {
      async list(): Promise<ACPSkill[]> {
        return skills.map((skill) => ({
          ...deep_clone(skill),
          actions: build_fallback_skill_actions(skill.id)
        }));
      },

      async getConfig(skill_id: string): Promise<ACPSkillConfig> {
        find_skill(skill_id);
        const stored = deep_clone(skill_settings[skill_id] ?? {});
        const masked_settings = mask_sensitive(stored, skill_sensitive_paths[skill_id] ?? []);
        return {
          skill_id,
          settings: masked_settings,
          masked: Object.fromEntries((skill_sensitive_paths[skill_id] ?? []).map((path_value) => [path_value, true])),
          actions: build_fallback_skill_actions(skill_id)
        };
      },

      async enable(skill_id: string): Promise<ACPSkill> {
        const skill = find_skill(skill_id);
        skill.enabled = true;
        skill.status = "loaded";
        delete skill.error;
        return deep_clone(skill);
      },

      async disable(skill_id: string): Promise<ACPSkill> {
        const skill = find_skill(skill_id);
        skill.enabled = false;
        skill.status = "disabled";
        delete skill.error;
        return deep_clone(skill);
      },

      async getSettings(skill_id: string): Promise<Record<string, unknown>> {
        find_skill(skill_id);
        const stored = deep_clone(skill_settings[skill_id] ?? {});
        return mask_sensitive(stored, skill_sensitive_paths[skill_id] ?? []);
      },

      async updateSettings(skill_id: string, settings: Record<string, unknown>): Promise<Record<string, unknown>> {
        find_skill(skill_id);
        const existing = is_plain_object(skill_settings[skill_id]) ? deep_clone(skill_settings[skill_id]) : {};
        skill_settings[skill_id] = apply_sensitive_patch(existing, settings, skill_sensitive_paths[skill_id] ?? []);
        if (skill_runtime[skill_id]) {
          skill_runtime[skill_id].mode = as_text(skill_settings[skill_id]?.mode) || skill_runtime[skill_id].mode;
        }
        return mask_sensitive(skill_settings[skill_id], skill_sensitive_paths[skill_id] ?? []);
      },

      async getRuntimeStatus(skill_id: string): Promise<ACPSkillRuntimeStatus> {
        find_skill(skill_id);
        if (skill_id !== "@xpell/agent-skill-telegram") {
          return { available: false, running: false };
        }
        const runtime = skill_runtime[skill_id] ?? { running: false, mode: "polling" };
        const mode = as_text(skill_settings[skill_id]?.mode) || runtime.mode;
        return {
          available: true,
          running: runtime.running,
          mode
        };
      },

      async startPolling(skill_id: string): Promise<ACPSkillRuntimeStatus> {
        find_skill(skill_id);
        if (skill_id !== "@xpell/agent-skill-telegram") {
          return { available: false, running: false };
        }
        const runtime = skill_runtime[skill_id] ?? { running: false, mode: "polling" };
        runtime.running = true;
        runtime.mode = "polling";
        skill_runtime[skill_id] = runtime;
        return {
          available: true,
          running: true,
          mode: "polling"
        };
      },

      async stopPolling(skill_id: string): Promise<ACPSkillRuntimeStatus> {
        find_skill(skill_id);
        if (skill_id !== "@xpell/agent-skill-telegram") {
          return { available: false, running: false };
        }
        const runtime = skill_runtime[skill_id] ?? { running: false, mode: "polling" };
        runtime.running = false;
        skill_runtime[skill_id] = runtime;
        return {
          available: true,
          running: false,
          mode: runtime.mode
        };
      },

      async verifyToken(skill_id: string, bot_token?: string): Promise<ACPTelegramTokenVerify> {
        find_skill(skill_id);
        if (skill_id !== "@xpell/agent-skill-telegram") {
          return {
            valid: false,
            source: "configured",
            error: "Token verification is available only for Telegram skill."
          };
        }

        const source: "input" | "configured" = bot_token && bot_token.trim().length > 0 ? "input" : "configured";
        const candidate = source === "input" ? bot_token!.trim() : as_text(skill_settings[skill_id]?.bot_token);
        const valid = candidate.length >= 10 && candidate !== MASK_SENTINEL;

        if (!valid) {
          return {
            valid: false,
            source,
            error: "Telegram token appears invalid."
          };
        }

        return {
          valid: true,
          source,
          bot: {
            id: "mock-bot",
            username: "mock_bot",
            first_name: "Mock"
          }
        };
      },

      async importEnv(skill_id: string): Promise<ACPSkillImportEnvResult> {
        find_skill(skill_id);
        if (skill_id !== AZURE_SKILL_ID) {
          return {
            imported_count: 0,
            imported_keys: [],
            detail: "env import is available only for Azure skill"
          };
        }
        return {
          imported_count: 0,
          imported_keys: [],
          detail: "mock mode: process env import is not available in browser runtime"
        };
      },

      async testAzureConnection(skill_id: string): Promise<ACPAzureConnectionTestResult> {
        find_skill(skill_id);
        if (skill_id !== AZURE_SKILL_ID) {
          return {
            openai: {
              ok: false,
              detail: "skipped: available only for Azure skill"
            },
            speech: {
              ok: false,
              detail: "skipped: available only for Azure skill"
            },
            _ts: Date.now()
          };
        }
        return {
          openai: {
            ok: false,
            detail: "skipped: mock mode"
          },
          speech: {
            ok: false,
            detail: "skipped: mock mode"
          },
          _ts: Date.now()
        };
      }
    },

    intents: {
      async list(): Promise<ACPIntentRecord[]> {
        return [];
      },
      async setEnabled(): Promise<void> {
        return;
      },
      async updateConfig(): Promise<void> {
        return;
      }
    },

    system: {
      async getAbout(): Promise<ACPAbout> {
        return deep_clone(about);
      },
      getServerUrl(): string {
        return server_url;
      },
      setServerUrl(next_url: string): string {
        server_url = normalize_server_url(next_url);
        persist_server_url(server_url);
        return server_url;
      },
      async resetDb(): Promise<ACPResetDbResult> {
        const users_deleted = admins.length;
        admins.splice(0, admins.length);
        return {
          ok: true,
          threads_deleted: 0,
          messages_deleted: 0,
          users_deleted,
          sessions_deleted: 0
        };
      }
    },

    agent: {
      async getProfile(): Promise<ACPAgentProfile> {
        return deep_clone(agent_profile);
      },

      async setProfile(patch: ACPAgentProfilePatch): Promise<ACPAgentProfile> {
        const patch_obj = is_plain_object(patch) ? patch : {};
        const next_identity: ACPAgentIdentity = {
          name: Object.prototype.hasOwnProperty.call(patch_obj, "name")
            ? as_text(patch_obj.name)
            : agent_profile.identity.name,
          role: Object.prototype.hasOwnProperty.call(patch_obj, "role")
            ? as_text(patch_obj.role)
            : agent_profile.identity.role,
          system_prompt: Object.prototype.hasOwnProperty.call(patch_obj, "system_prompt")
            ? (typeof patch_obj.system_prompt === "string" ? patch_obj.system_prompt.trim() : "")
            : agent_profile.identity.system_prompt,
          language_policy: Object.prototype.hasOwnProperty.call(patch_obj, "language_policy")
            ? normalize_language_policy(patch_obj.language_policy)
            : agent_profile.identity.language_policy
        };
        agent_profile = {
          ...agent_profile,
          identity: next_identity
        };
        return deep_clone(agent_profile);
      }
    },

    async conversations_list(): Promise<ACPConversationSummary[]> {
      return [];
    },

    async conversations_get_thread(): Promise<ACPConversationSummary | null> {
      return null;
    },

    async conversations_list_messages(): Promise<ACPConversationMessage[]> {
      return [];
    },

    async qagent_run_quick(): Promise<ACPQAgentLastRun> {
      return {
        run: null,
        top_failures: []
      };
    },

    async qagent_get_last_run(): Promise<ACPQAgentLastRun> {
      return {
        run: null,
        top_failures: []
      };
    },

    async qagent_list_runs(): Promise<ACPQAgentRun[]> {
      return [];
    },

    async qagent_get_run(): Promise<{ run: ACPQAgentRun | null; cases: ACPQAgentCase[] }> {
      return {
        run: null,
        cases: []
      };
    },

    async users_list(input: ACPUsersListInput = {}): Promise<ACPUsersListResult> {
      const q = as_text(input.q).toLowerCase();
      const items = admins
        .map((admin) => ({
          user_id: admin.id,
          display_name: admin.name,
          role: admin.role,
          channels: [],
          created_at: 0,
          updated_at: 0
        }) satisfies ACPUserSummary)
        .filter((user) => {
          if (!q) return true;
          return (
            user.user_id.toLowerCase().includes(q) ||
            user.display_name.toLowerCase().includes(q) ||
            user.role.toLowerCase().includes(q)
          );
        });

      return {
        items: deep_clone(items)
      };
    }
  };
}

function create_wormholes_api(opts: CreateAgentApiOptions): AgentApi {
  let server_url = resolve_server_url(opts);
  const resolve_sid = (): string | undefined => {
    const token = load_token();
    if (!token) return undefined;
    const trimmed = token.trim();
    return trimmed.length > 0 ? trimmed : undefined;
  };

  const call_xcmd = async <T>(
    module_name: string,
    op: string,
    params: Record<string, unknown>,
    sid?: string
  ): Promise<T> => {
    const endpoint = `${server_url}/wh/v2/call`;
    const req_env = create_req_envelope(module_name, op, params, sid);

    let response: Response;
    try {
      const headers: Record<string, string> = {
        "content-type": "application/json"
      };
      if (sid && sid.trim()) {
        headers["x-wormholes-sid"] = sid.trim();
      }

      response = await fetch(endpoint, {
        method: "POST",
        headers,
        body: JSON.stringify(req_env)
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      throw new AgentApiError({
        message: `Network error: ${message}`,
        code: "E_AGENT_NETWORK",
        network: true
      });
    }

    let raw_body: unknown = null;
    try {
      raw_body = await response.json();
    } catch {
      throw new AgentApiError({
        message: `Invalid response body from ${endpoint}`,
        code: "E_AGENT_BAD_RESPONSE"
      });
    }

    if (!response.ok) {
      throw new AgentApiError({
        message: `HTTP ${response.status} from agent server`,
        code: "E_AGENT_HTTP"
      });
    }

    const res_env = ensure_res_envelope(raw_body);
    if (res_env._payload._ok !== true) {
      const result = res_env._payload._result;
      const code = read_error_code(result);
      const message = read_error_message(result);
      const auth_required = code === "E_AGENT_FORBIDDEN";
      throw new AgentApiError({ message, code, auth_required });
    }

    return res_env._payload._result as T;
  };

  const read_skill_record = (skill_id: string, loaded: unknown): ACPSkill => {
    const loaded_obj = is_plain_object(loaded) ? loaded : {};
    const status_raw = as_text(loaded_obj.status);
    const status: ACPSkillStatus =
      status_raw === "loaded" || status_raw === "error" || status_raw === "disabled" ? status_raw : "disabled";
    const actions = Array.isArray(loaded_obj.actions)
      ? loaded_obj.actions.map((action) => to_skill_action(action)).filter((action): action is ACPSkillAction => action !== null)
      : build_fallback_skill_actions(skill_id);
    const settings_meta = is_plain_object(loaded_obj.settings_meta)
      ? {
          ...(to_skill_schema(loaded_obj.settings_meta.schema) ? { schema: to_skill_schema(loaded_obj.settings_meta.schema) } : {}),
          ...(Array.isArray(loaded_obj.settings_meta.sensitive)
            ? {
                sensitive: loaded_obj.settings_meta.sensitive
                  .map((entry) => as_text(entry))
                  .filter((entry) => entry.length > 0)
              }
            : {})
        }
      : undefined;

    return {
      id: skill_id,
      version: as_text(loaded_obj.version) || "unknown",
      enabled: loaded_obj.enabled === true,
      status,
      ...(as_text(loaded_obj.name) ? { name: as_text(loaded_obj.name) } : {}),
      ...(as_text(loaded_obj.description) ? { description: as_text(loaded_obj.description) } : {}),
      ...(settings_meta ? { settings_meta } : {}),
      ...(actions.length > 0 ? { actions } : {}),
      ...(as_text(loaded_obj.error) ? { error: as_text(loaded_obj.error) } : {})
    };
  };

  return {
    async exec(module_name: string, op: string, params: Record<string, unknown> = {}): Promise<unknown> {
      return call_xcmd(module_name, op, params, resolve_sid());
    },

    auth: {
      async login(identifier: string, password: string): Promise<ACPLoginResponse> {
        const out = await call_xcmd<{ token: unknown; user: unknown }>(
          "users",
          "login",
          {
            username: identifier,
            password
          },
          undefined
        );

        if (!is_plain_object(out.user)) {
          throw new AgentApiError({ message: "Invalid login response: user", code: "E_AGENT_BAD_RESPONSE" });
        }

        const token = as_text(out.token);
        if (!token) {
          throw new AgentApiError({ message: "Invalid login response: token", code: "E_AGENT_BAD_RESPONSE" });
        }
        save_token(token);

        const user: ACPAuthUser = {
          user_id: as_text(out.user.user_id),
          role: as_text(out.user.role) === "owner" ? "owner" : "admin",
          name: as_text(out.user.name) || "Admin"
        };

        if (!user.user_id) {
          throw new AgentApiError({ message: "Invalid login response: user_id", code: "E_AGENT_BAD_RESPONSE" });
        }

        return { token, user };
      },

      async logout(token?: string): Promise<void> {
        const sid = (token && token.trim()) || resolve_sid();
        await call_xcmd("users", "logout", token ? { token } : {}, sid);
        clear_token();
      },

      async session(token?: string): Promise<AuthSessionResponse> {
        if (!token || !token.trim()) {
          return { is_authenticated: false, user: null };
        }

        try {
          const out = await call_xcmd<{ is_authenticated: unknown; user: unknown }>(
            "users",
            "session",
            {},
            token
          );

          const is_authenticated = out.is_authenticated === true;
          if (!is_authenticated || !is_plain_object(out.user)) {
            return { is_authenticated: false, user: null };
          }

          const user: ACPAuthUser = {
            user_id: as_text(out.user.user_id),
            role: as_text(out.user.role) === "owner" ? "owner" : "admin",
            name: as_text(out.user.name) || "Admin"
          };

          if (!user.user_id) return { is_authenticated: false, user: null };
          return { is_authenticated: true, user };
        } catch (err) {
          if (err instanceof AgentApiError && err.code === "E_AGENT_FORBIDDEN") {
            return { is_authenticated: false, user: null };
          }
          throw err;
        }
      }
    },

    users: {
      async listAdmins(): Promise<ACPAdminUser[]> {
        const out = await call_xcmd<{ admins: unknown }>("users", "list_admins", {}, resolve_sid());
        if (!Array.isArray(out.admins)) return [];

        return out.admins
          .map((entry) => {
            if (!is_plain_object(entry)) return null;
            const id = as_text(entry.id);
            const username = as_text(entry.username);
            if (!id || !username) return null;
            return {
              id,
              name: as_text(entry.name) || id,
              username,
              role: as_text(entry.role) === "owner" ? "owner" : "admin"
            } satisfies ACPAdminUser;
          })
          .filter((entry): entry is ACPAdminUser => entry !== null);
      },

      async createAdmin(input: ACPCreateAdminInput): Promise<ACPAdminUser> {
        const out = await call_xcmd<{ admin: unknown }>("users", "create_admin", {
          name: input.name,
          username: input.username,
          password: input.password
        }, resolve_sid());

        if (!is_plain_object(out.admin)) {
          throw new AgentApiError({ message: "Invalid create_admin response", code: "E_AGENT_BAD_RESPONSE" });
        }

        return {
          id: as_text(out.admin.id),
          name: as_text(out.admin.name),
          username: as_text(out.admin.username),
          role: as_text(out.admin.role) === "owner" ? "owner" : "admin"
        };
      },

      async updateAdmin(input: ACPUpdateAdminInput): Promise<ACPAdminUser> {
        const out = await call_xcmd<{ admin: unknown }>("users", "update_admin", {
          id: input.id,
          ...(input.name ? { name: input.name } : {}),
          ...(input.username ? { username: input.username } : {}),
          ...(input.password ? { password: input.password } : {})
        }, resolve_sid());

        if (!is_plain_object(out.admin)) {
          throw new AgentApiError({ message: "Invalid update_admin response", code: "E_AGENT_BAD_RESPONSE" });
        }

        return {
          id: as_text(out.admin.id),
          name: as_text(out.admin.name),
          username: as_text(out.admin.username),
          role: as_text(out.admin.role) === "owner" ? "owner" : "admin"
        };
      },

      async deleteAdmin(id: string): Promise<void> {
        await call_xcmd("users", "delete_admin", { id }, resolve_sid());
      }
    },

    skills: {
      async list(): Promise<ACPSkill[]> {
        const out = await call_xcmd<{ loaded: unknown }>("skills", "list", {}, resolve_sid());
        const loaded_list = Array.isArray(out.loaded) ? out.loaded : [];

        return loaded_list
          .map((entry) => {
            if (!is_plain_object(entry)) return null;
            const skill_id = as_text(entry.id);
            if (!skill_id) return null;
            return read_skill_record(skill_id, entry);
          })
          .filter((entry): entry is ACPSkill => entry !== null);
      },

      async getConfig(skill_id: string): Promise<ACPSkillConfig> {
        const out = await call_xcmd<{ result?: unknown }>(
          "settings",
          "get_skill",
          {
            skill_id,
            include_schema: true,
            include_masked: true
          },
          resolve_sid()
        );

        return to_skill_config_result(skill_id, out.result);
      },

      async enable(skill_id: string): Promise<ACPSkill> {
        const out = await call_xcmd<{ skill?: unknown }>("skills", "enable", { id: skill_id }, resolve_sid());
        if (is_plain_object(out.skill)) {
          return read_skill_record(skill_id, out.skill);
        }
        return {
          id: skill_id,
          version: "unknown",
          enabled: true,
          status: "loaded"
        };
      },

      async disable(skill_id: string): Promise<ACPSkill> {
        const out = await call_xcmd<{ skill?: unknown }>("skills", "disable", { id: skill_id }, resolve_sid());
        if (is_plain_object(out.skill)) {
          return read_skill_record(skill_id, out.skill);
        }
        return {
          id: skill_id,
          version: "unknown",
          enabled: false,
          status: "disabled"
        };
      },

      async getSettings(skill_id: string): Promise<Record<string, unknown>> {
        const out = await call_xcmd<{ result?: unknown }>(
          "settings",
          "get_skill",
          {
            skill_id,
            include_schema: true,
            include_masked: true
          },
          resolve_sid()
        );

        const result = is_plain_object(out.result) ? out.result : {};
        if (!is_plain_object(result.settings)) return {};
        return deep_clone(result.settings);
      },

      async updateSettings(skill_id: string, settings: Record<string, unknown>): Promise<Record<string, unknown>> {
        await call_xcmd<{ ok?: unknown }>(
          "settings",
          "set_skill",
          {
            skill_id,
            patch: settings
          },
          resolve_sid()
        );

        const refreshed = await call_xcmd<{ result?: unknown }>(
          "settings",
          "get_skill",
          {
            skill_id,
            include_schema: true,
            include_masked: true
          },
          resolve_sid()
        );

        const result = is_plain_object(refreshed.result) ? refreshed.result : {};
        if (!is_plain_object(result.settings)) return {};
        return deep_clone(result.settings);
      },

      async getRuntimeStatus(skill_id: string): Promise<ACPSkillRuntimeStatus> {
        if (skill_id !== "@xpell/agent-skill-telegram") {
          return { available: false, running: false };
        }

        try {
          const out = await call_xcmd<{ running?: unknown; mode?: unknown }>(
            "telegram",
            "status",
            {},
            resolve_sid()
          );

          return {
            available: true,
            running: out.running === true,
            mode: as_text(out.mode) || "polling"
          };
        } catch (err) {
          if (err instanceof AgentApiError) {
            return {
              available: false,
              running: false,
              error: err.message
            };
          }
          return {
            available: false,
            running: false,
            error: "status_unavailable"
          };
        }
      },

      async startPolling(skill_id: string): Promise<ACPSkillRuntimeStatus> {
        if (skill_id !== "@xpell/agent-skill-telegram") {
          return { available: false, running: false };
        }

        // Ensure connector runtime config is refreshed from kernel SettingsModule
        // before starting polling after process restart or skill reload.
        try {
          await call_xcmd("telegram", "reload_settings", {}, resolve_sid());
        } catch {
          // Best-effort compatibility: older runtimes may not expose reload_settings.
        }

        await call_xcmd("telegram", "start", { mode: "polling" }, resolve_sid());
        const out = await call_xcmd<{ running?: unknown; mode?: unknown }>(
          "telegram",
          "status",
          {},
          resolve_sid()
        );
        return {
          available: true,
          running: out.running === true,
          mode: as_text(out.mode) || "polling"
        };
      },

      async stopPolling(skill_id: string): Promise<ACPSkillRuntimeStatus> {
        if (skill_id !== "@xpell/agent-skill-telegram") {
          return { available: false, running: false };
        }

        await call_xcmd("telegram", "stop", {}, resolve_sid());
        const out = await call_xcmd<{ running?: unknown; mode?: unknown }>(
          "telegram",
          "status",
          {},
          resolve_sid()
        );
        return {
          available: true,
          running: out.running === true,
          mode: as_text(out.mode) || "polling"
        };
      },

      async verifyToken(skill_id: string, bot_token?: string): Promise<ACPTelegramTokenVerify> {
        if (skill_id !== "@xpell/agent-skill-telegram") {
          return {
            valid: false,
            source: "configured",
            error: "Token verification is available only for Telegram skill."
          };
        }

        const source: "input" | "configured" = bot_token && bot_token.trim().length > 0 ? "input" : "configured";
        const out = await call_xcmd<{
          valid?: unknown;
          source?: unknown;
          error?: unknown;
          bot?: unknown;
        }>(
          "telegram",
          "verify_token",
          {
            ...(source === "input" ? { bot_token: bot_token!.trim() } : {})
          },
          resolve_sid()
        );

        const bot_obj = is_plain_object(out.bot) ? out.bot : undefined;
        return {
          valid: out.valid === true,
          source: as_text(out.source) === "input" ? "input" : "configured",
          ...(as_text(out.error) ? { error: as_text(out.error) } : {}),
          ...(bot_obj
            ? {
                bot: {
                  id: as_text(bot_obj.id) || String(bot_obj.id ?? ""),
                  ...(as_text(bot_obj.username) ? { username: as_text(bot_obj.username) } : {}),
                  ...(as_text(bot_obj.first_name) ? { first_name: as_text(bot_obj.first_name) } : {})
                }
              }
            : {})
        };
      },

      async importEnv(skill_id: string): Promise<ACPSkillImportEnvResult> {
        if (skill_id !== AZURE_SKILL_ID) {
          return {
            imported_count: 0,
            imported_keys: [],
            detail: "env import is available only for Azure skill"
          };
        }

        const out = await call_xcmd<{
          imported_count?: unknown;
          imported_keys?: unknown;
          detail?: unknown;
        }>("azure", "import_env", {}, resolve_sid());

        const imported_count = Number(out.imported_count);
        const imported_keys = Array.isArray(out.imported_keys)
          ? out.imported_keys.map((value) => as_text(value)).filter((value) => value.length > 0)
          : [];
        const detail = as_text(out.detail);

        return {
          imported_count: Number.isFinite(imported_count) ? Math.max(0, Math.floor(imported_count)) : 0,
          imported_keys,
          ...(detail ? { detail } : {})
        };
      },

      async testAzureConnection(skill_id: string): Promise<ACPAzureConnectionTestResult> {
        if (skill_id !== AZURE_SKILL_ID) {
          return {
            openai: {
              ok: false,
              detail: "skipped: available only for Azure skill"
            },
            speech: {
              ok: false,
              detail: "skipped: available only for Azure skill"
            },
            _ts: Date.now()
          };
        }

        const out = await call_xcmd<{
          openai?: unknown;
          speech?: unknown;
          _ts?: unknown;
        }>("azure", "test_connection", {}, resolve_sid());

        const openai_obj = is_plain_object(out.openai) ? out.openai : {};
        const speech_obj = is_plain_object(out.speech) ? out.speech : {};
        const ts = Number(out._ts);

        return {
          openai: {
            ok: openai_obj.ok === true,
            ...(as_text(openai_obj.detail) ? { detail: as_text(openai_obj.detail) } : {})
          },
          speech: {
            ok: speech_obj.ok === true,
            ...(as_text(speech_obj.detail) ? { detail: as_text(speech_obj.detail) } : {})
          },
          _ts: Number.isFinite(ts) ? Math.floor(ts) : Date.now()
        };
      }
    },

    intents: {
      async list(): Promise<ACPIntentRecord[]> {
        const out = await call_xcmd<{ items?: unknown }>("intent", "list_all", {}, resolve_sid());
        const items = Array.isArray(out.items) ? out.items : [];
        return items.map((entry) => to_intent_record(entry)).filter((entry): entry is ACPIntentRecord => entry !== null);
      },

      async setEnabled(intent_id: string, enabled: boolean): Promise<void> {
        await call_xcmd("intent", "set_enabled", { intent_id, enabled }, resolve_sid());
      },

      async updateConfig(input: ACPIntentUpdateInput): Promise<void> {
        await call_xcmd(
          "intent",
          "update_config",
          {
            intent_id: input.intent_id,
            ...(typeof input.priority === "number" ? { priority: input.priority } : {}),
            ...(input.roles_allowed ? { roles_allowed: input.roles_allowed } : {}),
            ...(input.channels_allowed ? { channels_allowed: input.channels_allowed } : {}),
            ...(input.synonyms ? { synonyms: input.synonyms } : {}),
            ...(input.examples ? { examples: input.examples } : {}),
            ...(typeof input.default_params_json === "string" ? { default_params_json: input.default_params_json } : {})
          },
          resolve_sid()
        );
      }
    },

    system: {
      async getAbout(): Promise<ACPAbout> {
        const hello_url = `${server_url}/wh/v2/hello`;

        let connected = false;
        let xpell_version = "unknown";
        let agent_version = "unknown";

        try {
          const hello_res = await fetch(hello_url, { method: "GET", cache: "no-store" });
          if (hello_res.ok) {
            const hello_payload = await hello_res.json();
            if (is_plain_object(hello_payload) && is_plain_object(hello_payload._payload)) {
              xpell_version = as_text(hello_payload._payload._xpell) || xpell_version;
            }
            connected = true;
          }
        } catch {
          connected = false;
        }

        if (connected) {
          try {
            const status = await call_xcmd<{ version?: unknown }>("agent", "status", {});
            agent_version = as_text(status.version) || agent_version;
          } catch {
            connected = false;
          }
        }

        return {
          agent_version,
          xpell_version,
          connected,
          server_url
        };
      },

      getServerUrl(): string {
        return server_url;
      },

      setServerUrl(next_url: string): string {
        server_url = normalize_server_url(next_url);
        persist_server_url(server_url);
        return server_url;
      },

      async resetDb(): Promise<ACPResetDbResult> {
        const out = await call_xcmd<{
          ok?: unknown;
          threads_deleted?: unknown;
          messages_deleted?: unknown;
          users_deleted?: unknown;
          sessions_deleted?: unknown;
        }>("agent", "reset_db", {}, resolve_sid());

        const read_count = (value: unknown): number => {
          const parsed = Number.parseInt(String(value ?? ""), 10);
          if (!Number.isFinite(parsed) || parsed <= 0) return 0;
          return parsed;
        };

        return {
          ok: out.ok !== false,
          threads_deleted: read_count(out.threads_deleted),
          messages_deleted: read_count(out.messages_deleted),
          users_deleted: read_count(out.users_deleted),
          sessions_deleted: read_count(out.sessions_deleted)
        };
      }
    },

    agent: {
      async getProfile(): Promise<ACPAgentProfile> {
        const out = await call_xcmd<unknown>("agent", "get_profile", {}, resolve_sid());
        return to_agent_profile(out);
      },

      async setProfile(patch: ACPAgentProfilePatch): Promise<ACPAgentProfile> {
        const patch_obj = is_plain_object(patch) ? patch : {};
        const out = await call_xcmd<unknown>(
          "agent",
          "set_profile",
          {
            ...(Object.prototype.hasOwnProperty.call(patch_obj, "name")
              ? { name: as_text(patch_obj.name) }
              : {}),
            ...(Object.prototype.hasOwnProperty.call(patch_obj, "role")
              ? { role: as_text(patch_obj.role) }
              : {}),
            ...(Object.prototype.hasOwnProperty.call(patch_obj, "system_prompt")
              ? { system_prompt: typeof patch_obj.system_prompt === "string" ? patch_obj.system_prompt.trim() : "" }
              : {}),
            ...(Object.prototype.hasOwnProperty.call(patch_obj, "language_policy")
              ? { language_policy: normalize_language_policy(patch_obj.language_policy) }
              : {})
          },
          resolve_sid()
        );
        return to_agent_profile(out);
      }
    },

    async conversations_list(limit = 100): Promise<ACPConversationSummary[]> {
      const safe_limit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 100;
      const out = await call_xcmd<{ threads?: unknown }>("conv", "list_threads", { limit: safe_limit }, resolve_sid());
      return Array.isArray(out.threads)
        ? out.threads.map((entry) => to_conversation_summary(entry)).filter((entry): entry is ACPConversationSummary => entry !== null)
        : [];
    },

    async conversations_get_thread(thread_id: string): Promise<ACPConversationSummary | null> {
      const safe_thread_id = as_text(thread_id);
      if (!safe_thread_id) return null;
      const out = await call_xcmd<{ thread?: unknown }>("conv", "get_thread", { thread_id: safe_thread_id }, resolve_sid());
      return to_conversation_summary(out.thread);
    },

    async conversations_list_messages(input: {
      thread_id: string;
      limit?: number;
      before_ts?: number;
    }): Promise<ACPConversationMessage[]> {
      const safe_thread_id = as_text(input.thread_id);
      if (!safe_thread_id) return [];
      const params: Record<string, unknown> = { thread_id: safe_thread_id };
      if (typeof input.limit === "number" && Number.isFinite(input.limit) && input.limit > 0) {
        params.limit = Math.floor(input.limit);
      }
      if (typeof input.before_ts === "number" && Number.isFinite(input.before_ts) && input.before_ts > 0) {
        params.before_ts = Math.floor(input.before_ts);
      }
      const out = await call_xcmd<{ messages?: unknown }>("conv", "list_messages", params, resolve_sid());
      return Array.isArray(out.messages)
        ? out.messages
            .map((entry) => to_conversation_message(entry))
            .filter((entry): entry is ACPConversationMessage => entry !== null)
        : [];
    },

    async qagent_run_quick(max_cases = 8): Promise<ACPQAgentLastRun> {
      const safe_max_cases = Number.isFinite(max_cases) && max_cases > 0 ? Math.floor(max_cases) : 8;
      const out = await call_xcmd<Record<string, unknown>>(
        "qagent",
        "run_quick",
        { _max_cases: safe_max_cases },
        resolve_sid()
      );
      return to_qagent_last_run(out);
    },

    async qagent_get_last_run(): Promise<ACPQAgentLastRun> {
      const out = await call_xcmd<Record<string, unknown>>("qagent", "get_last_run", {}, resolve_sid());
      return to_qagent_last_run(out);
    },

    async qagent_list_runs(limit = 10): Promise<ACPQAgentRun[]> {
      const safe_limit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 10;
      const out = await call_xcmd<{ items?: unknown }>("qagent", "list_runs", { _limit: safe_limit }, resolve_sid());
      return Array.isArray(out.items)
        ? out.items.map((entry) => to_qagent_run(entry)).filter((entry): entry is ACPQAgentRun => entry !== null)
        : [];
    },

    async qagent_get_run(run_id: string): Promise<{ run: ACPQAgentRun | null; cases: ACPQAgentCase[] }> {
      const safe_run_id = as_text(run_id);
      if (!safe_run_id) {
        return {
          run: null,
          cases: []
        };
      }
      const out = await call_xcmd<Record<string, unknown>>("qagent", "get_run", { run_id: safe_run_id }, resolve_sid());
      const cases_source = Array.isArray(out.cases) ? out.cases : [];
      return {
        run: to_qagent_run(out.run, out.totals),
        cases: cases_source.map((entry) => to_qagent_case(entry)).filter((entry): entry is ACPQAgentCase => entry !== null)
      };
    },

    async users_list(input: ACPUsersListInput = {}): Promise<ACPUsersListResult> {
      const params: Record<string, unknown> = {};
      if (as_text(input.q)) params._q = as_text(input.q);
      if (typeof input.limit === "number" && Number.isFinite(input.limit) && input.limit > 0) {
        params._limit = Math.floor(input.limit);
      }
      if (as_text(input.cursor)) params._cursor = as_text(input.cursor);

      const out = await call_xcmd<{ items?: unknown; next_cursor?: unknown }>("users", "list", params, resolve_sid());
      const items = Array.isArray(out.items)
        ? out.items.map((entry) => to_user_summary(entry)).filter((entry): entry is ACPUserSummary => entry !== null)
        : [];
      const next_cursor = as_text(out.next_cursor);

      return {
        items,
        ...(next_cursor ? { next_cursor } : {})
      };
    }
  };
}

export function create_agent_api(opts: CreateAgentApiOptions = {}): AgentApi {
  const mode = opts.mode ?? "wormholes";
  const runtime_dev_mode =
    typeof opts.dev_mode === "boolean"
      ? opts.dev_mode
      : Boolean((import.meta as { env?: { DEV?: boolean } }).env?.DEV);

  if (mode === "wormholes") {
    return create_wormholes_api(opts);
  }

  return create_mock_agent_api({ ...opts, mode: "mock", dev_mode: runtime_dev_mode });
}
