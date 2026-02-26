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

export type ACPSkill = {
  id: string;
  version: string;
  enabled: boolean;
  status: ACPSkillStatus;
  error?: string;
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
  system: {
    getAbout(): Promise<ACPAbout>;
    getServerUrl(): string;
    setServerUrl(next_url: string): string;
  };
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
        return skills.map((skill) => deep_clone(skill));
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
      }
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

    return {
      id: skill_id,
      version: as_text(loaded_obj.version) || "unknown",
      enabled: loaded_obj.enabled === true,
      status,
      ...(as_text(loaded_obj.error) ? { error: as_text(loaded_obj.error) } : {})
    };
  };

  return {
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
      }
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
