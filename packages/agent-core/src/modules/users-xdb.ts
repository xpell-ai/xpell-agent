import { XDB, XDBEntity, _xem, _xlog, type XResponseData } from "@xpell/node";

import {
  USER_ROLE_ADMIN,
  USER_ROLE_CUSTOMER,
  USER_ROLE_OWNER,
  type BotIdentity,
  type BotUserRole
} from "../types/users.js";

const ENTITY_USERS_PREFIX = "agent.users";
const ENTITY_SESSIONS_PREFIX = "agent.user_sessions";

type Dict = Record<string, unknown>;

export type AgentUsersXdbScope = {
  _app_id: string;
  _env: string;
};

export type PersistedUserRecord = {
  _id: string;
  _app_id: string;
  _env: string;
  _user_id: string;
  _role: BotUserRole;
  _display_name: string;
  _identities: BotIdentity[];
  _created_at: number;
  _updated_at: number;
  _username?: string;
  _password_digest?: string;
};

export type PersistedSessionRecord = {
  _id: string;
  _app_id: string;
  _env: string;
  _token: string;
  _user_id: string;
  _created_at: number;
  _updated_at: number;
};

type UsersXdb = {
  _users: XDBEntity;
  _sessions: XDBEntity;
};

const _xdb_by_scope = new Map<string, UsersXdb>();
const _xdb_creation_by_scope = new Map<string, Promise<UsersXdb>>();

let _xdb_ready: Promise<void> | null = null;
let _xdb_ready_resolved = false;
let _resolve_xdb_ready: (() => void) | null = null;

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function has_function(value: unknown): boolean {
  if (typeof value === "function") return true;
  if (Array.isArray(value)) return value.some(has_function);
  if (is_plain_object(value)) return Object.values(value).some(has_function);
  return false;
}

function to_text(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function to_ts(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const parsed = Date.parse(String(value ?? ""));
  if (Number.isNaN(parsed)) return Date.now();
  return parsed;
}

function normalize_role(value: unknown): BotUserRole {
  const role = to_text(value).toLowerCase();
  if (role === USER_ROLE_OWNER || role === USER_ROLE_ADMIN || role === USER_ROLE_CUSTOMER) return role;
  return USER_ROLE_CUSTOMER;
}

function normalize_identity(value: unknown): BotIdentity | undefined {
  if (!is_plain_object(value)) return undefined;

  const identity_id = to_text(value.identity_id);
  const channel = to_text(value.channel).toLowerCase();
  const channel_user_id = to_text(value.channel_user_id);

  if (!identity_id || !channel || !channel_user_id) return undefined;

  return {
    identity_id,
    channel,
    channel_user_id,
    ...(to_text(value.display_name) ? { display_name: to_text(value.display_name) } : {}),
    created_at: to_ts(value.created_at),
    updated_at: to_ts(value.updated_at)
  };
}

function normalize_identities(value: unknown): BotIdentity[] {
  if (!Array.isArray(value)) return [];
  const out: BotIdentity[] = [];
  for (const item of value) {
    const normalized = normalize_identity(item);
    if (normalized) out.push(normalized);
  }
  return out;
}

function normalize_user_record(scope: AgentUsersXdbScope, value: unknown): PersistedUserRecord | undefined {
  if (!is_plain_object(value)) return undefined;

  const user_id = to_text(value._user_id || value._id);
  const role = normalize_role(value._role);
  const display_name = to_text(value._display_name);
  if (!user_id || !display_name) return undefined;

  return {
    _id: user_id,
    _app_id: scope._app_id,
    _env: scope._env,
    _user_id: user_id,
    _role: role,
    _display_name: display_name,
    _identities: normalize_identities(value._identities),
    _created_at: to_ts(value._created_at),
    _updated_at: to_ts(value._updated_at),
    ...(to_text(value._username) ? { _username: to_text(value._username).toLowerCase() } : {}),
    ...(to_text(value._password_digest) ? { _password_digest: to_text(value._password_digest) } : {})
  };
}

function normalize_session_record(scope: AgentUsersXdbScope, value: unknown): PersistedSessionRecord | undefined {
  if (!is_plain_object(value)) return undefined;

  const token = to_text(value._token || value._id);
  const user_id = to_text(value._user_id);
  if (!token || !user_id) return undefined;

  return {
    _id: token,
    _app_id: scope._app_id,
    _env: scope._env,
    _token: token,
    _user_id: user_id,
    _created_at: to_ts(value._created_at),
    _updated_at: to_ts(value._updated_at)
  };
}

function ensure_xdb_result<T>(res: XResponseData, label: string): T {
  if (res?._ok !== true) {
    const err = (res as any)?._error ?? (res as any)?._result ?? "unknown";
    let message = "";
    if (typeof err === "string") message = err;
    else if (is_plain_object(err)) {
      message = to_text(err._message) || to_text(err.message);
      if (!message) {
        try {
          message = JSON.stringify(err);
        } catch {
          message = String(err);
        }
      }
    } else {
      message = String(err);
    }
    throw new Error(`[agent-users:xdb] ${label} failed: ${message}`);
  }
  return res._result as T;
}

function scope_key(scope: AgentUsersXdbScope): string {
  return `${scope._app_id}::${scope._env}`;
}

function users_entity_name(scope: AgentUsersXdbScope): string {
  return `${ENTITY_USERS_PREFIX}::${scope_key(scope)}`;
}

function sessions_entity_name(scope: AgentUsersXdbScope): string {
  return `${ENTITY_SESSIONS_PREFIX}::${scope_key(scope)}`;
}

async function ensure_entity_loaded(entity: any): Promise<void> {
  if (!entity) return;
  if (entity._loaded === true) return;
  if (typeof entity.ensureLoaded === "function") return entity.ensureLoaded();
  if (typeof entity.load === "function") return entity.load();
  if (typeof entity.init === "function") return entity.init();
  if (typeof entity.loadData === "function") return entity.loadData();
}

function ensure_xdb_ready(): Promise<void> {
  if (_xdb_ready) return _xdb_ready;

  _xdb_ready = new Promise<void>((resolve) => {
    _resolve_xdb_ready = resolve;
  });

  if (typeof (XDB as any)?.ready === "boolean" && XDB.ready) {
    _xdb_ready_resolved = true;
    _resolve_xdb_ready?.();
  }

  _xem.on(
    "xdb-ready",
    () => {
      if (_xdb_ready_resolved) return;
      _xdb_ready_resolved = true;
      _resolve_xdb_ready?.();
    },
    { _once: true }
  );

  return _xdb_ready;
}

async function ensure_users_xdb(scope: AgentUsersXdbScope): Promise<UsersXdb> {
  const key = scope_key(scope);
  const existing = _xdb_by_scope.get(key);
  if (existing) return existing;

  const in_flight = _xdb_creation_by_scope.get(key);
  if (in_flight) return in_flight;

  const creation = (async () => {
    await ensure_xdb_ready();

    const users = XDB.create({
      _type: "xdb-entity",
      _name: users_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _user_id: { _type: "String", _required: true, _index: true },
        _role: { _type: "String", _required: true, _index: true },
        _display_name: { _type: "String", _required: true },
        _identities: { _type: "Array" },
        _created_at: { _type: "Number", _required: true, _index: true },
        _updated_at: { _type: "Number", _required: true, _index: true },
        _username: { _type: "String", _index: true },
        _password_digest: { _type: "String" }
      }
    }) as XDBEntity;

    const sessions = XDB.create({
      _type: "xdb-entity",
      _name: sessions_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _token: { _type: "String", _required: true, _index: true },
        _user_id: { _type: "String", _required: true, _index: true },
        _created_at: { _type: "Number", _required: true, _index: true },
        _updated_at: { _type: "Number", _required: true, _index: true }
      }
    }) as XDBEntity;

    await Promise.all([ensure_entity_loaded(users), ensure_entity_loaded(sessions)]);

    const out = { _users: users, _sessions: sessions };
    _xdb_by_scope.set(key, out);
    _xlog.log(`[agent-users:xdb] entities initialized scope=${key}`);
    return out;
  })();

  _xdb_creation_by_scope.set(key, creation);
  return creation;
}

export async function init_users_xdb(scope: AgentUsersXdbScope): Promise<void> {
  await ensure_users_xdb(scope);
}

export async function list_users_xdb(scope: AgentUsersXdbScope): Promise<PersistedUserRecord[]> {
  const xdb = await ensure_users_xdb(scope);
  const res = xdb._users.find({ _app_id: scope._app_id, _env: scope._env }, 0, 100000);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(res, "list_users")._data;

  const out: PersistedUserRecord[] = [];
  for (const row of rows) {
    const normalized = normalize_user_record(scope, row);
    if (normalized) out.push(normalized);
  }
  return out;
}

export async function list_user_ids_xdb(scope: AgentUsersXdbScope): Promise<string[]> {
  const users = await list_users_xdb(scope);
  return users.map((user) => user._user_id);
}

export async function upsert_user_xdb(scope: AgentUsersXdbScope, record: PersistedUserRecord): Promise<void> {
  if (has_function(record)) throw new Error("Persisted user record must be JSON-safe");

  const xdb = await ensure_users_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _user_id: record._user_id };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._users.find(filter, 0, 1), "find_user")._data;

  const row = {
    _id: record._user_id,
    _app_id: scope._app_id,
    _env: scope._env,
    _user_id: record._user_id,
    _role: record._role,
    _display_name: record._display_name,
    _identities: record._identities.map((identity) => ({ ...identity })),
    _created_at: record._created_at,
    _updated_at: record._updated_at,
    _username: record._username ?? "",
    _password_digest: record._password_digest ?? ""
  };

  if (existing.length > 0) {
    await xdb._users.update(filter, row, true);
    return;
  }

  await xdb._users.add(row, true, true);
}

export async function delete_user_xdb(scope: AgentUsersXdbScope, user_id: string): Promise<void> {
  const xdb = await ensure_users_xdb(scope);
  await xdb._users.delete({ _app_id: scope._app_id, _env: scope._env, _user_id: user_id }, true);
}

export async function list_sessions_xdb(scope: AgentUsersXdbScope): Promise<PersistedSessionRecord[]> {
  const xdb = await ensure_users_xdb(scope);
  const res = xdb._sessions.find({ _app_id: scope._app_id, _env: scope._env }, 0, 100000);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(res, "list_sessions")._data;

  const out: PersistedSessionRecord[] = [];
  for (const row of rows) {
    const normalized = normalize_session_record(scope, row);
    if (normalized) out.push(normalized);
  }
  return out;
}

export async function list_session_tokens_xdb(scope: AgentUsersXdbScope): Promise<string[]> {
  const sessions = await list_sessions_xdb(scope);
  return sessions.map((session) => session._token);
}

export async function upsert_session_xdb(scope: AgentUsersXdbScope, record: PersistedSessionRecord): Promise<void> {
  if (has_function(record)) throw new Error("Persisted session record must be JSON-safe");

  const xdb = await ensure_users_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _token: record._token };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._sessions.find(filter, 0, 1), "find_session")._data;

  const row = {
    _id: record._token,
    _app_id: scope._app_id,
    _env: scope._env,
    _token: record._token,
    _user_id: record._user_id,
    _created_at: record._created_at,
    _updated_at: record._updated_at
  };

  if (existing.length > 0) {
    await xdb._sessions.update(filter, row, true);
    return;
  }

  await xdb._sessions.add(row, true, true);
}

export async function delete_session_xdb(scope: AgentUsersXdbScope, token: string): Promise<void> {
  const xdb = await ensure_users_xdb(scope);
  await xdb._sessions.delete({ _app_id: scope._app_id, _env: scope._env, _token: token }, true);
}
