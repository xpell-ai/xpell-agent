import { randomBytes, randomUUID, scryptSync, timingSafeEqual } from "node:crypto";

import { XError, XModule, _x, _xlog, type XCommandData } from "@xpell/node";

import { readCommandCtx, requireKernelCap, requireKernelCapOrActorRole } from "../runtime/guards.js";
import { CONVERSATIONS_MODULE_NAME } from "./ConversationsModule.js";
import { SETTINGS_MODULE_NAME } from "./SettingsModule.js";
import {
  delete_session_xdb,
  delete_user_xdb,
  init_users_xdb,
  list_session_tokens_xdb,
  list_sessions_xdb,
  list_user_ids_xdb,
  list_users_xdb,
  upsert_session_xdb,
  upsert_user_xdb,
  type AgentUsersXdbScope,
  type PersistedSessionRecord,
  type PersistedUserRecord
} from "./users-xdb.js";
import {
  USER_ROLE_ADMIN,
  USER_ROLE_CUSTOMER,
  USER_ROLE_OWNER,
  type BotIdentity,
  type BotUser,
  type BotUserRole
} from "../types/users.js";

export const USERS_MODULE_NAME = "users";
const USERS_DEFAULT_LIST_LIMIT = 50;
const USERS_MAX_LIST_LIMIT = 500;
const BOOTSTRAP_ADMIN_CHANNEL = "telegram";
const USERS_ID_MIGRATION_FLAG = "migrations.users_id_v1_done";

type Dict = Record<string, unknown>;

type AdminCredentialRecord = {
  user_id: string;
  username: string;
  password_digest: string;
  created_at: number;
  updated_at: number;
};

type SessionRecord = {
  token: string;
  user_id: string;
  created_at: number;
  updated_at: number;
};

type AdminView = {
  id: string;
  display_id?: string;
  name: string;
  username: string;
  role: "admin" | "owner";
};

type UserSummary = {
  _id: string;
  _display_id?: string;
  _display_name: string;
  _role: BotUserRole;
  _channels: string[];
  _identities: BotIdentity[];
  _created_at: number;
  _updated_at: number;
};

type UsersModuleOptions = {
  _app_id?: string;
  _env?: string;
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

function ensure_non_empty_string(value: unknown, field_name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new XError("E_USERS_BAD_PARAMS", `Invalid ${field_name}: expected non-empty string`);
  }
  return value.trim();
}

function ensure_optional_string(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalize_role(value: unknown): BotUserRole {
  const raw = typeof value === "string" ? value.trim().toLowerCase() : "";
  if (raw === USER_ROLE_OWNER || raw === USER_ROLE_ADMIN || raw === USER_ROLE_CUSTOMER) return raw;
  throw new XError("E_USERS_BAD_PARAMS", `Invalid role: ${String(value)}`);
}

function normalize_channel(value: unknown): string {
  return ensure_non_empty_string(value, "channel").toLowerCase();
}

function normalize_channel_user_id(value: unknown): string {
  return ensure_non_empty_string(value, "channel_user_id");
}

function normalize_username(value: unknown): string {
  return ensure_non_empty_string(value, "username").toLowerCase();
}

function ensure_password(value: unknown): string {
  if (typeof value !== "string" || value.length < 4) {
    throw new XError("E_USERS_BAD_PARAMS", "Invalid password: expected string length >= 4");
  }
  return value;
}

function clone_identity(identity: BotIdentity): BotIdentity {
  return {
    identity_id: identity.identity_id,
    channel: identity.channel,
    channel_user_id: identity.channel_user_id,
    ...(identity.display_name ? { display_name: identity.display_name } : {}),
    created_at: identity.created_at,
    updated_at: identity.updated_at
  };
}

function clone_user(user: BotUser): BotUser {
  return {
    user_id: user.user_id,
    ...(user.display_id ? { display_id: user.display_id } : {}),
    role: user.role,
    display_name: user.display_name,
    created_at: user.created_at,
    updated_at: user.updated_at,
    identities: user.identities.map(clone_identity)
  };
}

function identity_key(channel: string, channel_user_id: string): string {
  return `${channel}::${channel_user_id}`;
}

function create_password_digest(password: string): string {
  const salt = randomBytes(16).toString("hex");
  const hash = scryptSync(password, salt, 64).toString("hex");
  return `${salt}:${hash}`;
}

function verify_password(password: string, digest: string): boolean {
  const parts = String(digest).split(":");
  if (parts.length !== 2) return false;
  const [salt, expected_hash] = parts;
  if (!salt || !expected_hash) return false;

  const actual_hash = scryptSync(password, salt, 64).toString("hex");
  const expected_buf = Buffer.from(expected_hash, "hex");
  const actual_buf = Buffer.from(actual_hash, "hex");

  if (expected_buf.length !== actual_buf.length) return false;
  return timingSafeEqual(expected_buf, actual_buf);
}

function is_legacy_user_id(value: string | undefined): boolean {
  if (typeof value !== "string") return false;
  return /^user_\d+$/i.test(value.trim());
}

export class UsersModule extends XModule {
  static _name = USERS_MODULE_NAME;

  private _identity_seq = 0;
  private _owner_user_id?: string;

  private _users_by_id = new Map<string, BotUser>();
  private _identity_to_user_id = new Map<string, string>();
  private _identity_by_key = new Map<string, BotIdentity>();

  private _admin_credentials_by_user_id = new Map<string, AdminCredentialRecord>();
  private _admin_user_id_by_username = new Map<string, string>();

  private _sessions_by_token = new Map<string, SessionRecord>();
  private _xdb_scope: AgentUsersXdbScope;
  private _xdb_initialized = false;

  constructor(opts: UsersModuleOptions = {}) {
    super({ _name: USERS_MODULE_NAME });
    this._xdb_scope = {
      _app_id: typeof opts._app_id === "string" && opts._app_id.trim() ? opts._app_id.trim() : "xbot",
      _env: typeof opts._env === "string" && opts._env.trim() ? opts._env.trim() : "default"
    };
  }

  async _bootstrap_owner(xcmd: XCommandData) {
    return this.bootstrap_owner_impl(xcmd);
  }
  async _op_bootstrap_owner(xcmd: XCommandData) {
    return this.bootstrap_owner_impl(xcmd);
  }

  async _add_admin_identity(xcmd: XCommandData) {
    return this.add_admin_identity_impl(xcmd);
  }
  async _op_add_admin_identity(xcmd: XCommandData) {
    return this.add_admin_identity_impl(xcmd);
  }

  async _resolve_identity(xcmd: XCommandData) {
    return this.resolve_identity_impl(xcmd);
  }
  async _op_resolve_identity(xcmd: XCommandData) {
    return this.resolve_identity_impl(xcmd);
  }

  async _upsert_from_channel_identity(xcmd: XCommandData) {
    return this.upsert_from_channel_identity_impl(xcmd);
  }
  async _op_upsert_from_channel_identity(xcmd: XCommandData) {
    return this.upsert_from_channel_identity_impl(xcmd);
  }

  async _list(xcmd: XCommandData) {
    return this.list_impl(xcmd);
  }
  async _op_list(xcmd: XCommandData) {
    return this.list_impl(xcmd);
  }

  async _debug_identities(xcmd: XCommandData) {
    return this.debug_identities_impl(xcmd);
  }
  async _op_debug_identities(xcmd: XCommandData) {
    return this.debug_identities_impl(xcmd);
  }

  async _init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }
  async _op_init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }

  async _reset_storage(xcmd: XCommandData) {
    return this.reset_storage_impl(xcmd);
  }
  async _op_reset_storage(xcmd: XCommandData) {
    return this.reset_storage_impl(xcmd);
  }

  async _login(xcmd: XCommandData) {
    return this.login_impl(xcmd);
  }
  async _op_login(xcmd: XCommandData) {
    return this.login_impl(xcmd);
  }

  async _session(xcmd: XCommandData) {
    return this.session_impl(xcmd);
  }
  async _op_session(xcmd: XCommandData) {
    return this.session_impl(xcmd);
  }

  async _logout(xcmd: XCommandData) {
    return this.logout_impl(xcmd);
  }
  async _op_logout(xcmd: XCommandData) {
    return this.logout_impl(xcmd);
  }

  async _list_admins(xcmd: XCommandData) {
    return this.list_admins_impl(xcmd);
  }
  async _op_list_admins(xcmd: XCommandData) {
    return this.list_admins_impl(xcmd);
  }

  async _create_admin(xcmd: XCommandData) {
    return this.create_admin_impl(xcmd);
  }
  async _op_create_admin(xcmd: XCommandData) {
    return this.create_admin_impl(xcmd);
  }

  async _set_role(xcmd: XCommandData) {
    return this.set_role_impl(xcmd);
  }
  async _op_set_role(xcmd: XCommandData) {
    return this.set_role_impl(xcmd);
  }

  async _update_admin(xcmd: XCommandData) {
    return this.update_admin_impl(xcmd);
  }
  async _op_update_admin(xcmd: XCommandData) {
    return this.update_admin_impl(xcmd);
  }

  async _delete_admin(xcmd: XCommandData) {
    return this.delete_admin_impl(xcmd);
  }
  async _op_delete_admin(xcmd: XCommandData) {
    return this.delete_admin_impl(xcmd);
  }

  resolve_session_actor(session_token: string | undefined):
    | {
        user_id: string;
        role: "owner" | "admin";
        name: string;
      }
    | undefined {
    if (!session_token || session_token.trim().length === 0) return undefined;
    const session = this._sessions_by_token.get(session_token.trim());
    if (!session) return undefined;

    const user = this._users_by_id.get(session.user_id);
    if (!user) return undefined;
    if (user.role !== USER_ROLE_OWNER && user.role !== USER_ROLE_ADMIN) return undefined;

    return {
      user_id: user.user_id,
      role: user.role,
      name: user.display_name
    };
  }

  private async init_on_boot_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    await init_users_xdb(this._xdb_scope);
    await this.hydrate_from_xdb();
    this._xdb_initialized = true;
    await this.run_users_id_migration_if_needed(readCommandCtx(xcmd));
    return {
      ok: true,
      users: this._users_by_id.size,
      sessions: this._sessions_by_token.size
    };
  }

  private async bootstrap_owner_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));

    const params = this.ensure_params(xcmd._params);
    const owner_name = ensure_optional_string(params.owner_name) ?? "Owner";

    let owner = this.get_owner_user();
    let owner_created = false;
    if (!owner) {
      owner = this.create_user(USER_ROLE_OWNER, owner_name);
      this._owner_user_id = owner.user_id;
      owner_created = true;
    }

    const admin_chat_ids = this.read_admin_chat_ids(params.admin_chat_ids);
    const admins = admin_chat_ids.map((chat_id) =>
      this.add_admin_identity_raw({
        channel: BOOTSTRAP_ADMIN_CHANNEL,
        channel_user_id: chat_id
      })
    );

    await this.persist_state_or_revert();

    return {
      owner: clone_user(owner),
      owner_created,
      admin_channel: BOOTSTRAP_ADMIN_CHANNEL,
      admins
    };
  }

  private async add_admin_identity_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));

    const params = this.ensure_params(xcmd._params);
    const channel = normalize_channel(params.channel);
    const channel_user_id = normalize_channel_user_id(params.channel_user_id);
    const display_name = ensure_optional_string(params.display_name);

    const out = this.add_admin_identity_raw({ channel, channel_user_id, display_name });
    await this.persist_state_or_revert();
    return out;
  }

  private async resolve_identity_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const channel = normalize_channel(params.channel);
    const channel_user_id = normalize_channel_user_id(params.channel_user_id);
    const profile = this.ensure_optional_profile(params.profile);

    return this.upsert_channel_identity({
      channel,
      channel_user_id,
      profile
    });
  }

  private async upsert_from_channel_identity_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const channel =
      ensure_optional_string(params.channel_id) ?? ensure_optional_string(params.channel);
    if (!channel) {
      throw new XError("E_USERS_BAD_PARAMS", "Invalid channel_id: expected non-empty string");
    }
    const external_user_id =
      ensure_optional_string(params.external_user_id) ?? ensure_optional_string(params.channel_user_id);
    if (!external_user_id) {
      throw new XError("E_USERS_BAD_PARAMS", "Invalid external_user_id: expected non-empty string");
    }

    const external_username = ensure_optional_string(params.external_username) ?? ensure_optional_string(params.username);
    const display_name = ensure_optional_string(params.display_name) ?? ensure_optional_string(params.name);
    const meta = params.meta === undefined ? undefined : this.ensure_optional_profile(params.meta);
    void meta;

    const profile: Dict = {};
    if (external_username) profile.username = external_username;
    if (display_name) {
      profile.display_name = display_name;
      profile.name = display_name;
    }

    const out = await this.upsert_channel_identity({
      channel: normalize_channel(channel),
      channel_user_id: normalize_channel_user_id(external_user_id),
      ...(Object.keys(profile).length > 0 ? { profile } : {})
    });
    return { user_id: out.user_id, role: out.role };
  }

  private async upsert_channel_identity(input: {
    channel: string;
    channel_user_id: string;
    profile?: Dict;
  }): Promise<{ user_id: string; role: BotUserRole }> {
    const profile = input.profile;

    const key = identity_key(input.channel, input.channel_user_id);
    const existing_user_id = this._identity_to_user_id.get(key);
    if (existing_user_id) {
      const existing_user = this.must_get_user(existing_user_id);
      const preferred_name = this.read_display_name_from_profile(profile);
      let mutated = false;
      if (preferred_name && !existing_user.display_name) {
        existing_user.display_name = preferred_name;
        existing_user.updated_at = this.now();
        mutated = true;
      }
      const identity = this._identity_by_key.get(key);
      if (identity && preferred_name && !identity.display_name) {
        identity.display_name = preferred_name;
        identity.updated_at = this.now();
        mutated = true;
      }
      if (mutated) {
        await this.persist_state_or_revert();
      }
      return { user_id: existing_user.user_id, role: existing_user.role };
    }

    const fallback_name = this.read_display_name_from_profile(profile) ?? `${input.channel}:${input.channel_user_id}`;
    const customer = this.create_user(USER_ROLE_CUSTOMER, fallback_name);
    this.attach_identity({
      user_id: customer.user_id,
      channel: input.channel,
      channel_user_id: input.channel_user_id,
      display_name: this.read_display_name_from_profile(profile)
    });

    await this.persist_state_or_revert();

    return { user_id: customer.user_id, role: customer.role };
  }

  private async list_impl(xcmd: XCommandData) {
    this.assert_admin_or_owner_or_system(readCommandCtx(xcmd));
    const params = this.ensure_params(xcmd._params);
    const filter = this.read_public_filter(params);
    const role = filter._role ?? (params.role === undefined ? undefined : normalize_role(params.role));
    const channel_filter = filter._channel;
    const limit = this.normalize_limit(params._limit ?? params.limit, USERS_DEFAULT_LIST_LIMIT, USERS_MAX_LIST_LIMIT);
    const cursor = this.normalize_cursor(params._cursor ?? params.cursor);
    const q = ensure_optional_string(params._q ?? params.q)?.toLowerCase();

    const users = (await list_users_xdb(this._xdb_scope))
      .filter((user) => (role ? user._role === role : true))
      .filter((user) => (channel_filter ? user._identities.some((identity) => identity.channel === channel_filter) : true))
      .filter((user) => this.matches_persisted_user_query(user, q))
      .sort((left, right) => {
        if (left._created_at !== right._created_at) return left._created_at - right._created_at;
        return left._id.localeCompare(right._id);
      });

    const items = users.slice(cursor, cursor + limit).map((user) => this.to_user_summary_from_persisted(user));
    const next_cursor = cursor + limit < users.length ? String(cursor + limit) : undefined;

    return {
      items,
      ...(next_cursor ? { next_cursor } : {})
    };
  }

  private async debug_identities_impl(xcmd: XCommandData) {
    this.assert_admin_or_owner_or_system(readCommandCtx(xcmd));
    const params = this.ensure_params(xcmd._params);
    const channel = params._channel === undefined ? "telegram" : normalize_channel(params._channel);
    const out = await this.list_impl({
      ...xcmd,
      _params: {
        _filter: { _channel: channel },
        _limit: USERS_MAX_LIST_LIMIT,
        _ctx: is_plain_object(xcmd._params) ? xcmd._params._ctx : undefined
      }
    });

    if (!is_plain_object(out) || !Array.isArray(out.items)) {
      throw new XError("E_USERS_UPSTREAM", "users.list returned invalid payload");
    }

    return out.items.map((entry) => {
      const identities = entry._identities.map((identity) => ({
        channel: ensure_non_empty_string(identity.channel, "identity.channel"),
        channel_user_id: ensure_non_empty_string(identity.channel_user_id, "identity.channel_user_id"),
        identity_id: ensure_non_empty_string(identity.identity_id, "identity.identity_id")
      }));

      return {
        user_id: ensure_non_empty_string(entry._id, "_id"),
        role: normalize_role(entry._role),
        display_name: ensure_non_empty_string(entry._display_name, "_display_name"),
        identities
      };
    });
  }

  private async login_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const username = normalize_username(params.username);
    const password = ensure_password(params.password);

    const user_id = this._admin_user_id_by_username.get(username);
    if (!user_id) {
      throw new XError("E_USERS_AUTH_FAILED", "Invalid username or password");
    }

    const credential = this.must_get_admin_credential(user_id);
    if (!verify_password(password, credential.password_digest)) {
      throw new XError("E_USERS_AUTH_FAILED", "Invalid username or password");
    }

    const user = this.must_get_user(user_id);
    if (user.role !== USER_ROLE_ADMIN && user.role !== USER_ROLE_OWNER) {
      throw new XError("E_USERS_AUTH_FAILED", "User is not allowed to access ACP");
    }

    const session = this.create_session(user.user_id);
    await this.persist_state_or_revert();
    return {
      token: session.token,
      user: this.to_auth_user(user)
    };
  }

  private session_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const params = this.ensure_params(xcmd._params);
    const token =
      ensure_optional_string(params.token) ??
      (typeof ctx._sid === "string" && ctx._sid.trim().length > 0 ? ctx._sid.trim() : undefined);

    if (!token) return { is_authenticated: false, user: null };

    const actor = this.resolve_session_actor(token);
    if (!actor) return { is_authenticated: false, user: null };

    return {
      is_authenticated: true,
      user: {
        user_id: actor.user_id,
        role: actor.role,
        name: actor.name
      }
    };
  }

  private async logout_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const params = this.ensure_params(xcmd._params);
    const token =
      ensure_optional_string(params.token) ??
      (typeof ctx._sid === "string" && ctx._sid.trim().length > 0 ? ctx._sid.trim() : undefined);

    if (token) {
      this._sessions_by_token.delete(token);
      await this.persist_state_or_revert();
    }

    return { ok: true };
  }

  private async reset_storage_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");

    const user_ids = await list_user_ids_xdb(this._xdb_scope);
    const session_tokens = await list_session_tokens_xdb(this._xdb_scope);

    try {
      for (const token of session_tokens) {
        await delete_session_xdb(this._xdb_scope, token);
      }
      for (const user_id of user_ids) {
        await delete_user_xdb(this._xdb_scope, user_id);
      }
    } catch {
      await this.hydrate_from_xdb();
      throw new XError("E_USERS_PERSIST_FAILED", "Failed to reset users storage");
    }

    this.clear_runtime_state();
    return {
      users_deleted: user_ids.length,
      sessions_deleted: session_tokens.length
    };
  }

  private list_admins_impl(xcmd: XCommandData) {
    this.require_admin_access(xcmd, { allow_when_empty: true });

    const admins = this.read_admin_views();
    return { admins };
  }

  private async create_admin_impl(xcmd: XCommandData) {
    this.require_admin_access(xcmd, { allow_when_empty: true });

    const params = this.ensure_params(xcmd._params);
    const name = ensure_non_empty_string(params.name, "name");
    const username = normalize_username(params.username);
    const password =
      params.password === undefined || params.password === null
        ? "admin"
        : ensure_password(params.password);

    if (this._admin_user_id_by_username.has(username)) {
      throw new XError("E_USERS_CONFLICT", `Admin username already exists: ${username}`);
    }

    const user = this.create_user(USER_ROLE_ADMIN, name);
    this.attach_admin_credentials(user.user_id, username, password);
    await this.persist_state_or_revert();

    return {
      admin: this.to_admin_view(user.user_id),
      bootstrap_mode: this._admin_credentials_by_user_id.size === 1
    };
  }

  private async set_role_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    this.assert_admin_or_owner(ctx);

    const params = this.ensure_params(xcmd._params);
    const user_id = this.resolve_user_lookup_id(ensure_non_empty_string(params._user_id, "_user_id"));
    const next_role = normalize_role(params._role);

    if (next_role === USER_ROLE_OWNER && ctx.actor?.role !== USER_ROLE_OWNER) {
      throw new XError("E_AGENT_FORBIDDEN", "Only owner can promote a user to owner");
    }

    const user = this.must_get_user(user_id);
    if (user.role === next_role) {
      return {
        _ok: true,
        _user_id: user.user_id,
        _role: user.role
      };
    }

    user.role = next_role;
    user.updated_at = this.now();
    if (next_role === USER_ROLE_OWNER) {
      this._owner_user_id = user.user_id;
    }

    await this.persist_state_or_revert();

    return {
      _ok: true,
      _user_id: user.user_id,
      _role: user.role
    };
  }

  private async update_admin_impl(xcmd: XCommandData) {
    this.require_admin_access(xcmd, { allow_when_empty: false });

    const params = this.ensure_params(xcmd._params);
    const user_id = this.resolve_user_lookup_id(ensure_non_empty_string(params.id, "id"));

    const user = this.must_get_user(user_id);
    const credential = this.must_get_admin_credential(user_id);

    const next_name = ensure_optional_string(params.name);
    const next_username = params.username === undefined ? undefined : normalize_username(params.username);
    const next_password = params.password === undefined ? undefined : ensure_password(params.password);

    if (!next_name && !next_username && !next_password) {
      throw new XError("E_USERS_BAD_PARAMS", "Provide at least one update field");
    }

    if (next_name) {
      user.display_name = next_name;
      user.updated_at = this.now();
    }

    if (next_username && next_username !== credential.username) {
      if (this._admin_user_id_by_username.has(next_username)) {
        throw new XError("E_USERS_CONFLICT", `Admin username already exists: ${next_username}`);
      }
      this._admin_user_id_by_username.delete(credential.username);
      credential.username = next_username;
      this._admin_user_id_by_username.set(next_username, user_id);
      credential.updated_at = this.now();
    }

    if (next_password) {
      credential.password_digest = create_password_digest(next_password);
      credential.updated_at = this.now();
    }

    await this.persist_state_or_revert();

    return {
      admin: this.to_admin_view(user_id)
    };
  }

  private async delete_admin_impl(xcmd: XCommandData) {
    this.require_admin_access(xcmd, { allow_when_empty: false });

    const params = this.ensure_params(xcmd._params);
    const user_id = this.resolve_user_lookup_id(ensure_non_empty_string(params.id, "id"));

    const credential = this.must_get_admin_credential(user_id);
    const user = this.must_get_user(user_id);

    const admin_count = this._admin_credentials_by_user_id.size;
    if (admin_count <= 1) {
      throw new XError("E_USERS_FORBIDDEN", "Cannot delete the last admin user");
    }

    if (user.role === USER_ROLE_OWNER) {
      throw new XError("E_USERS_FORBIDDEN", "Cannot delete owner user");
    }

    this._admin_user_id_by_username.delete(credential.username);
    this._admin_credentials_by_user_id.delete(user_id);

    for (const [token, session] of this._sessions_by_token.entries()) {
      if (session.user_id === user_id) {
        this._sessions_by_token.delete(token);
      }
    }

    user.role = USER_ROLE_CUSTOMER;
    user.updated_at = this.now();
    await this.persist_state_or_revert();

    return { ok: true };
  }

  private ensure_params(value: unknown): Dict {
    if (value === undefined || value === null) return {};
    if (!is_plain_object(value)) {
      throw new XError("E_USERS_BAD_PARAMS", "Expected params to be an object");
    }
    if (has_function(value)) {
      throw new XError("E_USERS_BAD_PARAMS", "params must be JSON-safe");
    }
    return value;
  }

  private require_admin_access(xcmd: XCommandData, opts: { allow_when_empty: boolean }): void {
    if (opts.allow_when_empty && this._admin_credentials_by_user_id.size === 0) {
      return;
    }
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
  }

  private assert_admin_or_owner(ctx: ReturnType<typeof readCommandCtx>): void {
    const role = typeof ctx.actor?.role === "string" ? ctx.actor.role : "";
    if (role !== USER_ROLE_ADMIN && role !== USER_ROLE_OWNER) {
      throw new XError("E_AGENT_FORBIDDEN", "Admin access required");
    }
  }

  private assert_admin_or_owner_or_system(ctx: ReturnType<typeof readCommandCtx>): void {
    const role = typeof ctx.actor?.role === "string" ? ctx.actor.role : "";
    if (role !== USER_ROLE_ADMIN && role !== USER_ROLE_OWNER && role !== "system") {
      throw new XError("E_AGENT_FORBIDDEN", "Admin access required");
    }
  }

  private read_admin_chat_ids(value: unknown): string[] {
    if (value === undefined || value === null) return [];
    if (!Array.isArray(value)) {
      throw new XError("E_USERS_BAD_PARAMS", "admin_chat_ids must be an array of strings");
    }

    const out: string[] = [];
    const seen = new Set<string>();
    for (const item of value) {
      if (typeof item !== "string" || item.trim().length === 0) continue;
      const normalized = item.trim();
      if (seen.has(normalized)) continue;
      seen.add(normalized);
      out.push(normalized);
    }
    return out;
  }

  private ensure_optional_profile(value: unknown): Dict | undefined {
    if (value === undefined || value === null) return undefined;
    if (!is_plain_object(value)) {
      throw new XError("E_USERS_BAD_PARAMS", "profile must be an object when provided");
    }
    if (has_function(value)) {
      throw new XError("E_USERS_BAD_PARAMS", "profile must be JSON-safe");
    }
    return value;
  }

  private read_display_name_from_profile(profile: Dict | undefined): string | undefined {
    if (!profile) return undefined;
    const display_name = ensure_optional_string(profile.display_name);
    if (display_name) return display_name;
    const name = ensure_optional_string(profile.name);
    if (name) return name;
    return undefined;
  }

  private normalize_limit(value: unknown, fallback: number, max: number): number {
    if (value === undefined || value === null) return fallback;
    const parsed = Number.parseInt(String(value), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new XError("E_USERS_BAD_PARAMS", "limit must be a positive integer");
    }
    return Math.min(parsed, max);
  }

  private normalize_cursor(value: unknown): number {
    if (value === undefined || value === null) return 0;
    const parsed = Number.parseInt(String(value), 10);
    if (!Number.isFinite(parsed) || parsed < 0) {
      throw new XError("E_USERS_BAD_PARAMS", "cursor must be a non-negative integer");
    }
    return parsed;
  }

  private matches_user_query(user: BotUser, query: string | undefined): boolean {
    if (!query) return true;
    const needle = query.trim().toLowerCase();
    if (!needle) return true;

    if (user.user_id.toLowerCase().includes(needle)) return true;
    if (user.display_id && user.display_id.toLowerCase().includes(needle)) return true;
    if (user.display_name.toLowerCase().includes(needle)) return true;
    if (user.role.toLowerCase().includes(needle)) return true;

    for (const identity of user.identities) {
      if (identity.channel.toLowerCase().includes(needle)) return true;
      if (identity.channel_user_id.toLowerCase().includes(needle)) return true;
      if (identity.display_name && identity.display_name.toLowerCase().includes(needle)) return true;
    }

    return false;
  }

  private matches_persisted_user_query(user: PersistedUserRecord, query: string | undefined): boolean {
    if (!query) return true;
    const needle = query.trim().toLowerCase();
    if (!needle) return true;

    if (user._id.toLowerCase().includes(needle)) return true;
    if (user._display_id && user._display_id.toLowerCase().includes(needle)) return true;
    if (user._display_name.toLowerCase().includes(needle)) return true;
    if (user._role.toLowerCase().includes(needle)) return true;

    for (const identity of user._identities) {
      if (identity.channel.toLowerCase().includes(needle)) return true;
      if (identity.channel_user_id.toLowerCase().includes(needle)) return true;
      if (identity.display_name && identity.display_name.toLowerCase().includes(needle)) return true;
    }

    return false;
  }

  private read_public_filter(params: Dict): { _role?: BotUserRole; _channel?: string } {
    const raw = params._filter;
    if (raw === undefined || raw === null) return {};
    if (!is_plain_object(raw) || has_function(raw)) {
      throw new XError("E_USERS_BAD_PARAMS", "_filter must be a JSON-safe object");
    }
    const role = raw._role === undefined ? undefined : normalize_role(raw._role);
    const channel = raw._channel === undefined ? undefined : normalize_channel(raw._channel);
    return {
      ...(role ? { _role: role } : {}),
      ...(channel ? { _channel: channel } : {})
    };
  }

  private add_admin_identity_raw(input: {
    channel: string;
    channel_user_id: string;
    display_name?: string;
  }): {
    channel: string;
    channel_user_id: string;
    user_id: string;
    role: BotUserRole;
    created: boolean;
  } {
    const key = identity_key(input.channel, input.channel_user_id);
    const existing_user_id = this._identity_to_user_id.get(key);
    const now = this.now();

    if (existing_user_id) {
      const user = this.must_get_user(existing_user_id);
      const identity = this.must_get_identity(key);

      if (user.role === USER_ROLE_CUSTOMER) {
        user.role = USER_ROLE_ADMIN;
        user.updated_at = now;
      }
      if (user.role === USER_ROLE_ADMIN && input.display_name && !user.display_name) {
        user.display_name = input.display_name;
        user.updated_at = now;
      }
      if (input.display_name && !identity.display_name) {
        identity.display_name = input.display_name;
        identity.updated_at = now;
      }

      return {
        channel: input.channel,
        channel_user_id: input.channel_user_id,
        user_id: user.user_id,
        role: user.role,
        created: false
      };
    }

    const admin_user = this.create_user(USER_ROLE_ADMIN, input.display_name ?? "Admin");
    this.attach_identity({
      user_id: admin_user.user_id,
      channel: input.channel,
      channel_user_id: input.channel_user_id,
      ...(input.display_name ? { display_name: input.display_name } : {})
    });

    return {
      channel: input.channel,
      channel_user_id: input.channel_user_id,
      user_id: admin_user.user_id,
      role: admin_user.role,
      created: true
    };
  }

  private create_user(role: BotUserRole, display_name: string): BotUser {
    const now = this.now();
    const user_id = randomUUID();

    const user: BotUser = {
      user_id,
      role,
      display_name,
      created_at: now,
      updated_at: now,
      identities: []
    };
    this._users_by_id.set(user_id, user);

    if (role === USER_ROLE_OWNER) this._owner_user_id = user_id;

    return user;
  }

  private attach_identity(input: {
    user_id: string;
    channel: string;
    channel_user_id: string;
    display_name?: string;
  }): BotIdentity {
    const user = this.must_get_user(input.user_id);
    const key = identity_key(input.channel, input.channel_user_id);

    this._identity_seq += 1;
    const now = this.now();
    const identity: BotIdentity = {
      identity_id: `ident_${this._identity_seq.toString().padStart(6, "0")}`,
      channel: input.channel,
      channel_user_id: input.channel_user_id,
      ...(input.display_name ? { display_name: input.display_name } : {}),
      created_at: now,
      updated_at: now
    };

    user.identities.push(identity);
    user.updated_at = now;
    this._identity_to_user_id.set(key, user.user_id);
    this._identity_by_key.set(key, identity);

    return identity;
  }

  private attach_admin_credentials(user_id: string, username: string, password: string): AdminCredentialRecord {
    const now = this.now();
    const credential: AdminCredentialRecord = {
      user_id,
      username,
      password_digest: create_password_digest(password),
      created_at: now,
      updated_at: now
    };

    this._admin_credentials_by_user_id.set(user_id, credential);
    this._admin_user_id_by_username.set(username, user_id);
    return credential;
  }

  private create_session(user_id: string): SessionRecord {
    const token = `sid_${randomBytes(24).toString("hex")}`;
    const now = this.now();
    const session: SessionRecord = {
      token,
      user_id,
      created_at: now,
      updated_at: now
    };

    this._sessions_by_token.set(token, session);
    return session;
  }

  private read_admin_views(): AdminView[] {
    return Array.from(this._admin_credentials_by_user_id.values())
      .map((record) => this.to_admin_view(record.user_id))
      .sort((left, right) => left.username.localeCompare(right.username));
  }

  private to_admin_view(user_id: string): AdminView {
    const user = this.must_get_user(user_id);
    const credential = this.must_get_admin_credential(user_id);

    const role: "admin" | "owner" = user.role === USER_ROLE_OWNER ? "owner" : "admin";

    return {
      id: user.user_id,
      ...(user.display_id ? { display_id: user.display_id } : {}),
      name: user.display_name,
      username: credential.username,
      role
    };
  }

  private to_auth_user(user: BotUser): { user_id: string; role: "admin" | "owner"; name: string } {
    return {
      user_id: user.user_id,
      role: user.role === USER_ROLE_OWNER ? "owner" : "admin",
      name: user.display_name
    };
  }

  private resolve_user_lookup_id(value: string): string {
    if (this._users_by_id.has(value)) return value;
    const by_display = this.find_user_id_by_display_id(value);
    if (by_display) return by_display;
    throw new XError("E_USERS_NOT_FOUND", `User not found: ${value}`);
  }

  private find_user_id_by_display_id(display_id: string): string | undefined {
    const needle = display_id.trim();
    if (!needle) return undefined;
    for (const user of this._users_by_id.values()) {
      if (user.display_id === needle) return user.user_id;
    }
    return undefined;
  }

  private resolve_persisted_session_user_id(value: string): string | undefined {
    if (this._users_by_id.has(value)) return value;
    if (!is_legacy_user_id(value)) return undefined;
    return this.find_user_id_by_display_id(value);
  }

  private to_user_summary(user: BotUser): UserSummary {
    const channels = Array.from(new Set(user.identities.map((identity) => identity.channel))).sort((a, b) =>
      a.localeCompare(b)
    );

    return {
      _id: user.user_id,
      ...(user.display_id ? { _display_id: user.display_id } : {}),
      _display_name: user.display_name,
      _role: user.role,
      _channels: channels,
      _identities: user.identities.map(clone_identity),
      _created_at: user.created_at,
      _updated_at: user.updated_at
    } as UserSummary;
  }

  private to_user_summary_from_persisted(user: PersistedUserRecord): UserSummary {
    const channels = Array.from(new Set(user._identities.map((identity) => identity.channel))).sort((a, b) =>
      a.localeCompare(b)
    );

    return {
      _id: user._id,
      ...(user._display_id ? { _display_id: user._display_id } : {}),
      _display_name: user._display_name,
      _role: user._role,
      _channels: channels,
      _identities: user._identities.map(clone_identity),
      _created_at: user._created_at,
      _updated_at: user._updated_at
    } as UserSummary;
  }

  private read_identity_seq_from_id(identity_id: string): number {
    const matched = /^ident_(\d+)$/.exec(identity_id);
    if (!matched) return 0;
    const parsed = Number.parseInt(matched[1], 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  private clear_runtime_state(): void {
    this._identity_seq = 0;
    this._owner_user_id = undefined;
    this._users_by_id.clear();
    this._identity_to_user_id.clear();
    this._identity_by_key.clear();
    this._admin_credentials_by_user_id.clear();
    this._admin_user_id_by_username.clear();
    this._sessions_by_token.clear();
  }

  private to_persisted_user_record(user: BotUser): PersistedUserRecord {
    const credential = this._admin_credentials_by_user_id.get(user.user_id);
    return {
      _id: user.user_id,
      _app_id: this._xdb_scope._app_id,
      _env: this._xdb_scope._env,
      _role: user.role,
      _display_name: user.display_name,
      _identities: user.identities.map(clone_identity),
      _created_at: user.created_at,
      _updated_at: user.updated_at,
      ...(user.display_id ? { _display_id: user.display_id } : {}),
      ...(credential ? { _username: credential.username } : {}),
      ...(credential ? { _password_digest: credential.password_digest } : {})
    };
  }

  private to_persisted_session_record(session: SessionRecord): PersistedSessionRecord {
    return {
      _id: session.token,
      _app_id: this._xdb_scope._app_id,
      _env: this._xdb_scope._env,
      _token: session.token,
      _user_id: session.user_id,
      _created_at: session.created_at,
      _updated_at: session.updated_at
    };
  }

  private async hydrate_from_xdb(): Promise<void> {
    const persisted_users = await list_users_xdb(this._xdb_scope);
    const persisted_sessions = await list_sessions_xdb(this._xdb_scope);

    this.clear_runtime_state();

    for (const row of persisted_users) {
      const canonical_user_id = row._id;
      const role =
        row._role === USER_ROLE_OWNER || row._role === USER_ROLE_ADMIN || row._role === USER_ROLE_CUSTOMER
          ? row._role
          : USER_ROLE_CUSTOMER;

      const user: BotUser = {
        user_id: canonical_user_id,
        ...(ensure_optional_string(row._display_id)
          ? { display_id: ensure_optional_string(row._display_id) }
          : ensure_optional_string(row._user_id)
            ? { display_id: ensure_optional_string(row._user_id) }
            : {}),
        role,
        display_name: row._display_name,
        created_at: row._created_at,
        updated_at: row._updated_at,
        identities: row._identities.map(clone_identity)
      };

      this._users_by_id.set(user.user_id, user);

      for (const identity of user.identities) {
        const key = identity_key(identity.channel, identity.channel_user_id);
        this._identity_to_user_id.set(key, user.user_id);
        this._identity_by_key.set(key, identity);
        this._identity_seq = Math.max(this._identity_seq, this.read_identity_seq_from_id(identity.identity_id));
      }

      if (role === USER_ROLE_OWNER && !this._owner_user_id) {
        this._owner_user_id = user.user_id;
      }

      const username = ensure_optional_string(row._username);
      const password_digest = ensure_optional_string(row._password_digest);
      if (username && password_digest && (role === USER_ROLE_ADMIN || role === USER_ROLE_OWNER)) {
        const now = this.now();
        const credential: AdminCredentialRecord = {
          user_id: user.user_id,
          username: username.toLowerCase(),
          password_digest,
          created_at: row._created_at || now,
          updated_at: row._updated_at || now
        };
        this._admin_credentials_by_user_id.set(user.user_id, credential);
        this._admin_user_id_by_username.set(credential.username, user.user_id);
      }
    }

    for (const row of persisted_sessions) {
      const session_user_id = this.resolve_persisted_session_user_id(row._user_id);
      if (!session_user_id) continue;
      this._sessions_by_token.set(row._token, {
        token: row._token,
        user_id: session_user_id,
        created_at: row._created_at,
        updated_at: row._updated_at
      });
    }
  }

  private async persist_state(): Promise<void> {
    if (!this._xdb_initialized) return;

    const users = Array.from(this._users_by_id.values()).map((user) => this.to_persisted_user_record(user));
    const sessions = Array.from(this._sessions_by_token.values()).map((session) => this.to_persisted_session_record(session));

    const keep_user_ids = new Set(users.map((user) => user._id));
    const keep_session_tokens = new Set(sessions.map((session) => session._token));

    for (const user of users) {
      await upsert_user_xdb(this._xdb_scope, user);
    }
    const existing_user_ids = await list_user_ids_xdb(this._xdb_scope);
    for (const user_id of existing_user_ids) {
      if (!keep_user_ids.has(user_id)) {
        await delete_user_xdb(this._xdb_scope, user_id);
      }
    }

    for (const session of sessions) {
      await upsert_session_xdb(this._xdb_scope, session);
    }
    const existing_session_tokens = await list_session_tokens_xdb(this._xdb_scope);
    for (const token of existing_session_tokens) {
      if (!keep_session_tokens.has(token)) {
        await delete_session_xdb(this._xdb_scope, token);
      }
    }
  }

  private async persist_state_or_revert(): Promise<void> {
    if (!this._xdb_initialized) return;
    try {
      await this.persist_state();
    } catch (err) {
      _xlog.error("[agent-core] users persistence failed; restoring from xdb", err);
      await this.hydrate_from_xdb();
      throw new XError("E_USERS_PERSIST_FAILED", "Failed to persist users state");
    }
  }

  private async run_users_id_migration_if_needed(ctx: ReturnType<typeof readCommandCtx>): Promise<void> {
    const done = await this.read_migration_flag(ctx);
    if (done) return;
    const legacy_to_canonical: Dict = {};
    for (const user of this._users_by_id.values()) {
      if (!user.display_id || user.display_id === user.user_id) continue;
      legacy_to_canonical[user.display_id] = user.user_id;
    }
    if (Object.keys(legacy_to_canonical).length > 0) {
      await _x.execute({
        _module: CONVERSATIONS_MODULE_NAME,
        _op: "remap_user_ids",
        _params: {
          map: legacy_to_canonical,
          _ctx: this.forward_ctx(ctx)
        }
      });
    }
    await this.persist_state_or_revert();
    await this.write_migration_flag(ctx);
  }

  private async read_migration_flag(ctx: ReturnType<typeof readCommandCtx>): Promise<boolean> {
    const out = await _x.execute({
      _module: SETTINGS_MODULE_NAME,
      _op: "get",
      _params: {
        key: USERS_ID_MIGRATION_FLAG,
        _ctx: this.forward_ctx(ctx)
      }
    });
    return is_plain_object(out) && out.value === true;
  }

  private async write_migration_flag(ctx: ReturnType<typeof readCommandCtx>): Promise<void> {
    await _x.execute({
      _module: SETTINGS_MODULE_NAME,
      _op: "set",
      _params: {
        key: USERS_ID_MIGRATION_FLAG,
        value: true,
        _ctx: this.forward_ctx(ctx)
      }
    });
  }

  private get_owner_user(): BotUser | undefined {
    if (!this._owner_user_id) return undefined;
    return this._users_by_id.get(this._owner_user_id);
  }

  private forward_ctx(ctx: ReturnType<typeof readCommandCtx>): Dict {
    const out: Dict = {};
    if (typeof ctx.kernel_cap === "string" && ctx.kernel_cap.trim().length > 0) {
      out.kernel_cap = ctx.kernel_cap;
    }
    if (ctx.actor && is_plain_object(ctx.actor)) {
      out.actor = ctx.actor;
    }
    return out;
  }

  private must_get_user(user_id: string): BotUser {
    const user = this._users_by_id.get(user_id);
    if (!user) {
      throw new XError("E_USERS_INTERNAL", `User not found: ${user_id}`);
    }
    return user;
  }

  private must_get_admin_credential(user_id: string): AdminCredentialRecord {
    const credential = this._admin_credentials_by_user_id.get(user_id);
    if (!credential) {
      throw new XError("E_USERS_NOT_FOUND", `Admin credential not found for user: ${user_id}`);
    }
    return credential;
  }

  private must_get_identity(key: string): BotIdentity {
    const identity = this._identity_by_key.get(key);
    if (!identity) {
      throw new XError("E_USERS_INTERNAL", `Identity not found: ${key}`);
    }
    return identity;
  }

  private now(): number {
    return Date.now();
  }
}

export default UsersModule;
