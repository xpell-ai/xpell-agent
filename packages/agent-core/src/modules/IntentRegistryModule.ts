import { XError, XModule, _x, type XCommandData } from "@xpell/node";

import { readCommandCtx, requireKernelCap, requireKernelCapOrActorRole } from "../runtime/guards.js";
import type { XBotIntent } from "../types/skills.js";
import { SKILL_MANAGER_MODULE_NAME } from "./SkillManagerModule.js";
import {
  get_intent_config_xdb,
  init_intents_xdb,
  list_intent_configs_xdb,
  upsert_intent_config_xdb,
  type AgentIntentXdbScope,
  type PersistedIntentConfigRecord
} from "../xdb/intent-xdb.js";

export const INTENT_REGISTRY_MODULE_NAME = "intent";

type Dict = Record<string, unknown>;

type IntentRegistryModuleOptions = {
  _app_id?: string;
  _env?: string;
};

type DiscoveredIntent = XBotIntent & {
  skill_id: string;
  enabled_by_default?: boolean;
};

type IntentView = DiscoveredIntent & {
  enabled: boolean;
  priority: number;
  roles_allowed: Array<"owner" | "admin" | "customer">;
  channels_allowed?: string[];
  synonyms?: string[];
  examples?: string[];
  default_params_json?: string;
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
    throw new XError("E_INTENT_BAD_PARAMS", `Invalid ${field_name}: expected non-empty string`);
  }
  return value.trim();
}

function ensure_boolean(value: unknown, field_name: string): boolean {
  if (typeof value !== "boolean") {
    throw new XError("E_INTENT_BAD_PARAMS", `Invalid ${field_name}: expected boolean`);
  }
  return value;
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

function normalize_roles(value: unknown, fallback: Array<"owner" | "admin" | "customer">): Array<"owner" | "admin" | "customer"> {
  const out = normalize_string_array(value).filter(
    (entry): entry is "owner" | "admin" | "customer" => entry === "owner" || entry === "admin" || entry === "customer"
  );
  return out.length > 0 ? out : [...fallback];
}

function normalize_priority(value: unknown, fallback: number): number {
  if (value === undefined || value === null) return fallback;
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(parsed)) {
    throw new XError("E_INTENT_BAD_PARAMS", "Invalid priority");
  }
  return Math.floor(parsed);
}

function builtin_intents(): DiscoveredIntent[] {
  return [
    {
      intent_id: "admin.conv.summary_today",
      title: "Summary Today",
      description: "Summarize inbound conversations from today.",
      roles_allowed: ["owner", "admin"],
      channels_allowed: ["telegram"],
      handler: {
        module: "conv",
        op: "summary_today"
      },
      examples: ["summarize today", "summary for today", "today summary"],
      synonyms: ["summarize", "summary", "today"],
      skill_id: "core",
      enabled_by_default: true
    }
  ];
}

export class IntentRegistryModule extends XModule {
  static _name = INTENT_REGISTRY_MODULE_NAME;

  private _scope: AgentIntentXdbScope;
  private _configs_by_id = new Map<string, PersistedIntentConfigRecord>();

  constructor(opts: IntentRegistryModuleOptions = {}) {
    super({ _name: INTENT_REGISTRY_MODULE_NAME });
    this._scope = {
      _app_id: typeof opts._app_id === "string" && opts._app_id.trim() ? opts._app_id.trim() : "xbot",
      _env: typeof opts._env === "string" && opts._env.trim() ? opts._env.trim() : "default"
    };
  }

  async _init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }
  async _op_init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }

  async _list_all(xcmd: XCommandData) {
    return this.list_all_impl(xcmd);
  }
  async _op_list_all(xcmd: XCommandData) {
    return this.list_all_impl(xcmd);
  }

  async _set_enabled(xcmd: XCommandData) {
    return this.set_enabled_impl(xcmd);
  }
  async _op_set_enabled(xcmd: XCommandData) {
    return this.set_enabled_impl(xcmd);
  }

  async _update_config(xcmd: XCommandData) {
    return this.update_config_impl(xcmd);
  }
  async _op_update_config(xcmd: XCommandData) {
    return this.update_config_impl(xcmd);
  }

  async _get_enabled_for_context(xcmd: XCommandData) {
    return this.get_enabled_for_context_impl(xcmd);
  }
  async _op_get_enabled_for_context(xcmd: XCommandData) {
    return this.get_enabled_for_context_impl(xcmd);
  }

  private async init_on_boot_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    await init_intents_xdb(this._scope);
    await this.hydrate_from_xdb();
    await this.seed_discovered_defaults(xcmd);
    return {
      ok: true,
      intents: this._configs_by_id.size
    };
  }

  private async list_all_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    return {
      items: await this.build_merged_items(xcmd)
    };
  }

  private async set_enabled_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    const params = this.ensure_params(xcmd._params);
    const intent_id = ensure_non_empty_string(params.intent_id, "intent_id");
    const enabled = ensure_boolean(params.enabled, "enabled");
    const discovered = await this.must_get_discovered_intent(xcmd, intent_id);
    const next = this.build_next_record(intent_id, discovered, {
      _enabled: enabled
    });
    await this.persist_record(next);
    return {
      ok: true,
      intent_id,
      enabled
    };
  }

  private async update_config_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    const params = this.ensure_params(xcmd._params);
    const intent_id = ensure_non_empty_string(params.intent_id, "intent_id");
    const discovered = await this.must_get_discovered_intent(xcmd, intent_id);
    const patch: Partial<PersistedIntentConfigRecord> = {};

    if (params.priority !== undefined) {
      patch._priority = normalize_priority(params.priority, 100);
    }
    if (params.roles_allowed !== undefined) {
      patch._roles_allowed = normalize_roles(params.roles_allowed, discovered.roles_allowed);
    }
    if (params.channels_allowed !== undefined) {
      patch._channels_allowed = normalize_string_array(params.channels_allowed);
    }
    if (params.synonyms !== undefined) {
      patch._synonyms = normalize_string_array(params.synonyms);
    }
    if (params.examples !== undefined) {
      patch._examples = normalize_string_array(params.examples);
    }
    if (params.default_params !== undefined) {
      if (has_function(params.default_params)) {
        throw new XError("E_INTENT_BAD_PARAMS", "default_params must be JSON-safe");
      }
      patch._default_params_json = JSON.stringify(params.default_params);
    } else if (params.default_params_json !== undefined) {
      if (typeof params.default_params_json !== "string") {
        throw new XError("E_INTENT_BAD_PARAMS", "default_params_json must be a string");
      }
      const trimmed = params.default_params_json.trim();
      if (trimmed) {
        try {
          const parsed = JSON.parse(trimmed);
          if (has_function(parsed)) {
            throw new XError("E_INTENT_BAD_PARAMS", "default_params_json must decode to JSON-safe data");
          }
        } catch (err) {
          if (err instanceof XError) throw err;
          throw new XError("E_INTENT_BAD_PARAMS", "default_params_json must be valid JSON");
        }
        patch._default_params_json = trimmed;
      } else {
        patch._default_params_json = "";
      }
    }

    const next = this.build_next_record(intent_id, discovered, patch);
    await this.persist_record(next);
    return {
      ok: true,
      intent_id
    };
  }

  private async get_enabled_for_context_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    const params = this.ensure_params(xcmd._params);
    const role = ensure_non_empty_string(params.role, "role").toLowerCase();
    const channel = ensure_non_empty_string(params.channel, "channel").toLowerCase();
    if (role !== "owner" && role !== "admin" && role !== "customer") {
      throw new XError("E_INTENT_BAD_PARAMS", "role must be owner, admin, or customer");
    }

    const items = await this.build_merged_items(xcmd);
    return {
      items: items.filter((item) => {
        if (item.enabled !== true) return false;
        if (!item.roles_allowed.includes(role as "owner" | "admin" | "customer")) return false;
        if (item.channels_allowed && item.channels_allowed.length > 0 && !item.channels_allowed.includes(channel)) return false;
        return true;
      })
    };
  }

  private async build_merged_items(xcmd: XCommandData): Promise<IntentView[]> {
    const discovered = await this.load_discovered_intents(xcmd);
    return discovered
      .map((intent) => this.merge_intent_with_config(intent))
      .sort((left, right) => {
        if (left.priority !== right.priority) return left.priority - right.priority;
        return left.intent_id.localeCompare(right.intent_id);
      });
  }

  private merge_intent_with_config(intent: DiscoveredIntent): IntentView {
    const config = this._configs_by_id.get(intent.intent_id);
    const roles_allowed = config ? normalize_roles(config._roles_allowed, intent.roles_allowed) : [...intent.roles_allowed];
    const channels_allowed = config
      ? config._channels_allowed.length > 0
        ? [...config._channels_allowed]
        : intent.channels_allowed
          ? [...intent.channels_allowed]
          : []
      : intent.channels_allowed
        ? [...intent.channels_allowed]
        : [];
    const synonyms = config && config._synonyms.length > 0 ? [...config._synonyms] : intent.synonyms ? [...intent.synonyms] : [];
    const examples = config && config._examples.length > 0 ? [...config._examples] : intent.examples ? [...intent.examples] : [];

    return {
      ...intent,
      enabled: config ? config._enabled === true : intent.enabled_by_default === true,
      priority: config ? config._priority : 100,
      roles_allowed,
      ...(channels_allowed.length > 0 ? { channels_allowed } : {}),
      ...(synonyms.length > 0 ? { synonyms } : {}),
      ...(examples.length > 0 ? { examples } : {}),
      ...(config && config._default_params_json ? { default_params_json: config._default_params_json } : {})
    };
  }

  private build_next_record(
    intent_id: string,
    discovered: DiscoveredIntent,
    patch: Partial<PersistedIntentConfigRecord>
  ): PersistedIntentConfigRecord {
    const existing = this._configs_by_id.get(intent_id) ?? {
      _id: intent_id,
      _app_id: this._scope._app_id,
      _env: this._scope._env,
      _enabled: discovered.enabled_by_default === true,
      _priority: 100,
      _roles_allowed: [...discovered.roles_allowed],
      _channels_allowed: discovered.channels_allowed ? [...discovered.channels_allowed] : [],
      _synonyms: discovered.synonyms ? [...discovered.synonyms] : [],
      _examples: discovered.examples ? [...discovered.examples] : [],
      _created_at: Date.now(),
      _updated_at: Date.now()
    };

    return {
      ...existing,
      ...patch,
      _id: intent_id,
      _app_id: this._scope._app_id,
      _env: this._scope._env,
      _roles_allowed: patch._roles_allowed ? [...patch._roles_allowed] : [...existing._roles_allowed],
      _channels_allowed: patch._channels_allowed ? [...patch._channels_allowed] : [...existing._channels_allowed],
      _synonyms: patch._synonyms ? [...patch._synonyms] : [...existing._synonyms],
      _examples: patch._examples ? [...patch._examples] : [...existing._examples],
      _created_at: existing._created_at,
      _updated_at: Date.now()
    };
  }

  private async persist_record(record: PersistedIntentConfigRecord): Promise<void> {
    await upsert_intent_config_xdb(this._scope, record);
    this._configs_by_id.set(record._id, {
      ...record,
      _roles_allowed: [...record._roles_allowed],
      _channels_allowed: [...record._channels_allowed],
      _synonyms: [...record._synonyms],
      _examples: [...record._examples]
    });
  }

  private async seed_discovered_defaults(xcmd: XCommandData): Promise<void> {
    const intents = await this.load_discovered_intents(xcmd);
    for (const intent of intents) {
      if (this._configs_by_id.has(intent.intent_id)) continue;
      const existing = await get_intent_config_xdb(this._scope, intent.intent_id);
      if (existing) {
        this._configs_by_id.set(existing._id, existing);
        continue;
      }
      const record: PersistedIntentConfigRecord = {
        _id: intent.intent_id,
        _app_id: this._scope._app_id,
        _env: this._scope._env,
        _enabled: intent.enabled_by_default === true,
        _priority: 100,
        _roles_allowed: [...intent.roles_allowed],
        _channels_allowed: intent.channels_allowed ? [...intent.channels_allowed] : [],
        _synonyms: intent.synonyms ? [...intent.synonyms] : [],
        _examples: intent.examples ? [...intent.examples] : [],
        _created_at: Date.now(),
        _updated_at: Date.now()
      };
      await this.persist_record(record);
    }
  }

  private async must_get_discovered_intent(xcmd: XCommandData, intent_id: string): Promise<DiscoveredIntent> {
    const intents = await this.load_discovered_intents(xcmd);
    const found = intents.find((entry) => entry.intent_id === intent_id);
    if (!found) {
      throw new XError("E_INTENT_NOT_FOUND", `Intent not found: ${intent_id}`);
    }
    return found;
  }

  private async load_discovered_intents(xcmd: XCommandData): Promise<DiscoveredIntent[]> {
    const out = await _x.execute({
      _module: SKILL_MANAGER_MODULE_NAME,
      _op: "list_intents",
      _params: {
        _ctx: this.forward_ctx(xcmd)
      }
    });
    const items = is_plain_object(out) && Array.isArray(out.items) ? out.items : [];
    const out_items: DiscoveredIntent[] = [...builtin_intents()];
    const seen = new Set<string>(out_items.map((entry) => entry.intent_id));
    for (const entry of items) {
      if (!is_plain_object(entry)) continue;
      const intent_id = typeof entry.intent_id === "string" ? entry.intent_id.trim() : "";
      const title = typeof entry.title === "string" ? entry.title.trim() : "";
      const skill_id = typeof entry.skill_id === "string" ? entry.skill_id.trim() : "";
      const handler = is_plain_object(entry.handler) ? entry.handler : {};
      const handler_module = typeof handler.module === "string" ? handler.module.trim() : "";
      const handler_op = typeof handler.op === "string" ? handler.op.trim() : "";
      const roles_allowed = normalize_roles(entry.roles_allowed, []);
      if (!intent_id || !title || !skill_id || !handler_module || !handler_op || roles_allowed.length === 0) continue;
      if (seen.has(intent_id)) continue;
      seen.add(intent_id);
      out_items.push({
        intent_id,
        title,
        ...(typeof entry.description === "string" && entry.description.trim() ? { description: entry.description.trim() } : {}),
        roles_allowed,
        ...(normalize_string_array(entry.channels_allowed).length > 0
          ? { channels_allowed: normalize_string_array(entry.channels_allowed) }
          : {}),
        handler: {
          module: handler_module,
          op: handler_op
        },
        ...(is_plain_object(entry.params_schema) ? { params_schema: entry.params_schema as XBotIntent["params_schema"] } : {}),
        ...(normalize_string_array(entry.examples).length > 0 ? { examples: normalize_string_array(entry.examples) } : {}),
        ...(normalize_string_array(entry.synonyms).length > 0 ? { synonyms: normalize_string_array(entry.synonyms) } : {}),
        skill_id
      });
    }
    return out_items;
  }

  private async hydrate_from_xdb(): Promise<void> {
    const rows = await list_intent_configs_xdb(this._scope);
    this._configs_by_id.clear();
    for (const row of rows) {
      this._configs_by_id.set(row._id, {
        ...row,
        _roles_allowed: [...row._roles_allowed],
        _channels_allowed: [...row._channels_allowed],
        _synonyms: [...row._synonyms],
        _examples: [...row._examples]
      });
    }
  }

  private forward_ctx(xcmd: XCommandData): Dict {
    const ctx = readCommandCtx(xcmd);
    return {
      ...(typeof ctx.kernel_cap === "string" ? { kernel_cap: ctx.kernel_cap } : {}),
      ...(ctx.actor ? { actor: { ...ctx.actor } } : {})
    };
  }

  private ensure_params(value: unknown): Dict {
    if (value === undefined || value === null) return {};
    if (!is_plain_object(value)) {
      throw new XError("E_INTENT_BAD_PARAMS", "Expected params to be an object");
    }
    if (has_function(value)) {
      throw new XError("E_INTENT_BAD_PARAMS", "params must be JSON-safe");
    }
    return value;
  }
}

export default IntentRegistryModule;
