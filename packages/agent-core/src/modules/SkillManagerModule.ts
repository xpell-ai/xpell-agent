import fs from "node:fs/promises";
import path from "node:path";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

import { XError, XModule, _x, _xem, _xlog, type XCommandData } from "@xpell/node";

import { SETTINGS_MODULE_NAME } from "./SettingsModule.js";
import { readCommandCtx, requireKernelCap, requireKernelCapOrActorRole } from "../runtime/guards.js";
import type { SkillSettingsMeta, XSettingsSchema, XSettingsSchemaFieldType } from "../types/settings.js";
import type {
  AgentConfig,
  LoadedSkillRecord,
  SkillConfig,
  SkillLogLevel,
  SkillRegisterFn,
  XBotSkill,
  XBotSkillCapability,
  XBotSkillContext
} from "../types/skills.js";

export const SKILL_MANAGER_MODULE_NAME = "skills";

type Dict = Record<string, unknown>;

type SkillManagerOptions = {
  agent_id: string;
  version: string;
  config_path: string;
  repo_root: string;
  package_root: string;
  kernel_cap: string;
};

type ResolvedSkillEntry = {
  specifier: string;
  source: string;
};

type SkillRuntimeRecord = {
  id: string;
  version: string;
  kind: "xbot" | "legacy";
  capabilities: XBotSkillCapability;
  source: string;
  context: XBotSkillContext;
  on_disable?: ((ctx: XBotSkillContext) => Promise<void> | void) | undefined;
};

type LegacySkillContext = XBotSkillContext & {
  agent: {
    agent_id: string;
    version: string;
  };
  call(moduleName: string, op: string, params?: Record<string, unknown>): Promise<unknown>;
};

const DEFAULT_SKILL_CONFIG: SkillConfig = {
  allow: [],
  enabled: [],
  resolve: {
    node_modules: true,
    local_paths: []
  }
};

const DEFAULT_AGENT_CONFIG: AgentConfig = {
  skills: { ...DEFAULT_SKILL_CONFIG, resolve: { ...DEFAULT_SKILL_CONFIG.resolve } }
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
    throw new XError("E_SKILLS_BAD_PARAMS", `Invalid ${field_name}: expected non-empty string`);
  }
  return value.trim();
}

function ensure_optional_string(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalize_string_array(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (typeof item !== "string") continue;
    const trimmed = item.trim();
    if (!trimmed || seen.has(trimmed)) continue;
    seen.add(trimmed);
    out.push(trimmed);
  }
  return out;
}

function clone_json<T>(value: T): T {
  return structuredClone(value);
}

function is_valid_settings_field_type(value: unknown): value is XSettingsSchemaFieldType {
  return value === "string" || value === "number" || value === "boolean" || value === "select" || value === "string_list";
}

function to_error_message(err: unknown): string {
  if (err && typeof err === "object" && typeof (err as any).toXData === "function") {
    const data = (err as any).toXData();
    if (typeof (data as any)?._message === "string" && (data as any)._message.trim().length > 0) {
      return (data as any)._message.trim();
    }
  }
  if (err instanceof Error) return err.message;
  return String(err);
}

function is_path_within(target_path: string, root_path: string): boolean {
  const normalized_target = path.resolve(target_path);
  const normalized_root = path.resolve(root_path);
  if (normalized_target === normalized_root) return true;
  const root_with_sep = normalized_root.endsWith(path.sep) ? normalized_root : `${normalized_root}${path.sep}`;
  return normalized_target.startsWith(root_with_sep);
}

function clone_capabilities(capabilities: XBotSkillCapability | undefined): XBotSkillCapability | undefined {
  if (!capabilities) return undefined;
  return {
    ...(capabilities.kernel_ops ? { kernel_ops: [...capabilities.kernel_ops] } : {}),
    ...(capabilities.channels ? { channels: [...capabilities.channels] } : {}),
    ...(typeof capabilities.network === "boolean" ? { network: capabilities.network } : {})
  };
}

function clone_loaded_record(record: LoadedSkillRecord): LoadedSkillRecord {
  return {
    id: record.id,
    ...(record.version ? { version: record.version } : {}),
    enabled: record.enabled,
    status: record.status,
    ...(record.error ? { error: record.error } : {}),
    ...(record.source ? { source: record.source } : {}),
    ...(record.capabilities ? { capabilities: clone_capabilities(record.capabilities) } : {}),
    ...(record.modules_registered ? { modules_registered: [...record.modules_registered] } : {})
  };
}

function normalize_capabilities(value: unknown): XBotSkillCapability {
  if (!is_plain_object(value)) return {};
  return {
    kernel_ops: normalize_string_array(value.kernel_ops),
    channels: normalize_string_array(value.channels),
    ...(typeof value.network === "boolean" ? { network: value.network } : {})
  };
}

function op_key(module_name: string, op: string): string {
  return `${module_name}.${op}`;
}

export class SkillManagerModule extends XModule {
  static _name = SKILL_MANAGER_MODULE_NAME;

  private _agent_id: string;
  private _version: string;
  private _config_path: string;
  private _repo_root: string;
  private _package_root: string;
  private _kernel_cap: string;

  private _config: AgentConfig = DEFAULT_AGENT_CONFIG;
  private _enabled = new Set<string>();
  private _activating = new Set<string>();
  private _loaded = new Map<string, LoadedSkillRecord>();

  private _skill_to_modules = new Map<string, Set<string>>();
  private _module_to_skill = new Map<string, string>();
  private _runtime_by_skill = new Map<string, SkillRuntimeRecord>();
  private _settings_meta_by_skill = new Map<string, SkillSettingsMeta>();

  constructor(opts: SkillManagerOptions) {
    super({ _name: SKILL_MANAGER_MODULE_NAME });
    this._agent_id = opts.agent_id;
    this._version = opts.version;
    this._config_path = opts.config_path;
    this._repo_root = path.resolve(opts.repo_root);
    this._package_root = path.resolve(opts.package_root);
    this._kernel_cap = opts.kernel_cap;
  }

  async _list(_xcmd: XCommandData) {
    return this.list_impl();
  }
  async _op_list(xcmd: XCommandData) {
    return this._list(xcmd);
  }

  async _enable(xcmd: XCommandData) {
    return this.enable_impl(xcmd);
  }
  async _op_enable(xcmd: XCommandData) {
    return this._enable(xcmd);
  }

  async _disable(xcmd: XCommandData) {
    return this.disable_impl(xcmd);
  }
  async _op_disable(xcmd: XCommandData) {
    return this._disable(xcmd);
  }

  async _reload_enabled(xcmd: XCommandData) {
    return this.reload_enabled_impl(xcmd);
  }
  async _op_reload_enabled(xcmd: XCommandData) {
    return this._reload_enabled(xcmd);
  }

  async _get_settings(xcmd: XCommandData) {
    return this.get_settings_impl(xcmd);
  }
  async _op_get_settings(xcmd: XCommandData) {
    return this._get_settings(xcmd);
  }

  async _update_settings(xcmd: XCommandData) {
    return this.update_settings_impl(xcmd);
  }
  async _op_update_settings(xcmd: XCommandData) {
    return this._update_settings(xcmd);
  }

  assert_module_command_allowed(module_name: string): void {
    const skill_id = this._module_to_skill.get(module_name);
    if (!skill_id) return;
    if (this._enabled.has(skill_id)) return;
    throw new XError("E_SKILLS_MODULE_DISABLED", `Module '${module_name}' is disabled by skill '${skill_id}' state`);
  }

  resolve_skill_settings_meta(skill_id: string): SkillSettingsMeta | undefined {
    const current = this._settings_meta_by_skill.get(skill_id);
    if (!current) return undefined;
    return clone_json(current);
  }

  private list_impl() {
    return {
      allow: [...this._config.skills.allow],
      enabled: this.sorted_enabled(),
      loaded: this.to_loaded_list()
    };
  }

  private async enable_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");

    const params = this.ensure_params(xcmd._params);
    const id = ensure_non_empty_string(params.id, "id");
    const was_enabled = this._enabled.has(id);
    await this.enable_skill(id, "command");
    try {
      await this.persist_enabled_to_agent_config();
    } catch (err) {
      if (!was_enabled) {
        try {
          await this.disable_skill(id, "command");
        } catch {
          // keep original persistence error
        }
      }
      throw new XError("E_SKILLS_PERSIST_FAILED", "Failed to save enabled skills to agent.config.json", {
        _cause: to_error_message(err)
      });
    }
    const loaded = this._loaded.get(id);
    return { ok: true, ...(loaded ? { skill: clone_loaded_record(loaded) } : {}) };
  }

  private async disable_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");

    const params = this.ensure_params(xcmd._params);
    const id = ensure_non_empty_string(params.id, "id");
    const was_enabled = this._enabled.has(id);
    await this.disable_skill(id, "command");
    try {
      await this.persist_enabled_to_agent_config();
    } catch (err) {
      if (was_enabled) {
        try {
          await this.enable_skill(id, "command");
        } catch {
          // keep original persistence error
        }
      }
      throw new XError("E_SKILLS_PERSIST_FAILED", "Failed to save enabled skills to agent.config.json", {
        _cause: to_error_message(err)
      });
    }
    const loaded = this._loaded.get(id);
    return { ok: true, ...(loaded ? { skill: clone_loaded_record(loaded) } : {}) };
  }

  private async reload_enabled_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));

    this._config = await this.read_agent_config();

    _xlog.log("[agent-core] SkillManager ready");
    _xlog.log(`[agent-core] Loading enabled skills: ${JSON.stringify(this._config.skills.enabled)}`);

    const target_enabled = new Set(this._config.skills.enabled);

    for (const id of [...this._enabled]) {
      if (!target_enabled.has(id)) {
        await this.disable_skill(id, "reload");
      }
    }

    for (const id of this._config.skills.enabled) {
      try {
        await this.enable_skill(id, "reload");
      } catch (err) {
        this.set_error_state(id, to_error_message(err));
        _xlog.error("[agent-core] skill enable failed", { id, error: to_error_message(err) });
      }
    }

    return {
      ok: true,
      allow: [...this._config.skills.allow],
      enabled: this.sorted_enabled(),
      loaded: this.to_loaded_list()
    };
  }

  private async get_settings_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");

    const params = this.ensure_params(xcmd._params);
    const id = ensure_non_empty_string(params.id, "id");
    this.assert_allowlisted(id);

    const out = await _x.execute({
      _module: SETTINGS_MODULE_NAME,
      _op: "get_skill",
      _params: {
        skill_id: id,
        include_masked: false,
        include_schema: false,
        _ctx: this.forward_ctx(xcmd)
      }
    });

    const data = is_plain_object(out) ? out : {};
    const result = is_plain_object(data.result) ? data.result : {};
    return {
      id,
      settings: is_plain_object(result.settings) ? this.ensure_json_object(result.settings, "settings") : {}
    };
  }

  private async update_settings_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");

    const params = this.ensure_params(xcmd._params);
    const id = ensure_non_empty_string(params.id, "id");
    this.assert_allowlisted(id);

    const settings = this.ensure_json_object(params.settings ?? {}, "settings");

    await _x.execute({
      _module: SETTINGS_MODULE_NAME,
      _op: "set_skill",
      _params: {
        skill_id: id,
        patch: settings,
        _ctx: this.forward_ctx(xcmd)
      }
    });

    const refreshed = await _x.execute({
      _module: SETTINGS_MODULE_NAME,
      _op: "get_skill",
      _params: {
        skill_id: id,
        include_masked: false,
        include_schema: false,
        _ctx: this.forward_ctx(xcmd)
      }
    });

    const data = is_plain_object(refreshed) ? refreshed : {};
    const result = is_plain_object(data.result) ? data.result : {};
    return {
      ok: true,
      id,
      settings: is_plain_object(result.settings) ? this.ensure_json_object(result.settings, "settings") : {}
    };
  }

  private ensure_params(value: unknown): Dict {
    if (value === undefined || value === null) return {};
    if (!is_plain_object(value)) {
      throw new XError("E_SKILLS_BAD_PARAMS", "Expected params to be an object");
    }
    if (has_function(value)) {
      throw new XError("E_SKILLS_BAD_PARAMS", "params must be JSON-safe");
    }
    return value;
  }

  private async enable_skill(id: string, source: "reload" | "command"): Promise<void> {
    this.assert_allowlisted(id);

    const existing = this._loaded.get(id);
    if (existing?.status === "loaded" && this._enabled.has(id)) {
      return;
    }

    if (existing?.status === "disabled" && this._runtime_by_skill.has(id)) {
      await this.bootstrap_skill_settings_defaults(id, this._settings_meta_by_skill.get(id));
      await this.call_module_hooks(id, "enable");
      this._enabled.add(id);

      const runtime_record = this._runtime_by_skill.get(id);
      const modules = runtime_record ? Array.from(this._skill_to_modules.get(id) ?? []) : [];
      this._loaded.set(id, {
        id,
        version: runtime_record?.version,
        enabled: true,
        status: "loaded",
        source: existing.source ?? runtime_record?.source,
        capabilities: clone_capabilities(runtime_record?.capabilities),
        modules_registered: [...modules].sort((a, b) => a.localeCompare(b))
      });
      _xlog.log(`[agent-core] skill enabled id=${id} source=${source}`);
      return;
    }

    const resolved = await this.resolve_skill_entry(id);
    const loaded_module = await import(resolved.specifier);
    const descriptor = this.resolve_skill_descriptor(id, loaded_module);
    const ctx = this.create_skill_context(id, descriptor.version, descriptor.capabilities);

    try {
      this._activating.add(id);
      this._settings_meta_by_skill.set(id, descriptor.settings_meta ? clone_json(descriptor.settings_meta) : {});
      await this.bootstrap_skill_settings_defaults(id, descriptor.settings_meta);
      await descriptor.on_enable(ctx);
      this._enabled.add(id);
      const modules = Array.from(this._skill_to_modules.get(id) ?? []).sort((a, b) => a.localeCompare(b));

      this._runtime_by_skill.set(id, {
        id,
        version: descriptor.version,
        kind: descriptor.kind,
        capabilities: descriptor.capabilities,
        source: resolved.source,
        context: ctx,
        on_disable: descriptor.on_disable
      });

      this._loaded.set(id, {
        id,
        version: descriptor.version,
        enabled: true,
        status: "loaded",
        source: resolved.source,
        capabilities: clone_capabilities(descriptor.capabilities),
        modules_registered: modules
      });
      _xlog.log(`[agent-core] skill loaded id=${id} source=${resolved.source}`);
    } catch (err) {
      this._enabled.delete(id);
      this.set_error_state(id, to_error_message(err), resolved.source);
      this._settings_meta_by_skill.delete(id);
      throw err;
    } finally {
      this._activating.delete(id);
    }
  }

  private async disable_skill(id: string, source: "reload" | "command"): Promise<void> {
    this.assert_allowlisted(id);

    const runtime_record = this._runtime_by_skill.get(id);

    this._enabled.delete(id);

    if (runtime_record?.on_disable) {
      try {
        await runtime_record.on_disable(runtime_record.context);
      } catch (err) {
        this.log_skill("warn", id, "skill onDisable failed", { error: to_error_message(err) });
      }
    }

    await this.call_module_hooks(id, "disable");

    const modules = Array.from(this._skill_to_modules.get(id) ?? []).sort((a, b) => a.localeCompare(b));
    const existing = this._loaded.get(id);
    this._loaded.set(id, {
      id,
      version: runtime_record?.version ?? existing?.version,
      enabled: false,
      status: "disabled",
      source: existing?.source ?? runtime_record?.source,
      capabilities: clone_capabilities(runtime_record?.capabilities ?? existing?.capabilities),
      modules_registered: modules
    });

    _xlog.log(`[agent-core] skill disabled id=${id} source=${source}`);
  }

  private set_error_state(id: string, error_message: string, source?: string): void {
    const existing = this._loaded.get(id);
    const runtime_record = this._runtime_by_skill.get(id);
    const modules = Array.from(this._skill_to_modules.get(id) ?? []).sort((a, b) => a.localeCompare(b));
    this._loaded.set(id, {
      id,
      version: runtime_record?.version ?? existing?.version,
      enabled: false,
      status: "error",
      error: error_message,
      source: source ?? existing?.source ?? runtime_record?.source,
      capabilities: clone_capabilities(runtime_record?.capabilities ?? existing?.capabilities),
      modules_registered: modules
    });
  }

  private assert_allowlisted(id: string): void {
    if (!this._config.skills.allow.includes(id)) {
      throw new XError("E_SKILLS_NOT_ALLOWLISTED", `Skill id is not in allowlist: ${id}`);
    }
  }

  private resolve_skill_descriptor(id: string, module_exports: unknown): {
    kind: "xbot" | "legacy";
    version: string;
    capabilities: XBotSkillCapability;
    settings_meta?: SkillSettingsMeta;
    on_enable: (ctx: XBotSkillContext) => Promise<void> | void;
    on_disable?: (ctx: XBotSkillContext) => Promise<void> | void;
  } {
    if (!is_plain_object(module_exports)) {
      throw new XError("E_SKILLS_BAD_EXPORT", "Skill module must export an object");
    }

    const skill_export = (module_exports as Dict).skill;
    if (is_plain_object(skill_export)) {
      const skill = this.validate_xbot_skill(id, skill_export);
      return {
        kind: "xbot",
        version: skill.version,
        capabilities: normalize_capabilities(skill.capabilities),
        ...(skill.settings ? { settings_meta: this.normalize_skill_settings_meta(skill.settings, id) } : {}),
        on_enable: skill.onEnable.bind(skill),
        ...(typeof skill.onDisable === "function" ? { on_disable: skill.onDisable.bind(skill) } : {})
      };
    }

    const register_skill = this.resolve_legacy_register_fn(module_exports);
    return {
      kind: "legacy",
      version: "0.0.0-legacy",
      capabilities: {},
      settings_meta: {},
      on_enable: (ctx) => register_skill(this.to_legacy_context(ctx))
    };
  }

  private validate_xbot_skill(id: string, value: Dict): XBotSkill {
    const skill_id = ensure_non_empty_string(value.id, "skill.id");
    if (skill_id !== id) {
      throw new XError("E_SKILLS_BAD_EXPORT", `Skill id mismatch. expected=${id} got=${skill_id}`);
    }

    const version = ensure_non_empty_string(value.version, "skill.version");
    const on_enable = value.onEnable;
    if (typeof on_enable !== "function") {
      throw new XError("E_SKILLS_BAD_EXPORT", "skill.onEnable must be a function");
    }

    const skill: XBotSkill = {
      id: skill_id,
      version,
      ...(ensure_optional_string(value.name) ? { name: ensure_optional_string(value.name) } : {}),
      ...(ensure_optional_string(value.description) ? { description: ensure_optional_string(value.description) } : {}),
      ...(value.settings !== undefined ? { settings: value.settings as SkillSettingsMeta } : {}),
      capabilities: normalize_capabilities(value.capabilities),
      onEnable: on_enable as XBotSkill["onEnable"],
      ...(typeof value.onDisable === "function" ? { onDisable: value.onDisable as XBotSkill["onDisable"] } : {})
    };
    return skill;
  }

  private resolve_legacy_register_fn(module_exports: Dict): SkillRegisterFn {
    const named = module_exports.registerSkill;
    if (typeof named === "function") return named as SkillRegisterFn;

    const default_export = module_exports.default;
    if (typeof default_export === "function") return default_export as SkillRegisterFn;
    if (is_plain_object(default_export) && typeof default_export.registerSkill === "function") {
      return default_export.registerSkill as SkillRegisterFn;
    }

    throw new XError("E_SKILLS_BAD_EXPORT", "Skill must export `skill` or legacy `registerSkill(ctx)`");
  }

  private create_skill_context(skill_id: string, skill_version: string, capabilities: XBotSkillCapability): XBotSkillContext {
    const kernel_allow = new Set(normalize_string_array(capabilities.kernel_ops));

    return {
      skill: {
        id: skill_id,
        version: skill_version
      },
      registerModule: (module_instance) => {
        if (!module_instance || typeof module_instance !== "object") {
          throw new XError("E_SKILLS_BAD_MODULE", "registerModule expects XModule instance");
        }

        const module_name = ensure_non_empty_string(
          (module_instance as Dict)._name ?? (module_instance as any)?.constructor?._name,
          "module _name"
        );
        const mapped_skill = this._module_to_skill.get(module_name);
        if (mapped_skill && mapped_skill !== skill_id) {
          throw new XError("E_SKILLS_MODULE_CONFLICT", `Module '${module_name}' already registered by '${mapped_skill}'`);
        }

        let existing_module: unknown;
        let already_loaded = false;
        try {
          existing_module = _x.getModule(module_name);
          already_loaded = existing_module !== undefined && existing_module !== null;
        } catch {
          already_loaded = false;
        }
        if (!already_loaded) {
          _x.loadModule(module_instance as any);
        } else if (!mapped_skill) {
          throw new XError(
            "E_SKILLS_MODULE_CONFLICT",
            `Module '${module_name}' already exists and is not managed by skill '${skill_id}'`
          );
        }

        if (!this._skill_to_modules.has(skill_id)) {
          this._skill_to_modules.set(skill_id, new Set());
        }
        this._skill_to_modules.get(skill_id)?.add(module_name);
        this._module_to_skill.set(module_name, skill_id);
      },
      execute: async (module_name, op, params, meta) => {
        this.assert_skill_enabled_for_callback(skill_id);

        const _module = ensure_non_empty_string(module_name, "module");
        const _op = ensure_non_empty_string(op, "op");
        const _params = params === undefined ? {} : this.ensure_json_object(params, "params");
        const op_name = op_key(_module, _op);
        const is_kernel_allowed = kernel_allow.has(op_name);

        const _ctx: Dict = {
          actor: {
            role: "system",
            source: `skill:${skill_id}`
          }
        };
        if (is_kernel_allowed) {
          _ctx.kernel_cap = this._kernel_cap;
        }
        if (meta !== undefined) {
          if (has_function(meta)) {
            throw new XError("E_SKILLS_BAD_PARAMS", "meta must be JSON-safe");
          }
          _ctx.meta = meta as any;
        }

        return _x.execute({
          _module,
          _op,
          _params: {
            ..._params,
            _ctx
          }
        });
      },
      emit: (event_name, payload) => {
        this.assert_skill_enabled_for_callback(skill_id);
        const name = ensure_non_empty_string(event_name, "eventName");
        if (has_function(payload)) {
          throw new XError("E_SKILLS_BAD_PARAMS", "emit payload must be JSON-safe");
        }
        _xem.fire(name, payload);
      },
      log: (level, msg, meta) => {
        this.log_skill(level, skill_id, msg, meta);
      }
    };
  }

  private to_legacy_context(ctx: XBotSkillContext): LegacySkillContext {
    return {
      ...ctx,
      agent: {
        agent_id: this._agent_id,
        version: this._version
      },
      call: (module_name, op, params) => ctx.execute(module_name, op, params)
    };
  }

  private assert_skill_enabled_for_callback(skill_id: string): void {
    if (!this._enabled.has(skill_id) && !this._activating.has(skill_id)) {
      throw new XError("E_SKILLS_DISABLED", `Skill is disabled: ${skill_id}`);
    }
  }

  private log_skill(level: SkillLogLevel, skill_id: string, msg: string, meta?: unknown): void {
    const message = `[skill:${skill_id}] ${msg}`;
    if (level === "debug" || level === "info") {
      _xlog.log(message, meta);
      return;
    }
    if (level === "warn") {
      _xlog.warn(message, meta);
      return;
    }
    _xlog.error(message, meta);
  }

  private ensure_json_object(value: unknown, field_name: string): Record<string, unknown> {
    if (!is_plain_object(value)) {
      throw new XError("E_SKILLS_BAD_PARAMS", `${field_name} must be an object`);
    }
    if (has_function(value)) {
      throw new XError("E_SKILLS_BAD_PARAMS", `${field_name} must be JSON-safe`);
    }
    return { ...value };
  }

  private normalize_skill_settings_meta(value: SkillSettingsMeta, skill_id: string): SkillSettingsMeta {
    const out: SkillSettingsMeta = {};

    if (value.defaults !== undefined) {
      if (!is_plain_object(value.defaults) || has_function(value.defaults)) {
        throw new XError("E_SKILLS_BAD_EXPORT", `skill.settings.defaults must be a JSON object: ${skill_id}`);
      }
      out.defaults = clone_json(value.defaults);
    }

    if (value.sensitive !== undefined) {
      out.sensitive = normalize_string_array(value.sensitive);
    }

    if (value.schema !== undefined) {
      out.schema = this.normalize_settings_schema(value.schema, skill_id);
    }

    return out;
  }

  private normalize_settings_schema(value: XSettingsSchema, skill_id: string): XSettingsSchema {
    if (!is_plain_object(value) || !Array.isArray(value.fields)) {
      throw new XError("E_SKILLS_BAD_EXPORT", `skill.settings.schema must include fields[]: ${skill_id}`);
    }

    const fields = value.fields.map((field, idx) => {
      if (!is_plain_object(field)) {
        throw new XError("E_SKILLS_BAD_EXPORT", `skill.settings.schema.fields[${idx}] must be an object: ${skill_id}`);
      }

      const key = ensure_non_empty_string(field.key, `skill.settings.schema.fields[${idx}].key`);
      const label = ensure_non_empty_string(field.label, `skill.settings.schema.fields[${idx}].label`);
      if (!is_valid_settings_field_type(field.type)) {
        throw new XError("E_SKILLS_BAD_EXPORT", `Invalid skill.settings.schema.fields[${idx}].type: ${skill_id}`);
      }

      const options = Array.isArray(field.options)
        ? field.options.map((option, option_idx) => {
            if (!is_plain_object(option)) {
              throw new XError(
                "E_SKILLS_BAD_EXPORT",
                `skill.settings.schema.fields[${idx}].options[${option_idx}] must be an object: ${skill_id}`
              );
            }
            return {
              label: ensure_non_empty_string(
                option.label,
                `skill.settings.schema.fields[${idx}].options[${option_idx}].label`
              ),
              value: clone_json(option.value)
            };
          })
        : undefined;

      return {
        key,
        label,
        type: field.type,
        ...(typeof field.help === "string" ? { help: field.help } : {}),
        ...(typeof field.secret === "boolean" ? { secret: field.secret } : {}),
        ...(options && options.length > 0 ? { options } : {}),
        ...(typeof field.placeholder === "string" ? { placeholder: field.placeholder } : {})
      };
    });

    return {
      ...(typeof value.title === "string" ? { title: value.title } : {}),
      fields
    };
  }

  private forward_ctx(xcmd: XCommandData): Dict {
    const ctx = readCommandCtx(xcmd);
    return {
      ...(typeof ctx._wid === "string" ? { _wid: ctx._wid } : {}),
      ...(typeof ctx._sid === "string" ? { _sid: ctx._sid } : {}),
      ...(typeof ctx.kernel_cap === "string" ? { kernel_cap: ctx.kernel_cap } : {}),
      ...(ctx.actor ? { actor: { ...ctx.actor } } : {})
    };
  }

  private kernel_ctx(): Dict {
    return {
      kernel_cap: this._kernel_cap,
      actor: {
        role: "system",
        source: "skill-manager"
      }
    };
  }

  private async bootstrap_skill_settings_defaults(skill_id: string, meta?: SkillSettingsMeta): Promise<void> {
    await _x.execute({
      _module: SETTINGS_MODULE_NAME,
      _op: "get_skill",
      _params: {
        skill_id,
        include_masked: false,
        include_schema: false,
        _ctx: this.kernel_ctx()
      }
    });

    const defaults = is_plain_object(meta?.defaults) ? clone_json(meta.defaults) : {};
    if (Object.keys(defaults).length === 0) return;

    const existing = await _x.execute({
      _module: SETTINGS_MODULE_NAME,
      _op: "get",
      _params: {
        key: "skills",
        _ctx: this.kernel_ctx()
      }
    });

    const existing_data = is_plain_object(existing) ? existing : {};
    const skills_bucket = is_plain_object(existing_data.value) ? existing_data.value : {};
    const has_stored = Object.prototype.hasOwnProperty.call(skills_bucket, skill_id);
    if (has_stored) return;

    await _x.execute({
      _module: SETTINGS_MODULE_NAME,
      _op: "set_skill",
      _params: {
        skill_id,
        patch: defaults,
        _ctx: this.kernel_ctx()
      }
    });
  }

  private sorted_enabled(): string[] {
    return Array.from(this._enabled).sort((a, b) => a.localeCompare(b));
  }

  private to_loaded_list(): LoadedSkillRecord[] {
    const ids = new Set<string>();
    for (const id of this._config.skills.allow) ids.add(id);
    for (const id of this._loaded.keys()) ids.add(id);

    return Array.from(ids)
      .sort((a, b) => a.localeCompare(b))
      .map((id) => {
        const state = this._loaded.get(id);
        if (state) return clone_loaded_record(state);
        return {
          id,
          version: undefined,
          enabled: false,
          status: "disabled"
        } satisfies LoadedSkillRecord;
      });
  }

  private async call_module_hooks(skill_id: string, op: "enable" | "disable"): Promise<void> {
    const modules = Array.from(this._skill_to_modules.get(skill_id) ?? []);
    for (const module_name of modules) {
      try {
        await _x.execute({
          _module: module_name,
          _op: op,
          _params: {
            reason: `skills.${op}`,
            skill_id,
            _ctx: {
              kernel_cap: this._kernel_cap,
              actor: {
                role: "system",
                source: "skill-manager"
              }
            }
          }
        });
      } catch (err) {
        const msg = to_error_message(err);
        if (msg.includes("cant find op")) continue;
        this.log_skill("warn", skill_id, `module hook failed ${module_name}.${op}`, { error: msg });
      }
    }
  }

  private async resolve_skill_entry(id: string): Promise<ResolvedSkillEntry> {
    const errors: string[] = [];

    if (this._config.skills.resolve.node_modules) {
      try {
        return this.resolve_node_module(id);
      } catch (err) {
        errors.push(`node_modules: ${to_error_message(err)}`);
      }
    }

    for (const raw_local_path of this._config.skills.resolve.local_paths) {
      try {
        const resolved = await this.resolve_local_path(id, raw_local_path);
        if (resolved) return resolved;
      } catch (err) {
        errors.push(`local_path(${raw_local_path}): ${to_error_message(err)}`);
      }
    }

    throw new XError("E_SKILLS_RESOLVE_FAILED", `Unable to resolve skill '${id}'`, {
      _errors: errors
    });
  }

  private async persist_enabled_to_agent_config(): Promise<void> {
    const enabled = this.sorted_enabled().filter((id) => this._config.skills.allow.includes(id));
    this._config.skills.enabled = [...enabled];

    let parsed: Dict = {};
    try {
      const raw = await fs.readFile(this._config_path, "utf8");
      const decoded = JSON.parse(raw);
      if (is_plain_object(decoded)) parsed = { ...decoded };
    } catch (err: any) {
      if (err?.code !== "ENOENT") throw err;
    }

    const existing_skills = is_plain_object(parsed.skills) ? { ...parsed.skills } : {};
    const existing_resolve = is_plain_object(existing_skills.resolve) ? { ...existing_skills.resolve } : {};

    parsed.skills = {
      ...existing_skills,
      allow: [...this._config.skills.allow],
      enabled: [...enabled],
      resolve: {
        ...existing_resolve,
        node_modules: this._config.skills.resolve.node_modules,
        local_paths: [...this._config.skills.resolve.local_paths]
      }
    };

    await fs.writeFile(this._config_path, `${JSON.stringify(parsed, null, 2)}\n`, "utf8");
  }

  private resolve_node_module(id: string): ResolvedSkillEntry {
    const require = createRequire(import.meta.url);
    const resolved_path = require.resolve(id, { paths: [this._package_root] });
    return {
      specifier: pathToFileURL(resolved_path).href,
      source: "node_modules"
    };
  }

  private async resolve_local_path(id: string, raw_local_path: string): Promise<ResolvedSkillEntry | undefined> {
    const local_root = this.normalize_local_path(raw_local_path);
    const candidates = [local_root, path.resolve(local_root, id)];

    for (const candidate of candidates) {
      const pkg = await this.read_package_json(candidate);
      if (!pkg) continue;

      const package_name = ensure_optional_string(pkg.name);
      if (!package_name || package_name !== id) continue;

      const entry_file = this.resolve_package_entry(candidate, pkg);
      const stat = await fs.stat(entry_file);
      if (!stat.isFile()) {
        throw new XError("E_SKILLS_RESOLVE_FAILED", `Skill entry is not a file: ${entry_file}`);
      }

      return {
        specifier: pathToFileURL(entry_file).href,
        source: `local:${candidate}`
      };
    }

    return undefined;
  }

  private normalize_local_path(raw_local_path: string): string {
    const normalized = ensure_non_empty_string(raw_local_path, "local_paths[]");
    const absolute = path.isAbsolute(normalized) ? path.resolve(normalized) : path.resolve(this._repo_root, normalized);
    if (!is_path_within(absolute, this._repo_root)) {
      throw new XError("E_SKILLS_BAD_CONFIG", `local_paths entry escapes repo root: ${raw_local_path}`);
    }
    return absolute;
  }

  private async read_package_json(package_root: string): Promise<Dict | undefined> {
    const package_json_path = path.resolve(package_root, "package.json");
    try {
      const raw = await fs.readFile(package_json_path, "utf8");
      const parsed = JSON.parse(raw);
      if (!is_plain_object(parsed)) {
        throw new XError("E_SKILLS_BAD_CONFIG", `Invalid package.json object: ${package_json_path}`);
      }
      return parsed;
    } catch (err: any) {
      if (err?.code === "ENOENT") return undefined;
      throw err;
    }
  }

  private resolve_package_entry(package_root: string, pkg: Dict): string {
    const exports_field = pkg.exports;
    let entry_rel: string | undefined;

    if (typeof exports_field === "string") {
      entry_rel = exports_field;
    } else if (is_plain_object(exports_field)) {
      const root_export = exports_field["."] ?? exports_field;
      if (typeof root_export === "string") {
        entry_rel = root_export;
      } else if (is_plain_object(root_export)) {
        entry_rel =
          ensure_optional_string(root_export.import) ??
          ensure_optional_string(root_export.default) ??
          ensure_optional_string(root_export.node);
      }
    }

    if (!entry_rel) {
      entry_rel = ensure_optional_string(pkg.module) ?? ensure_optional_string(pkg.main) ?? "./index.js";
    }

    const entry_file = path.resolve(package_root, entry_rel);
    if (!is_path_within(entry_file, package_root)) {
      throw new XError("E_SKILLS_BAD_CONFIG", `Invalid package entry path: ${entry_rel}`);
    }
    return entry_file;
  }

  private async read_agent_config(): Promise<AgentConfig> {
    let raw: string;
    try {
      raw = await fs.readFile(this._config_path, "utf8");
    } catch (err: any) {
      if (err?.code === "ENOENT") {
        return {
          skills: {
            allow: [],
            enabled: [],
            resolve: {
              node_modules: true,
              local_paths: []
            }
          }
        };
      }
      throw err;
    }

    const parsed = JSON.parse(raw);
    if (!is_plain_object(parsed)) {
      throw new XError("E_SKILLS_BAD_CONFIG", "agent.config.json must be an object");
    }

    const raw_skills = is_plain_object(parsed.skills) ? parsed.skills : {};
    const allow = normalize_string_array(raw_skills.allow);
    const enabled = normalize_string_array(raw_skills.enabled).filter((id) => allow.includes(id));

    const raw_resolve = is_plain_object(raw_skills.resolve) ? raw_skills.resolve : {};
    const node_modules = raw_resolve.node_modules === undefined ? true : raw_resolve.node_modules === true;
    const local_paths = normalize_string_array(raw_resolve.local_paths);

    return {
      skills: {
        allow,
        enabled,
        resolve: {
          node_modules,
          local_paths
        }
      }
    };
  }
}

export default SkillManagerModule;
