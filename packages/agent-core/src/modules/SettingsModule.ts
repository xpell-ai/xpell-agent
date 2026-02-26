import path from "node:path";
import fs from "node:fs";

import { XError, XModule, _xem, type XCommandData, Settings as XSettings } from "@xpell/node";

import { readCommandCtx, requireKernelCap, requireKernelCapOrActorRole } from "../runtime/guards.js";
import {
  applyPatchWithMaskHandling,
  deepMerge,
  getByPath,
  maskSensitive,
  setByPath
} from "../runtime/settings_utils.js";
import type { SettingsGetSkillResult, SkillSettingsMeta, XSettingsSchema, XSettingsSchemaFieldType } from "../types/settings.js";

export const SETTINGS_MODULE_NAME = "settings";

type Dict = Record<string, unknown>;

type SettingsModuleOptions = {
  _work_dir?: string;
  _resolve_skill_meta?: (skill_id: string) => SkillSettingsMeta | undefined;
};

let _xsettings_initialized = false;
let _xsettings_work_dir = "";

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function has_function(value: unknown): boolean {
  if (typeof value === "function") return true;
  if (Array.isArray(value)) return value.some(has_function);
  if (is_plain_object(value)) return Object.values(value).some(has_function);
  return false;
}

function clone_json<T>(value: T): T {
  return structuredClone(value);
}

function ensure_non_empty_string(value: unknown, field_name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new XError("E_SETTINGS_BAD_PARAMS", `Invalid ${field_name}: expected non-empty string`);
  }
  return value.trim();
}

function ensure_optional_string(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalize_boolean(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") return value;
  return fallback;
}

function normalize_positive_int(value: unknown, fallback: number): number {
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return parsed;
}

function is_valid_field_type(value: unknown): value is XSettingsSchemaFieldType {
  return value === "string" || value === "number" || value === "boolean" || value === "select" || value === "string_list";
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

export class SettingsModule extends XModule {
  static _name = SETTINGS_MODULE_NAME;

  private _work_dir: string;
  private _resolve_skill_meta?: (skill_id: string) => SkillSettingsMeta | undefined;

  constructor(opts: SettingsModuleOptions = {}) {
    super({ _name: SETTINGS_MODULE_NAME });
    this._work_dir = path.resolve(opts._work_dir ?? path.resolve(process.cwd(), "work"));
    this._resolve_skill_meta = opts._resolve_skill_meta;
  }

  set_skill_meta_resolver(resolver?: (skill_id: string) => SkillSettingsMeta | undefined): void {
    this._resolve_skill_meta = resolver;
  }

  async _init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }
  async _op_init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }

  async _get(xcmd: XCommandData) {
    return this.get_impl(xcmd);
  }
  async _op_get(xcmd: XCommandData) {
    return this.get_impl(xcmd);
  }

  async _set(xcmd: XCommandData) {
    return this.set_impl(xcmd);
  }
  async _op_set(xcmd: XCommandData) {
    return this.set_impl(xcmd);
  }

  async _get_skill(xcmd: XCommandData) {
    return this.get_skill_impl(xcmd);
  }
  async _op_get_skill(xcmd: XCommandData) {
    return this.get_skill_impl(xcmd);
  }

  async _set_skill(xcmd: XCommandData) {
    return this.set_skill_impl(xcmd);
  }
  async _op_set_skill(xcmd: XCommandData) {
    return this.set_skill_impl(xcmd);
  }

  async _reset_skill(xcmd: XCommandData) {
    return this.reset_skill_impl(xcmd);
  }
  async _op_reset_skill(xcmd: XCommandData) {
    return this.reset_skill_impl(xcmd);
  }

  async _schema(xcmd: XCommandData) {
    return this.schema_impl(xcmd);
  }
  async _op_schema(xcmd: XCommandData) {
    return this.schema_impl(xcmd);
  }

  private init_on_boot_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    this.ensure_storage_ready();
    this.ensure_root_keys();
    return { ok: true };
  }

  private get_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    this.ensure_storage_ready();
    const params = this.ensure_params(xcmd._params);
    const key = ensure_optional_string(params.key);
    const root = this.read_root();

    if (!key) {
      return { ok: true, value: root };
    }

    return {
      ok: true,
      value: clone_json(getByPath(root, key))
    };
  }

  private set_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    this.ensure_storage_ready();
    const params = this.ensure_params(xcmd._params);
    const key = ensure_non_empty_string(params.key, "key");
    const root = this.read_root();
    setByPath(root, key, clone_json(params.value));
    this.persist_root(root);
    return { ok: true };
  }

  private get_skill_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    this.ensure_storage_ready();
    const params = this.ensure_params(xcmd._params);
    const skill_id = ensure_non_empty_string(params.skill_id, "skill_id");
    const include_schema = normalize_boolean(params.include_schema, false);
    const include_masked = normalize_boolean(params.include_masked, true);

    const root = this.read_root();
    const raw_stored = getByPath(root, `skills.${skill_id}`);
    const stored = is_plain_object(raw_stored) ? clone_json(raw_stored) : {};
    const meta = this.resolve_skill_meta(skill_id);
    const defaults = is_plain_object(meta?.defaults) ? clone_json(meta.defaults) : {};
    const sensitive_paths = normalize_string_array(meta?.sensitive);

    const merged = deepMerge(defaults, stored);
    const masked_payload = include_masked
      ? maskSensitive(merged, sensitive_paths)
      : { maskedSettings: clone_json(merged), maskedMap: {} };

    const result: SettingsGetSkillResult = {
      skill_id,
      settings: masked_payload.maskedSettings,
      masked: masked_payload.maskedMap,
      ...(include_schema && meta?.schema ? { schema: clone_json(meta.schema) } : {})
    };

    return {
      ok: true,
      result
    };
  }

  private set_skill_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    this.ensure_storage_ready();
    const params = this.ensure_params(xcmd._params);
    const skill_id = ensure_non_empty_string(params.skill_id, "skill_id");
    const patch = this.ensure_json_object(params.patch ?? {}, "patch");
    const meta = this.resolve_skill_meta(skill_id);
    const sensitive_paths = normalize_string_array(meta?.sensitive);
    const root = this.read_root();
    const skills = this.read_skills_bucket(root);
    const existing_raw = skills[skill_id];
    const existing = is_plain_object(existing_raw) ? clone_json(existing_raw) : {};

    const merged_stored = applyPatchWithMaskHandling(existing, patch, sensitive_paths);
    skills[skill_id] = merged_stored;
    root.skills = skills;
    this.persist_root(root);

    _xem.fire("settings.skill.updated", { skill_id });
    return { ok: true };
  }

  private reset_skill_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    this.ensure_storage_ready();
    const params = this.ensure_params(xcmd._params);
    const skill_id = ensure_non_empty_string(params.skill_id, "skill_id");
    const root = this.read_root();
    const skills = this.read_skills_bucket(root);
    if (Object.prototype.hasOwnProperty.call(skills, skill_id)) {
      delete skills[skill_id];
      root.skills = skills;
      this.persist_root(root);
    }
    _xem.fire("settings.skill.updated", { skill_id });
    return { ok: true };
  }

  private schema_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    this.ensure_storage_ready();
    const params = this.ensure_params(xcmd._params);
    const skill_id = ensure_non_empty_string(params.skill_id, "skill_id");
    const meta = this.resolve_skill_meta(skill_id);
    if (!meta?.schema) {
      return { ok: false, reason: "no_schema" };
    }
    return {
      ok: true,
      schema: clone_json(meta.schema)
    };
  }

  private ensure_params(value: unknown): Dict {
    if (value === undefined || value === null) return {};
    if (!is_plain_object(value)) {
      throw new XError("E_SETTINGS_BAD_PARAMS", "Expected params to be an object");
    }
    if (has_function(value)) {
      throw new XError("E_SETTINGS_BAD_PARAMS", "params must be JSON-safe");
    }
    return value;
  }

  private ensure_json_object(value: unknown, field_name: string): Record<string, unknown> {
    if (!is_plain_object(value)) {
      throw new XError("E_SETTINGS_BAD_PARAMS", `${field_name} must be an object`);
    }
    if (has_function(value)) {
      throw new XError("E_SETTINGS_BAD_PARAMS", `${field_name} must be JSON-safe`);
    }
    return clone_json(value);
  }

  private ensure_storage_ready(): void {
    const settings_file_path = path.resolve(this._work_dir, "settings", "server-settings.json");

    if (!_xsettings_initialized) {
      // IMPORTANT:
      // _xs.onSetup() writes current in-memory data to disk immediately.
      // Calling it on every process start can overwrite persisted settings.
      // Use onSetup only for first-time bootstrap when file is missing.
      if (!fs.existsSync(settings_file_path)) {
        XSettings.onSetup(this._work_dir);
      }
      _xsettings_initialized = true;
      _xsettings_work_dir = this._work_dir;
    } else if (_xsettings_work_dir !== this._work_dir) {
      throw new XError(
        "E_SETTINGS_INIT",
        `XSettings already initialized with work_dir='${_xsettings_work_dir}' and cannot switch to '${this._work_dir}'`
      );
    }
    XSettings.init(this._work_dir);
  }

  private ensure_root_keys(): void {
    const root = this.read_root();
    this.persist_root(root);
  }

  private read_root(): Dict {
    const source = XSettings.getAll();
    const root = is_plain_object(source) ? clone_json(source) : {};

    if (!is_plain_object(root.ui)) {
      root.ui = {};
    }

    if (!is_plain_object(root.skills)) {
      root.skills = {};
    }

    const agent_settings = is_plain_object(root.agent) ? clone_json(root.agent) : {};
    if (!Object.prototype.hasOwnProperty.call(agent_settings, "name")) {
      agent_settings.name = "XBot";
    }
    if (!Object.prototype.hasOwnProperty.call(agent_settings, "business_name")) {
      agent_settings.business_name = "Ruta1";
    }
    root.agent = agent_settings;

    const kb_settings = is_plain_object(root.kb) ? clone_json(root.kb) : {};
    const export_roles = normalize_string_array(kb_settings.export_roles).filter(
      (role): role is "owner" | "admin" => role === "owner" || role === "admin"
    );
    kb_settings.allow_export = normalize_boolean(kb_settings.allow_export, false);
    kb_settings.export_roles = export_roles.length > 0 ? export_roles : ["owner", "admin"];
    kb_settings.max_export_chars = normalize_positive_int(kb_settings.max_export_chars, 8000);
    root.kb = kb_settings;

    return root;
  }

  private read_skills_bucket(root: Dict): Dict {
    const current = root.skills;
    if (is_plain_object(current)) {
      return clone_json(current);
    }
    return {};
  }

  private persist_root(root: Dict): void {
    for (const [key, value] of Object.entries(root)) {
      XSettings.set(key, clone_json(value));
    }
  }

  private resolve_skill_meta(skill_id: string): SkillSettingsMeta | undefined {
    if (!this._resolve_skill_meta) return undefined;
    const raw_meta = this._resolve_skill_meta(skill_id);
    if (!raw_meta) return undefined;
    if (!is_plain_object(raw_meta)) {
      throw new XError("E_SETTINGS_BAD_META", `Skill settings meta for '${skill_id}' must be an object`);
    }

    const normalized: SkillSettingsMeta = {};

    if (raw_meta.defaults !== undefined) {
      if (!is_plain_object(raw_meta.defaults)) {
        throw new XError("E_SETTINGS_BAD_META", `Skill settings defaults for '${skill_id}' must be an object`);
      }
      if (has_function(raw_meta.defaults)) {
        throw new XError("E_SETTINGS_BAD_META", `Skill settings defaults for '${skill_id}' must be JSON-safe`);
      }
      normalized.defaults = clone_json(raw_meta.defaults);
    }

    if (raw_meta.sensitive !== undefined) {
      normalized.sensitive = normalize_string_array(raw_meta.sensitive);
    }

    if (raw_meta.schema !== undefined) {
      normalized.schema = this.normalize_schema(skill_id, raw_meta.schema);
    }

    return normalized;
  }

  private normalize_schema(skill_id: string, value: unknown): XSettingsSchema {
    if (!is_plain_object(value)) {
      throw new XError("E_SETTINGS_BAD_META", `Skill schema for '${skill_id}' must be an object`);
    }

    if (!Array.isArray(value.fields)) {
      throw new XError("E_SETTINGS_BAD_META", `Skill schema fields for '${skill_id}' must be an array`);
    }

    const fields = value.fields.map((field, idx) => {
      if (!is_plain_object(field)) {
        throw new XError("E_SETTINGS_BAD_META", `Skill schema field[${idx}] for '${skill_id}' must be an object`);
      }

      const key = ensure_non_empty_string(field.key, `schema.fields[${idx}].key`);
      const label = ensure_non_empty_string(field.label, `schema.fields[${idx}].label`);
      if (!is_valid_field_type(field.type)) {
        throw new XError("E_SETTINGS_BAD_META", `Invalid schema field type for '${skill_id}' at key='${key}'`);
      }

      const options = Array.isArray(field.options)
        ? field.options
            .map((item, option_idx) => {
              if (!is_plain_object(item)) {
                throw new XError(
                  "E_SETTINGS_BAD_META",
                  `Skill schema option[${option_idx}] for '${skill_id}' field='${key}' must be an object`
                );
              }
              return {
                label: ensure_non_empty_string(item.label, `schema.fields[${idx}].options[${option_idx}].label`),
                value: clone_json(item.value)
              };
            })
            .filter((item) => item.label.length > 0)
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
}
