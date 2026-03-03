import { randomUUID, createHash } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import { XError, _x, _xd, _xem, _xlog, type XCommandData } from "@xpell/node";

import { readCommandCtx, requireKernelCap, requireKernelCapOrActorRole, type AgentCommandCtx } from "../runtime/guards.js";
import { extract_section, normalize_heading_query } from "../kb/kb_markdown.js";
import {
  init_kb_xdb,
  kb_append,
  kb_delete_section,
  kb_get_doc,
  kb_list_audit,
  kb_patch_section,
  kb_set_doc,
  pending_action_consume,
  pending_action_create,
  pending_action_get,
  type AgentKbXdbScope,
  type KbActor,
  type PersistedAdminPendingActionRecord,
  type PersistedKbAuditRecord,
  type PersistedKbDocRecord
} from "../xdb/kb-xdb.js";
import { KNOWLEDGE_MODULE_NAME, KnowledgeModule } from "./KnowledgeModule.js";
import { SETTINGS_MODULE_NAME } from "./SettingsModule.js";

type Dict = Record<string, unknown>;

type KnowledgeBaseModuleOptions = {
  _kb_dir: string;
  _work_dir: string;
  _app_id?: string;
  _env?: string;
};

type KbDocLang = "es" | "en";
type PendingProposal = {
  kind:
    | "kb.replace"
    | "kb.delete_section"
    | "kb.patch_section"
    | "kb.append"
    | "kb.append_to_section"
    | "kb.remove_from_section"
    | "kb.awaiting_input"
    | "kb.update_price";
  kb_id: string;
  lang: string;
  content?: string;
  section_title?: string;
  rationale?: string;
  item_name?: string;
  from_price?: string;
  to_price?: string;
};

type ResolvedKbTarget = {
  _kb_file: string;
  _abs_path: string;
  _legacy_abs_path: string;
  _kb_id: string;
  _lang: KbDocLang;
};

const DEFAULT_KB_ID = "ruta1";
const DEFAULT_KB_LANG: KbDocLang = "en";
const PENDING_TTL_MS = 10 * 60 * 1000;
const DEFAULT_KB_FILE = "ruta1_kb.md";
const PENDING_KB_PATCH_PREFIX = "pending_kb_patch";
const PENDING_KB_PATCH_SOURCE = "kb:update_price";

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
    throw new XError("E_KB_BAD_PARAMS", `Invalid ${field_name}: expected non-empty string`);
  }
  return value.trim();
}

function ensure_optional_string(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalize_lang(value: unknown, fallback: KbDocLang = DEFAULT_KB_LANG): KbDocLang {
  const normalized = typeof value === "string" ? value.trim().toLowerCase() : "";
  return normalized === "es" ? "es" : normalized === "en" ? "en" : fallback;
}

function truncate_text(value: string, max_chars: number): string {
  const trimmed = value.trim();
  if (trimmed.length <= max_chars) return trimmed;
  return `${trimmed.slice(0, max_chars)}...`;
}

function strip_code_fences(text: string): string {
  const trimmed = text.trim();
  if (!trimmed.startsWith("```")) return trimmed;
  const first_break = trimmed.indexOf("\n");
  if (first_break < 0) return trimmed.replace(/^```[a-zA-Z0-9_-]*\s*/, "").replace(/```$/, "").trim();
  const body = trimmed.slice(first_break + 1);
  const closing = body.lastIndexOf("```");
  return (closing >= 0 ? body.slice(0, closing) : body).trim();
}

function safe_json_stringify(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return "{}";
  }
}

function parse_json_object(text: string): Dict {
  const cleaned = strip_code_fences(text);
  try {
    const parsed = JSON.parse(cleaned);
    if (!is_plain_object(parsed) || has_function(parsed)) {
      throw new XError("E_KB_BAD_UPSTREAM", "Expected a JSON-safe object");
    }
    return parsed;
  } catch (err) {
    if (err instanceof XError) throw err;
    throw new XError("E_KB_BAD_UPSTREAM", "Expected valid JSON");
  }
}

function render_replace_preview(kb_id: string, lang: string, content: string): string {
  return [
    `I’m about to REPLACE the knowledge base (kb=${kb_id}, lang=${lang}).`,
    `Preview: ${truncate_text(content, 1000)}`,
    "Reply 'confirm' to proceed or 'cancel'."
  ].join("\n");
}

function render_delete_preview(kb_id: string, lang: string, section_title: string): string {
  return [
    `I’m about to DELETE section '${section_title}' from the knowledge base (kb=${kb_id}, lang=${lang}).`,
    "Reply 'confirm' to proceed or 'cancel'."
  ].join("\n");
}

function render_append_preview(kb_id: string, lang: string, md: string): string {
  return [
    `Proposed KB append (kb=${kb_id}, lang=${lang}):`,
    truncate_text(md, 1000),
    "Reply /kb_apply to publish or /kb_cancel"
  ].join("\n");
}

function render_patch_preview(kb_id: string, lang: string, section_title: string, md: string): string {
  return [
    `Proposed KB patch for '${section_title}' (kb=${kb_id}, lang=${lang}):`,
    truncate_text(md, 1000),
    "Reply /kb_apply to publish or /kb_cancel"
  ].join("\n");
}

function render_append_to_section_preview(kb_id: string, lang: string, section_title: string, line_text: string): string {
  return [
    `Proposed KB append for '${section_title}' (kb=${kb_id}, lang=${lang}):`,
    line_text,
    "Reply 'confirm' to proceed or 'cancel'."
  ].join("\n");
}

function render_remove_from_section_preview(
  kb_id: string,
  lang: string,
  section_title: string,
  removed_line: string
): string {
  return [
    `Proposed KB removal from '${section_title}' (kb=${kb_id}, lang=${lang}):`,
    `- ${truncate_text(removed_line, 1000)}`,
    "Reply 'confirm' to proceed or 'cancel'."
  ].join("\n");
}

function render_price_update_preview(
  kb_id: string,
  lang: string,
  item_name: string,
  before_line: string,
  after_line: string
): string {
  return [
    `Proposed KB price update for '${item_name}' (kb=${kb_id}, lang=${lang}):`,
    `- ${truncate_text(before_line, 1000)}`,
    `+ ${truncate_text(after_line, 1000)}`
  ].join("\n");
}

function escape_regexp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function detect_first_price_token(line: string): RegExpMatchArray | null {
  return line.match(/[¢₡$]\s?\d[\d.,]*/u) ?? line.match(/\b\d[\d.,]*\b/u);
}

function parse_kb_file_hint(value: string): { kb_id?: string; lang?: KbDocLang } {
  const normalized = path.basename(value).trim().toLowerCase();
  if (!normalized) return {};
  let kb_id: string | undefined;
  if (normalized.startsWith("ruta1")) kb_id = "ruta1";
  else if (normalized.endsWith(".md")) kb_id = normalized.replace(/\.md$/u, "").replace(/_kb$/u, "");
  const lang = normalized.includes(".es.") || normalized.endsWith("_es.md") ? "es" : normalized.includes(".en.") || normalized.endsWith("_en.md") ? "en" : undefined;
  return {
    ...(kb_id ? { kb_id } : {}),
    ...(lang ? { lang } : {})
  };
}

function build_runtime_default_kb_file(kb_id: string, lang: KbDocLang): string {
  if (kb_id === DEFAULT_KB_ID && lang === DEFAULT_KB_LANG) return DEFAULT_KB_FILE;
  return `${kb_id}${lang === "en" ? "" : `_${lang}`}_kb.md`;
}

function normalize_pending_proposal(value: unknown): PendingProposal {
  if (!is_plain_object(value)) {
    throw new XError("E_KB_BAD_PARAMS", "Pending proposal is invalid");
  }
  const kind = ensure_non_empty_string(value.kind, "kind") as PendingProposal["kind"];
  const kb_id = ensure_non_empty_string(value.kb_id, "kb_id");
  const lang = normalize_lang(value.lang);
  return {
    kind,
    kb_id,
    lang,
    ...(ensure_optional_string(value.content) ? { content: ensure_optional_string(value.content) } : {}),
    ...(ensure_optional_string(value.section_title) ? { section_title: ensure_optional_string(value.section_title) } : {}),
    ...(ensure_optional_string(value.rationale) ? { rationale: ensure_optional_string(value.rationale) } : {}),
    ...(ensure_optional_string(value.item_name) ? { item_name: ensure_optional_string(value.item_name) } : {}),
    ...(ensure_optional_string(value.from_price) ? { from_price: ensure_optional_string(value.from_price) } : {}),
    ...(ensure_optional_string(value.to_price) ? { to_price: ensure_optional_string(value.to_price) } : {})
  };
}

function sha_preview(text: string): string {
  return createHash("sha1").update(text).digest("hex").slice(0, 10);
}

export class KnowledgeBaseModule extends KnowledgeModule {
  static _name = KNOWLEDGE_MODULE_NAME;

  private _scope: AgentKbXdbScope;
  private _mirror_root: string;
  private _default_seed_path: string;

  constructor(opts: KnowledgeBaseModuleOptions) {
    super({
      _kb_dir: opts._kb_dir,
      _app_id: opts._app_id,
      _env: opts._env
    });
    this._scope = {
      _app_id: typeof opts._app_id === "string" && opts._app_id.trim() ? opts._app_id.trim() : "xbot",
      _env: typeof opts._env === "string" && opts._env.trim() ? opts._env.trim() : "default"
    };
    this._mirror_root = path.resolve(opts._kb_dir, this._scope._app_id, this._scope._env);
    this._default_seed_path = path.resolve(process.cwd(), "ruta1_kb_real.md");
  }

  async _init_on_boot(xcmd: XCommandData) {
    return this.kb_init_on_boot_impl(xcmd);
  }
  async _op_init_on_boot(xcmd: XCommandData) {
    return this.kb_init_on_boot_impl(xcmd);
  }

  async _show(xcmd: XCommandData) {
    return this.show_impl(xcmd);
  }
  async _op_show(xcmd: XCommandData) {
    return this.show_impl(xcmd);
  }

  async _list_files(xcmd: XCommandData) {
    return this.kb_list_files_impl(xcmd);
  }
  async _op_list_files(xcmd: XCommandData) {
    return this.kb_list_files_impl(xcmd);
  }

  async _append(xcmd: XCommandData) {
    return this.append_impl(xcmd);
  }
  async _op_append(xcmd: XCommandData) {
    return this.append_impl(xcmd);
  }

  async _patch_section(xcmd: XCommandData) {
    return this.patch_section_impl(xcmd);
  }
  async _op_patch_section(xcmd: XCommandData) {
    return this.patch_section_impl(xcmd);
  }

  async _append_to_section(xcmd: XCommandData) {
    return this.append_to_section_impl(xcmd);
  }
  async _op_append_to_section(xcmd: XCommandData) {
    return this.append_to_section_impl(xcmd);
  }

  async _remove_from_section(xcmd: XCommandData) {
    return this.remove_from_section_impl(xcmd);
  }
  async _op_remove_from_section(xcmd: XCommandData) {
    return this.remove_from_section_impl(xcmd);
  }

  async _update_price(xcmd: XCommandData) {
    return this.update_price_impl(xcmd);
  }
  async _op_update_price(xcmd: XCommandData) {
    return this.update_price_impl(xcmd);
  }

  async _replace_propose(xcmd: XCommandData) {
    return this.replace_propose_impl(xcmd);
  }
  async _op_replace_propose(xcmd: XCommandData) {
    return this.replace_propose_impl(xcmd);
  }

  async _replace_confirm(xcmd: XCommandData) {
    return this.replace_confirm_impl(xcmd);
  }
  async _op_replace_confirm(xcmd: XCommandData) {
    return this.replace_confirm_impl(xcmd);
  }

  async _delete_section_propose(xcmd: XCommandData) {
    return this.delete_section_propose_impl(xcmd);
  }
  async _op_delete_section_propose(xcmd: XCommandData) {
    return this.delete_section_propose_impl(xcmd);
  }

  async _delete_section_confirm(xcmd: XCommandData) {
    return this.delete_section_confirm_impl(xcmd);
  }
  async _op_delete_section_confirm(xcmd: XCommandData) {
    return this.delete_section_confirm_impl(xcmd);
  }

  async _history(xcmd: XCommandData) {
    return this.history_impl(xcmd);
  }
  async _op_history(xcmd: XCommandData) {
    return this.history_impl(xcmd);
  }

  async _get_current(xcmd: XCommandData) {
    return this.show_impl(xcmd);
  }
  async _op_get_current(xcmd: XCommandData) {
    return this.show_impl(xcmd);
  }

  async _pending_get(xcmd: XCommandData) {
    return this.pending_get_impl(xcmd);
  }
  async _op_pending_get(xcmd: XCommandData) {
    return this.pending_get_impl(xcmd);
  }

  async _cancel_pending(xcmd: XCommandData) {
    return this.cancel_pending_impl(xcmd);
  }
  async _op_cancel_pending(xcmd: XCommandData) {
    return this.cancel_pending_impl(xcmd);
  }

  async _propose(xcmd: XCommandData) {
    return this.propose_impl(xcmd);
  }
  async _op_propose(xcmd: XCommandData) {
    return this.propose_impl(xcmd);
  }

  async _preview(xcmd: XCommandData) {
    return this.preview_impl(xcmd);
  }
  async _op_preview(xcmd: XCommandData) {
    return this.preview_impl(xcmd);
  }

  async _apply(xcmd: XCommandData) {
    return this.apply_impl(xcmd);
  }
  async _op_apply(xcmd: XCommandData) {
    return this.apply_impl(xcmd);
  }

  async _cancel(xcmd: XCommandData) {
    return this.cancel_pending_impl(xcmd);
  }
  async _op_cancel(xcmd: XCommandData) {
    return this.cancel_pending_impl(xcmd);
  }

  async _set_awaiting_input(xcmd: XCommandData) {
    return this.set_awaiting_input_impl(xcmd);
  }
  async _op_set_awaiting_input(xcmd: XCommandData) {
    return this.set_awaiting_input_impl(xcmd);
  }

  private async kb_init_on_boot_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    await init_kb_xdb(this._scope);
    await fs.mkdir(this._mirror_root, { recursive: true });
    await this.seed_default_doc_if_missing(xcmd);
    return super._op_init_on_boot(xcmd);
  }

  private async show_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    const params = this.ensure_kb_params(xcmd._params);
    const target = await this.resolve_kb_target(xcmd, params);
    _xlog.log("[kb-file]", {
      op: "show",
      kb_file: target._kb_file
    });
    const loaded = await this.load_kb_text_for_target(xcmd, target);
    return {
      kb_id: target._kb_id,
      lang: target._lang,
      content: loaded.content,
      updated_at: loaded.updated_at,
      _kb_file: target._kb_file
    };
  }

  private async kb_list_files_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    await fs.mkdir(this._mirror_root, { recursive: true });
    const entries = await fs.readdir(this._mirror_root, { withFileTypes: true });
    const files: Array<{ _name: string; _bytes: number; _mtime: number; _path: string }> = [];
    for (const entry of entries) {
      if (!entry.isFile() || !entry.name.toLowerCase().endsWith(".md")) continue;
      const abs_path = path.resolve(this._mirror_root, entry.name);
      const stat = await fs.stat(abs_path);
      files.push({
        _name: entry.name,
        _bytes: Math.max(0, Math.floor(stat.size)),
        _mtime: Math.floor(stat.mtimeMs),
        _path: abs_path
      });
    }
    files.sort((left, right) => left._name.localeCompare(right._name));
    return {
      _kb_dir: this._mirror_root,
      _files: files
    };
  }

  private async append_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const params = this.ensure_kb_params(xcmd._params);
    const kb_id = ensure_optional_string(params._kb_id) ?? DEFAULT_KB_ID;
    const lang = normalize_lang(params._lang);
    const md = ensure_non_empty_string(params._md, "_md");
    const doc = await kb_append(this._scope, kb_id, lang, md, actor);
    await this.sync_doc_to_mirror(doc, ctx);
    return {
      _ok: true,
      kb_id: doc._kb_id,
      lang: doc._lang,
      updated_at: doc._updated_at
    };
  }

  private async patch_section_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const params = this.ensure_kb_params(xcmd._params);
    const kb_id = ensure_optional_string(params._kb_id) ?? DEFAULT_KB_ID;
    const lang = normalize_lang(params._lang);
    const section_title = ensure_non_empty_string(params._section_title, "_section_title");
    const md = ensure_non_empty_string(params._md, "_md");
    try {
      const doc = await kb_patch_section(this._scope, kb_id, lang, section_title, md, actor);
      await this.sync_doc_to_mirror(doc, ctx);
      return {
        _ok: true,
        kb_id: doc._kb_id,
        lang: doc._lang,
        updated_at: doc._updated_at
      };
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      throw new XError("E_KB_SECTION_NOT_FOUND", message);
    }
  }

  private async append_to_section_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const params = this.ensure_kb_params(xcmd._params);
    const dry_run = typeof params._dry_run === "boolean" ? params._dry_run : true;
    const target = await this.resolve_kb_target(xcmd, params);
    _xlog.log("[kb-file]", {
      op: "append_to_section",
      kb_file: target._kb_file,
      dry_run
    });

    let section_title = ensure_optional_string(params._section_title);
    let line_text = ensure_optional_string(params._line_text);

    if (!dry_run && (!section_title || !line_text)) {
      const pending = await this.get_pending_for_actor(xcmd, actor);
      if (!pending) {
        return {
          _ok: true,
          _changed: false,
          _message: "No pending KB section append. Ask for a preview first."
        };
      }
      const proposal = normalize_pending_proposal(parse_json_object(pending._proposal_json));
      if (proposal.kind !== "kb.append_to_section") {
        return {
          _ok: true,
          _changed: false,
          _message: "No pending KB section append. Ask for a preview first."
        };
      }
      section_title = ensure_non_empty_string(proposal.section_title, "section_title");
      line_text = ensure_non_empty_string(proposal.content, "content");
    } else {
      section_title = ensure_non_empty_string(section_title, "_section_title");
      line_text = ensure_non_empty_string(line_text, "_line_text");
    }

    const current = await this.load_kb_text_for_target(xcmd, target).then((out) => out.content);
    const append_update = this.apply_append_to_section_to_text(current, section_title, line_text);
    if (!append_update._changed) {
      return {
        _ok: true,
        _changed: false,
        _message: append_update._message,
        _kb_file: target._kb_file
      };
    }

    if (dry_run) {
      const pending = await pending_action_create(this._scope, {
        _actor_user_id: actor._actor_user_id,
        _kind: "kb.append_to_section",
        _kb_id: target._kb_id,
        _lang: target._lang,
        _proposal_json: safe_json_stringify({
          kind: "kb.append_to_section",
          kb_id: target._kb_id,
          lang: target._lang,
          section_title,
          content: append_update._normalized_line
        }),
        _expires_at: Date.now() + PENDING_TTL_MS
      });
      return {
        _ok: true,
        _changed: true,
        _preview: render_append_to_section_preview(target._kb_id, target._lang, section_title, append_update._normalized_line),
        _message: "Reply 'confirm' to proceed or 'cancel'.",
        _action_id: pending._action_id,
        _kb_file: target._kb_file
      };
    }

    const doc = await kb_set_doc(this._scope, target._kb_id, target._lang, append_update._next_content, "chat", actor, {
      _op: "kb.append_to_section",
      _summary: `Append to section ${section_title}`,
      _payload: {
        _kb_id: target._kb_id,
        _lang: target._lang,
        _section_title: section_title,
        _line_text: append_update._normalized_line,
        _kb_file: target._kb_file
      },
      _previous_content: current
    });
    const pending = await this.get_pending_for_actor(xcmd, actor);
    if (pending) {
      const proposal = normalize_pending_proposal(parse_json_object(pending._proposal_json));
      if (proposal.kind === "kb.append_to_section") {
        await pending_action_consume(this._scope, pending._action_id);
      }
    }
    await this.sync_doc_to_mirror(doc, ctx, target._kb_file);
    _xem.fire("agent.kb.updated", {
      kb_id: doc._kb_id,
      lang: doc._lang,
      op: "kb.append_to_section",
      actor_user_id: actor._actor_user_id,
      kb_file: target._kb_file
    });
    return {
      _ok: true,
      _changed: true,
      _preview: render_append_to_section_preview(target._kb_id, target._lang, section_title, append_update._normalized_line),
      _message: "saved",
      _kb_file: target._kb_file
    };
  }

  private async remove_from_section_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const params = this.ensure_kb_params(xcmd._params);
    const dry_run = typeof params._dry_run === "boolean" ? params._dry_run : true;
    const target = await this.resolve_kb_target(xcmd, params);
    _xlog.log("[kb-file]", {
      op: "remove_from_section",
      kb_file: target._kb_file,
      dry_run
    });

    let section_title = ensure_optional_string(params._section_title);
    let match_text = ensure_optional_string(params._match_text);

    if (!dry_run && (!section_title || !match_text)) {
      const pending = await this.get_pending_for_actor(xcmd, actor);
      if (!pending) {
        return {
          _ok: false,
          _changed: false,
          _message: "No pending KB line removal. Ask for a preview first."
        };
      }
      const proposal = normalize_pending_proposal(parse_json_object(pending._proposal_json));
      if (proposal.kind !== "kb.remove_from_section") {
        return {
          _ok: false,
          _changed: false,
          _message: "No pending KB line removal. Ask for a preview first."
        };
      }
      section_title = ensure_non_empty_string(proposal.section_title, "section_title");
      match_text = ensure_non_empty_string(proposal.content, "content");
    } else {
      section_title = ensure_non_empty_string(section_title, "_section_title");
      match_text = ensure_non_empty_string(match_text, "_match_text");
    }

    const current = await this.load_kb_text_for_target(xcmd, target).then((out) => out.content);
    const removal = this.apply_remove_from_section_to_text(current, section_title, match_text);
    if (!removal._changed) {
      return {
        _ok: false,
        _changed: false,
        _message: removal._message,
        _kb_file: target._kb_file
      };
    }

    if (dry_run) {
      const pending = await pending_action_create(this._scope, {
        _actor_user_id: actor._actor_user_id,
        _kind: "kb.remove_from_section",
        _kb_id: target._kb_id,
        _lang: target._lang,
        _proposal_json: safe_json_stringify({
          kind: "kb.remove_from_section",
          kb_id: target._kb_id,
          lang: target._lang,
          section_title,
          content: match_text
        }),
        _expires_at: Date.now() + PENDING_TTL_MS
      });
      return {
        _ok: true,
        _changed: true,
        _preview: render_remove_from_section_preview(target._kb_id, target._lang, section_title, removal._removed_line),
        _message: "Reply 'confirm' to proceed or 'cancel'.",
        _action_id: pending._action_id,
        _kb_file: target._kb_file
      };
    }

    const doc = await kb_set_doc(this._scope, target._kb_id, target._lang, removal._next_content, "chat", actor, {
      _op: "kb.remove_from_section",
      _summary: `Remove line from section ${section_title}`,
      _payload: {
        _kb_id: target._kb_id,
        _lang: target._lang,
        _section_title: section_title,
        _match_text: match_text,
        _kb_file: target._kb_file
      },
      _previous_content: current
    });
    const pending = await this.get_pending_for_actor(xcmd, actor);
    if (pending) {
      const proposal = normalize_pending_proposal(parse_json_object(pending._proposal_json));
      if (proposal.kind === "kb.remove_from_section") {
        await pending_action_consume(this._scope, pending._action_id);
      }
    }
    await this.sync_doc_to_mirror(doc, ctx, target._kb_file);
    _xem.fire("agent.kb.updated", {
      kb_id: doc._kb_id,
      lang: doc._lang,
      op: "kb.remove_from_section",
      actor_user_id: actor._actor_user_id,
      kb_file: target._kb_file
    });
    return {
      _ok: true,
      _changed: true,
      _preview: render_remove_from_section_preview(target._kb_id, target._lang, section_title, removal._removed_line),
      _message: "saved",
      _kb_file: target._kb_file
    };
  }

  private async update_price_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const params = this.ensure_kb_params(xcmd._params);
    const dry_run = typeof params._dry_run === "boolean" ? params._dry_run : true;
    const target = await this.resolve_kb_target(xcmd, params);
    _xlog.log("[kb-file]", {
      op: "update_price",
      kb_file: target._kb_file,
      dry_run
    });

    let item_name = ensure_optional_string(params._item_name);
    let from_price = ensure_optional_string(params._from_price);
    let to_price = ensure_optional_string(params._to_price);

    if (!dry_run && (!item_name || !to_price)) {
      const pending = this.get_pending_price_update(ctx, actor._actor_user_id);
      if (!pending) {
        return {
          _ok: true,
          _changed: false,
          _message: "No pending KB price update. Ask for a preview first."
        };
      }
      item_name = ensure_non_empty_string(pending._item_name, "item_name");
      to_price = ensure_non_empty_string(pending._to_price, "to_price");
      from_price = ensure_optional_string(pending._from_price);
    } else {
      item_name = ensure_non_empty_string(item_name, "_item_name");
      to_price = ensure_non_empty_string(to_price, "_to_price");
    }

    const current = await this.load_kb_text_for_target(xcmd, target).then((out) => out.content);
    const price_update = this.apply_price_update_to_text(current, item_name, from_price, to_price);
    if (!price_update._changed) {
      return {
        _ok: true,
        _changed: false,
        _message: price_update._message,
        _kb_file: target._kb_file
      };
    }

    if (dry_run) {
      this.set_pending_price_update(ctx, actor._actor_user_id, {
        _item_name: item_name,
        ...(from_price ? { _from_price: from_price } : {}),
        _to_price: to_price,
        _kb_file: target._kb_file,
        _kb_id: target._kb_id,
        _lang: target._lang,
        _generated_at: Date.now(),
        _preview: render_price_update_preview(target._kb_id, target._lang, item_name, price_update._before_line, price_update._after_line)
      });
      return {
        _ok: true,
        _changed: true,
        _preview: render_price_update_preview(target._kb_id, target._lang, item_name, price_update._before_line, price_update._after_line),
        _message: "Reply 'confirm', 'apply', or 'yes' to publish, or 'cancel'.",
        _kb_file: target._kb_file
      };
    }

    const doc = await kb_set_doc(this._scope, target._kb_id, target._lang, price_update._next_content, "chat", actor, {
      _op: "kb.update_price",
      _summary: `Update the price of ${item_name}`,
      _payload: {
        _kb_id: target._kb_id,
        _lang: target._lang,
        _item_name: item_name,
        ...(from_price ? { _from_price: from_price } : {}),
        _to_price: to_price,
        _kb_file: target._kb_file
      },
      _previous_content: current
    });
    await this.sync_doc_to_mirror(doc, ctx, target._kb_file);
    this.clear_pending_price_update(ctx, actor._actor_user_id);
    _xem.fire("agent.kb.updated", {
      kb_id: doc._kb_id,
      lang: doc._lang,
      op: "kb.update_price",
      actor_user_id: actor._actor_user_id,
      kb_file: target._kb_file
    });
    return {
      _ok: true,
      _changed: true,
      _preview: render_price_update_preview(target._kb_id, target._lang, item_name, price_update._before_line, price_update._after_line),
      _message: "saved",
      _kb_file: target._kb_file
    };
  }

  private async replace_propose_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const params = this.ensure_kb_params(xcmd._params);
    const kb_id = ensure_optional_string(params._kb_id) ?? DEFAULT_KB_ID;
    const lang = normalize_lang(params._lang);
    const content = ensure_non_empty_string(params._content, "_content");
    const action = await pending_action_create(this._scope, {
      _actor_user_id: actor._actor_user_id,
      _kind: "kb.replace",
      _kb_id: kb_id,
      _lang: lang,
      _proposal_json: safe_json_stringify({
        kind: "kb.replace",
        kb_id,
        lang,
        content
      }),
      _expires_at: Date.now() + PENDING_TTL_MS
    });
    return {
      action_id: action._action_id,
      preview: render_replace_preview(kb_id, lang, content)
    };
  }

  private async replace_confirm_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const pending = await this.consume_pending_for_actor(xcmd, actor, "kb.replace");
    const proposal = normalize_pending_proposal(parse_json_object(pending._proposal_json));
    const doc = await kb_set_doc(this._scope, proposal.kb_id, proposal.lang, proposal.content ?? "", "chat", actor, {
      _op: "kb.replace",
      _summary: "Replace knowledge base",
      _payload: proposal
    });
    await this.sync_doc_to_mirror(doc, ctx);
    return {
      _ok: true,
      kb_id: doc._kb_id,
      lang: doc._lang,
      updated_at: doc._updated_at
    };
  }

  private async delete_section_propose_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const params = this.ensure_kb_params(xcmd._params);
    const kb_id = ensure_optional_string(params._kb_id) ?? DEFAULT_KB_ID;
    const lang = normalize_lang(params._lang);
    const section_title = ensure_non_empty_string(params._section_title, "_section_title");
    const action = await pending_action_create(this._scope, {
      _actor_user_id: actor._actor_user_id,
      _kind: "kb.delete_section",
      _kb_id: kb_id,
      _lang: lang,
      _proposal_json: safe_json_stringify({
        kind: "kb.delete_section",
        kb_id,
        lang,
        section_title
      }),
      _expires_at: Date.now() + PENDING_TTL_MS
    });
    return {
      action_id: action._action_id,
      preview: render_delete_preview(kb_id, lang, section_title)
    };
  }

  private async delete_section_confirm_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const pending = await this.consume_pending_for_actor(xcmd, actor, "kb.delete_section");
    const proposal = normalize_pending_proposal(parse_json_object(pending._proposal_json));
    if (!proposal.section_title) {
      throw new XError("E_KB_BAD_PARAMS", "Pending delete action is missing section_title");
    }
    try {
      const doc = await kb_delete_section(this._scope, proposal.kb_id, proposal.lang, proposal.section_title, actor);
      await this.sync_doc_to_mirror(doc, ctx);
      return {
        _ok: true,
        kb_id: doc._kb_id,
        lang: doc._lang,
        updated_at: doc._updated_at
      };
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      throw new XError("E_KB_SECTION_NOT_FOUND", message);
    }
  }

  private async history_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    const params = this.ensure_kb_params(xcmd._params);
    const since_ts = this.parse_optional_positive_int(params._since_ts ?? params.since_ts) ?? 0;
    const limit = this.parse_optional_positive_int(params._limit ?? params.limit) ?? 50;
    const items = await kb_list_audit(this._scope, since_ts, Math.min(limit, 50));
    return {
      _items: items.map((item) => ({
        _audit_id: item._audit_id,
        _actor_user_id: item._actor_user_id,
        _actor_role: item._actor_role,
        _op: item._op,
        _kb_id: item._kb_id,
        _lang: item._lang,
        _summary: item._summary ?? "",
        _diff_preview: item._diff_preview ?? "",
        _payload_json: item._payload_json ?? "",
        _created_at: item._created_at
      }))
    };
  }

  private async pending_get_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const params = this.ensure_kb_params(xcmd._params);
    const action_id = ensure_optional_string(params._action_id);
    const pending = await pending_action_get(this._scope, action_id, actor._actor_user_id);
    if (!pending || pending._expires_at <= Date.now()) {
      if (pending) {
        await pending_action_consume(this._scope, pending._action_id);
      }
      const pending_price = this.get_pending_price_update(ctx, actor._actor_user_id);
      if (!pending_price) {
        return { _ok: true, _pending: false };
      }
      return {
        _ok: true,
        _pending: true,
        _action_id: this.pending_price_latest_key(this.pending_price_scope_id(ctx, actor._actor_user_id)),
        _kind: "kb.update_price",
        _kb_id: pending_price._kb_id,
        _lang: pending_price._lang,
        _kb_file: pending_price._kb_file,
        _generated_at: pending_price._generated_at
      };
    }
    return {
      _ok: true,
      _pending: true,
      _action_id: pending._action_id,
      _kind: pending._kind,
      _kb_id: pending._kb_id,
      _lang: pending._lang,
      _expires_at: pending._expires_at
    };
  }

  private async cancel_pending_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const params = this.ensure_kb_params(xcmd._params);
    const action_id = ensure_optional_string(params._action_id);
    const pending = await pending_action_get(this._scope, action_id, actor._actor_user_id);
    if (!pending) {
      const cleared = this.clear_pending_price_update(ctx, actor._actor_user_id);
      if (cleared) {
        return { _ok: true, _reply_text: "Pending KB price update canceled." };
      }
      return { _ok: true, _reply_text: "No pending action." };
    }
    await pending_action_consume(this._scope, pending._action_id);
    return { _ok: true, _reply_text: "Pending action canceled." };
  }

  private async propose_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const params = this.ensure_kb_params(xcmd._params);
    const text = ensure_non_empty_string(params._text, "_text");
    const interpreted = await this.build_structured_kb_proposal(text);
    const kb_id = ensure_optional_string(params._kb_id) ?? DEFAULT_KB_ID;
    const lang = normalize_lang(params._lang ?? interpreted.lang);
    if (interpreted.action === "append") {
      const pending = await pending_action_create(this._scope, {
        _actor_user_id: actor._actor_user_id,
        _kind: "kb.append",
        _kb_id: kb_id,
        _lang: lang,
        _proposal_json: safe_json_stringify({
          kind: "kb.append",
          kb_id,
          lang,
          content: interpreted.content,
          rationale: interpreted.rationale
        }),
        _expires_at: Date.now() + PENDING_TTL_MS
      });
      return {
        _ok: true,
        _session_id: pending._action_id,
        _awaiting_input: false,
        _has_pending: true,
        _preview_text: render_append_preview(kb_id, lang, interpreted.content ?? "")
      };
    }
    if (interpreted.action === "patch_section") {
      const pending = await pending_action_create(this._scope, {
        _actor_user_id: actor._actor_user_id,
        _kind: "kb.patch_section",
        _kb_id: kb_id,
        _lang: lang,
        _proposal_json: safe_json_stringify({
          kind: "kb.patch_section",
          kb_id,
          lang,
          content: interpreted.content,
          section_title: interpreted.section_title,
          rationale: interpreted.rationale
        }),
        _expires_at: Date.now() + PENDING_TTL_MS
      });
      return {
        _ok: true,
        _session_id: pending._action_id,
        _awaiting_input: false,
        _has_pending: true,
        _preview_text: render_patch_preview(kb_id, lang, interpreted.section_title ?? "", interpreted.content ?? "")
      };
    }
    const out = await this.replace_propose_impl({
      ...xcmd,
      _params: {
        _kb_id: kb_id,
        _lang: lang,
        _content: interpreted.content ?? ""
      }
    });
    return {
      _ok: true,
      _session_id: is_plain_object(out) && typeof out.action_id === "string" ? out.action_id : "",
      _awaiting_input: false,
      _has_pending: true,
      _preview_text: is_plain_object(out) && typeof out.preview === "string" ? out.preview : "Pending KB replacement."
    };
  }

  private async preview_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const pending = await pending_action_get(this._scope, ensure_optional_string(this.ensure_kb_params(xcmd._params)._session_id), actor._actor_user_id);
    if (!pending || pending._expires_at <= Date.now()) {
      if (pending) await pending_action_consume(this._scope, pending._action_id);
      const pending_price = this.get_pending_price_update(ctx, actor._actor_user_id);
      if (!pending_price) {
        return {
          _ok: true,
          _awaiting_input: false,
          _has_pending: false,
          _preview_text: "No pending KB proposal."
        };
      }
      return {
        _ok: true,
        _awaiting_input: false,
        _has_pending: true,
        _preview_text: typeof pending_price._preview === "string" && pending_price._preview.trim().length > 0
          ? pending_price._preview
          : "Pending KB price update."
      };
    }
    const proposal = normalize_pending_proposal(parse_json_object(pending._proposal_json));
    if (proposal.kind === "kb.awaiting_input") {
      return {
        _ok: true,
        _awaiting_input: true,
        _has_pending: false,
        _preview_text: "KB update mode is active. Send the update text, or use /kb_cancel."
      };
    }
    if (proposal.kind === "kb.append") {
      return {
        _ok: true,
        _awaiting_input: false,
        _has_pending: true,
        _preview_text: render_append_preview(proposal.kb_id, proposal.lang, proposal.content ?? "")
      };
    }
    if (proposal.kind === "kb.patch_section") {
      return {
        _ok: true,
        _awaiting_input: false,
        _has_pending: true,
        _preview_text: render_patch_preview(proposal.kb_id, proposal.lang, proposal.section_title ?? "", proposal.content ?? "")
      };
    }
    if (proposal.kind === "kb.append_to_section") {
      return {
        _ok: true,
        _awaiting_input: false,
        _has_pending: true,
        _preview_text: render_append_to_section_preview(
          proposal.kb_id,
          proposal.lang,
          proposal.section_title ?? "",
          proposal.content ?? ""
        )
      };
    }
    if (proposal.kind === "kb.remove_from_section") {
      return {
        _ok: true,
        _awaiting_input: false,
        _has_pending: true,
        _preview_text: render_remove_from_section_preview(
          proposal.kb_id,
          proposal.lang,
          proposal.section_title ?? "",
          proposal.content ?? ""
        )
      };
    }
    if (proposal.kind === "kb.update_price") {
      return {
        _ok: true,
        _awaiting_input: false,
        _has_pending: true,
        _preview_text: render_price_update_preview(
          proposal.kb_id,
          proposal.lang,
          proposal.item_name ?? "",
          proposal.from_price ?? "(detected automatically)",
          proposal.to_price ?? ""
        )
      };
    }
    if (proposal.kind === "kb.delete_section") {
      return {
        _ok: true,
        _awaiting_input: false,
        _has_pending: true,
        _preview_text: render_delete_preview(proposal.kb_id, proposal.lang, proposal.section_title ?? "")
      };
    }
    return {
      _ok: true,
      _awaiting_input: false,
      _has_pending: true,
      _preview_text: render_replace_preview(proposal.kb_id, proposal.lang, proposal.content ?? "")
    };
  }

  private async apply_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const pending = await this.get_pending_for_actor(xcmd, actor);
    if (!pending) {
      const pending_price = this.get_pending_price_update(ctx, actor._actor_user_id);
      if (pending_price) {
        const out = await this.update_price_impl({
          ...xcmd,
          _params: {
            _dry_run: false,
            _ctx: this.forward_ctx(ctx)
          }
        });
        return {
          _ok: true,
          _reply_text: is_plain_object(out) && typeof out._message === "string" ? out._message : "saved"
        };
      }
      throw new XError("E_KB_NO_PENDING", "No pending KB proposal");
    }
    const proposal = normalize_pending_proposal(parse_json_object(pending._proposal_json));
    if (proposal.kind === "kb.append") {
      await pending_action_consume(this._scope, pending._action_id);
      const doc = await kb_append(this._scope, proposal.kb_id, proposal.lang, proposal.content ?? "", actor);
      await this.sync_doc_to_mirror(doc, ctx);
      return { _ok: true, _reply_text: `KB appended ✅ hash=${sha_preview(doc._content)}` };
    }
    if (proposal.kind === "kb.patch_section") {
      await pending_action_consume(this._scope, pending._action_id);
      const doc = await kb_patch_section(
        this._scope,
        proposal.kb_id,
        proposal.lang,
        ensure_non_empty_string(proposal.section_title, "section_title"),
        ensure_non_empty_string(proposal.content, "content"),
        actor
      );
      await this.sync_doc_to_mirror(doc, ctx);
      return { _ok: true, _reply_text: `KB section updated ✅ hash=${sha_preview(doc._content)}` };
    }
    if (proposal.kind === "kb.append_to_section") {
      const out = await this.append_to_section_impl({
        ...xcmd,
        _params: {
          _dry_run: false
        }
      });
      return {
        _ok: true,
        _reply_text: is_plain_object(out) && typeof out._message === "string" ? out._message : "KB section appended ✅"
      };
    }
    if (proposal.kind === "kb.remove_from_section") {
      const out = await this.remove_from_section_impl({
        ...xcmd,
        _params: {
          _dry_run: false
        }
      });
      return {
        _ok: true,
        _reply_text: is_plain_object(out) && typeof out._message === "string" ? out._message : "KB line removed ✅"
      };
    }
    if (proposal.kind === "kb.update_price") {
      const out = await this.update_price_impl({
        ...xcmd,
        _params: {
          _dry_run: false
        }
      });
      return {
        _ok: true,
        _reply_text: is_plain_object(out) && typeof out._message === "string" ? out._message : "KB price updated ✅"
      };
    }
    if (proposal.kind === "kb.replace") {
      await this.replace_confirm_impl({
        ...xcmd,
        _params: {
          _action_id: pending._action_id
        }
      });
      return { _ok: true, _reply_text: "KB replaced ✅" };
    }
    if (proposal.kind === "kb.delete_section") {
      await this.delete_section_confirm_impl({
        ...xcmd,
        _params: {
          _action_id: pending._action_id
        }
      });
      return { _ok: true, _reply_text: "KB section deleted ✅" };
    }
    throw new XError("E_KB_NO_PENDING", "No applicable pending KB proposal");
  }

  private async set_awaiting_input_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    const pending = await pending_action_create(this._scope, {
      _actor_user_id: actor._actor_user_id,
      _kind: "kb.awaiting_input",
      _kb_id: DEFAULT_KB_ID,
      _lang: DEFAULT_KB_LANG,
      _proposal_json: safe_json_stringify({
        kind: "kb.awaiting_input",
        kb_id: DEFAULT_KB_ID,
        lang: DEFAULT_KB_LANG
      }),
      _expires_at: Date.now() + PENDING_TTL_MS
    });
    return {
      _ok: true,
      _session_id: pending._action_id,
      _reply_text: "KB update mode is active. Send the update text, or use /kb_cancel."
    };
  }

  private ensure_kb_params(value: unknown): Dict {
    if (value === undefined || value === null) return {};
    if (!is_plain_object(value) || has_function(value)) {
      throw new XError("E_KB_BAD_PARAMS", "params must be a JSON-safe object");
    }
    return value;
  }

  private require_admin_actor(ctx: AgentCommandCtx): KbActor {
    const role = ensure_optional_string(ctx.actor?.role);
    const user_id = ensure_optional_string(ctx.actor?.user_id);
    if (!user_id || (role !== "admin" && role !== "owner")) {
      throw new XError("E_KB_FORBIDDEN", "Admin access required");
    }
    return {
      _actor_user_id: user_id,
      _actor_role: role
    };
  }

  private parse_optional_positive_int(value: unknown): number | undefined {
    if (value === undefined || value === null) return undefined;
    const parsed = Number.parseInt(String(value), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) return undefined;
    return Math.floor(parsed);
  }

  private async resolve_doc(kb_id: string, lang: KbDocLang): Promise<PersistedKbDocRecord | undefined> {
    const exact = await kb_get_doc(this._scope, kb_id, lang);
    if (exact) return exact;
    if (lang !== "en") {
      const fallback = await kb_get_doc(this._scope, kb_id, "en");
      if (fallback) return fallback;
    }
    return undefined;
  }

  private apply_price_update_to_text(
    content: string,
    item_name: string,
    from_price: string | undefined,
    to_price: string
  ): {
    _changed: boolean;
    _message: string;
    _before_line: string;
    _after_line: string;
    _next_content: string;
  } {
    const lines = content.split(/\r?\n/g);
    const target = item_name.trim().toLowerCase();
    let line_index = -1;
    for (let idx = 0; idx < lines.length; idx += 1) {
      if (lines[idx].toLowerCase().includes(target)) {
        line_index = idx;
        break;
      }
    }
    if (line_index < 0) {
      return {
        _changed: false,
        _message: `No KB line found for '${item_name}'.`,
        _before_line: "",
        _after_line: "",
        _next_content: content
      };
    }

    const before_line = lines[line_index];
    let after_line = before_line;
    if (from_price) {
      const matcher = new RegExp(escape_regexp(from_price), "u");
      if (!matcher.test(before_line)) {
        return {
          _changed: false,
          _message: `Could not find '${from_price}' on the '${item_name}' line.`,
          _before_line: before_line,
          _after_line: before_line,
          _next_content: content
        };
      }
      after_line = before_line.replace(matcher, to_price);
    } else {
      const token_match = detect_first_price_token(before_line);
      if (!token_match || !token_match[0]) {
        return {
          _changed: false,
          _message: `Could not detect a price token on the '${item_name}' line.`,
          _before_line: before_line,
          _after_line: before_line,
          _next_content: content
        };
      }
      after_line = `${before_line.slice(0, token_match.index ?? 0)}${to_price}${before_line.slice((token_match.index ?? 0) + token_match[0].length)}`;
    }

    if (after_line === before_line) {
      return {
        _changed: false,
        _message: `No change detected for '${item_name}'.`,
        _before_line: before_line,
        _after_line: after_line,
        _next_content: content
      };
    }

    const next_lines = [...lines];
    next_lines[line_index] = after_line;
    return {
      _changed: true,
      _message: `Preview ready for '${item_name}'.`,
      _before_line: before_line,
      _after_line: after_line,
      _next_content: next_lines.join("\n")
    };
  }

  private apply_append_to_section_to_text(
    content: string,
    section_title: string,
    line_text: string
  ): {
    _changed: boolean;
    _message: string;
    _normalized_line: string;
    _next_content: string;
  } {
    const section = extract_section(content, normalize_heading_query(section_title));
    const normalized_line = this.normalize_section_line_text(line_text);
    if (!section.found || !section.content) {
      return {
        _changed: false,
        _message: `KB section not found: ${section_title}`,
        _normalized_line: normalized_line,
        _next_content: content
      };
    }
    const current_section = section.content;
    const trimmed_section = current_section.replace(/\s*$/u, "");
    const next_section = `${trimmed_section}\n${normalized_line}`;
    if (next_section === current_section) {
      return {
        _changed: false,
        _message: `No change detected for section '${section_title}'.`,
        _normalized_line: normalized_line,
        _next_content: content
      };
    }
    const next_content = content.replace(current_section, next_section);
    return {
      _changed: true,
      _message: `Preview ready for section '${section_title}'.`,
      _normalized_line: normalized_line,
      _next_content: next_content
    };
  }

  private apply_remove_from_section_to_text(
    content: string,
    section_title: string,
    match_text: string
  ): {
    _changed: boolean;
    _message: string;
    _removed_line: string;
    _next_content: string;
  } {
    const section = extract_section(content, normalize_heading_query(section_title));
    const match = match_text.trim().toLowerCase();
    if (!section.found || !section.content) {
      return {
        _changed: false,
        _message: `KB section not found: ${section_title}`,
        _removed_line: "",
        _next_content: content
      };
    }
    if (!match) {
      throw new XError("E_KB_BAD_PARAMS", "Invalid _match_text: expected non-empty string");
    }
    const lines = section.content.split(/\r?\n/g);
    let removed_line = "";
    let removed = false;
    const next_lines: string[] = [];
    for (let idx = 0; idx < lines.length; idx += 1) {
      const line = lines[idx];
      if (!removed && idx > 0 && line.toLowerCase().includes(match)) {
        removed_line = line;
        removed = true;
        continue;
      }
      next_lines.push(line);
    }
    if (!removed) {
      return {
        _changed: false,
        _message: "line_not_found",
        _removed_line: "",
        _next_content: content
      };
    }
    const next_section = next_lines.join("\n");
    if (next_section === section.content) {
      return {
        _changed: false,
        _message: `No change detected for section '${section_title}'.`,
        _removed_line: "",
        _next_content: content
      };
    }
    return {
      _changed: true,
      _message: `Preview ready for section '${section_title}'.`,
      _removed_line: removed_line,
      _next_content: content.replace(section.content, next_section)
    };
  }

  private normalize_section_line_text(line_text: string): string {
    const trimmed = line_text.trim();
    if (!trimmed) {
      throw new XError("E_KB_BAD_PARAMS", "Invalid _line_text: expected non-empty string");
    }
    return trimmed.startsWith("-") ? trimmed : `- ${trimmed}`;
  }

  private doc_file_path(kb_id: string, lang: string): string {
    return path.resolve(this._mirror_root, `${kb_id}.${lang}.md`);
  }

  private async sync_doc_to_mirror(doc: PersistedKbDocRecord, ctx: AgentCommandCtx, kb_file?: string): Promise<void> {
    await fs.mkdir(this._mirror_root, { recursive: true });
    const target = await this.resolve_kb_target_from_ctx(ctx, kb_file ? { _kb_file: kb_file } : {}, doc._kb_id, doc._lang as KbDocLang);
    await fs.mkdir(path.dirname(target._abs_path), { recursive: true });
    await fs.writeFile(target._abs_path, doc._content, "utf8");
    const legacy_path = this.doc_file_path(doc._kb_id, doc._lang);
    if (legacy_path !== target._abs_path) {
      await fs.writeFile(legacy_path, doc._content, "utf8");
    }
    if (typeof ctx.kernel_cap === "string" && ctx.kernel_cap.trim().length > 0) {
      await _x.execute({
        _module: KNOWLEDGE_MODULE_NAME,
        _op: "reload",
        _params: {
          _ctx: {
            kernel_cap: ctx.kernel_cap
          }
        }
      });
    }
  }

  private async seed_default_doc_if_missing(xcmd: XCommandData): Promise<void> {
    const existing = await kb_get_doc(this._scope, DEFAULT_KB_ID, DEFAULT_KB_LANG);
    if (existing) {
      await this.sync_doc_to_mirror(existing, readCommandCtx(xcmd));
      return;
    }
    const seed_path = await this.resolve_seed_path(xcmd);
    if (!seed_path) return;
    try {
      const content = await fs.readFile(seed_path, "utf8");
      const doc = await kb_set_doc(
        this._scope,
        DEFAULT_KB_ID,
        DEFAULT_KB_LANG,
        content,
        `file:${path.basename(seed_path)}`,
        {
          _actor_user_id: "system",
          _actor_role: "owner"
        },
        {
          _op: "kb.set",
          _summary: "Seed KB from file",
          _payload: { _seed_path: seed_path }
        }
      );
      await this.sync_doc_to_mirror(doc, readCommandCtx(xcmd));
    } catch (err) {
      const code = typeof err === "object" && err !== null ? (err as any).code : undefined;
      if (code === "ENOENT") return;
      throw err;
    }
  }

  private async resolve_seed_path(xcmd: XCommandData): Promise<string | undefined> {
    const ctx = readCommandCtx(xcmd);
    try {
      const out = await _x.execute({
        _module: SETTINGS_MODULE_NAME,
        _op: "get",
        _params: {
          key: "kb.source_path",
          _ctx: this.forward_ctx(ctx)
        }
      });
      const value = is_plain_object(out) ? ensure_optional_string(out.value) : undefined;
      if (value) return path.resolve(process.cwd(), value);
    } catch {
      // ignore and fall back
    }
    try {
      await fs.access(this._default_seed_path);
      return this._default_seed_path;
    } catch {
      return undefined;
    }
  }

  private async resolve_kb_target(
    xcmd: XCommandData,
    params: Dict,
    fallback_kb_id = DEFAULT_KB_ID,
    fallback_lang: KbDocLang = DEFAULT_KB_LANG
  ): Promise<ResolvedKbTarget> {
    return this.resolve_kb_target_from_ctx(readCommandCtx(xcmd), params, fallback_kb_id, fallback_lang);
  }

  private async resolve_kb_target_from_ctx(
    ctx: AgentCommandCtx,
    params: Dict,
    fallback_kb_id = DEFAULT_KB_ID,
    fallback_lang: KbDocLang = DEFAULT_KB_LANG
  ): Promise<ResolvedKbTarget> {
    const explicit_file = ensure_optional_string(params._kb_file);
    const settings_file = explicit_file ? undefined : await this.read_default_kb_file(ctx);
    const raw_file = explicit_file ?? settings_file ?? build_runtime_default_kb_file(fallback_kb_id, fallback_lang);
    const abs_path = path.isAbsolute(raw_file) ? raw_file : path.resolve(this._mirror_root, raw_file);
    const file_hint = parse_kb_file_hint(raw_file);
    const kb_id = file_hint.kb_id ?? ensure_optional_string(params._kb_id) ?? fallback_kb_id;
    const lang = normalize_lang(file_hint.lang ?? params._lang ?? fallback_lang);
    const legacy_abs_path = this.doc_file_path(kb_id, lang);
    return {
      _kb_file: path.isAbsolute(raw_file) ? raw_file : raw_file,
      _abs_path: abs_path,
      _legacy_abs_path: legacy_abs_path,
      _kb_id: kb_id,
      _lang: lang
    };
  }

  private async read_default_kb_file(ctx: AgentCommandCtx): Promise<string | undefined> {
    try {
      const out = await _x.execute({
        _module: SETTINGS_MODULE_NAME,
        _op: "get",
        _params: {
          key: "kb.default_file",
          _ctx: this.forward_ctx(ctx)
        }
      });
      return is_plain_object(out) ? ensure_optional_string(out.value) : undefined;
    } catch {
      return undefined;
    }
  }

  private async load_kb_text_for_target(
    xcmd: XCommandData,
    target: ResolvedKbTarget
  ): Promise<{ content: string; updated_at: number }> {
    const disk = await this.try_read_file(target._abs_path);
    if (disk) {
      return disk;
    }
    if (target._legacy_abs_path !== target._abs_path) {
      const legacy = await this.try_read_file(target._legacy_abs_path);
      if (legacy) {
        return legacy;
      }
    }
    const existing = await this.resolve_doc(target._kb_id, target._lang);
    if (existing && existing._content.trim().length > 0) {
      return {
        content: existing._content,
        updated_at: existing._updated_at
      };
    }
    const fallback = await super._op_get_text({
      ...xcmd,
      _params: {
        _max_chars: 12000,
        _ctx: this.forward_ctx(readCommandCtx(xcmd))
      }
    } as XCommandData);
    return {
      content: is_plain_object(fallback) && typeof fallback._text === "string" ? fallback._text : "",
      updated_at:
        is_plain_object(fallback) && typeof fallback._updated_at === "number" && Number.isFinite(fallback._updated_at)
          ? Math.floor(fallback._updated_at)
          : 0
    };
  }

  private async try_read_file(file_path: string): Promise<{ content: string; updated_at: number } | undefined> {
    try {
      const [content, stat] = await Promise.all([fs.readFile(file_path, "utf8"), fs.stat(file_path)]);
      return {
        content,
        updated_at: Math.floor(stat.mtimeMs)
      };
    } catch (err) {
      const code = typeof err === "object" && err !== null ? (err as { code?: unknown }).code : undefined;
      if (code === "ENOENT") return undefined;
      throw err;
    }
  }

  private pending_price_scope_id(ctx: AgentCommandCtx, actor_user_id: string): string {
    const sid = ensure_optional_string(ctx._sid);
    return sid ?? actor_user_id;
  }

  private pending_price_key(scope_id: string, request_id: string): string {
    return `${PENDING_KB_PATCH_PREFIX}::${scope_id}::${request_id}`;
  }

  private pending_price_latest_key(scope_id: string): string {
    return `${PENDING_KB_PATCH_PREFIX}::${scope_id}::latest`;
  }

  private set_pending_price_update(
    ctx: AgentCommandCtx,
    actor_user_id: string,
    payload: Record<string, unknown>
  ): string {
    const scope_id = this.pending_price_scope_id(ctx, actor_user_id);
    const request_id = ensure_optional_string(ctx._wid) ?? "manual";
    const key = this.pending_price_key(scope_id, request_id);
    _xd.set(key, { ...payload }, { source: PENDING_KB_PATCH_SOURCE });
    _xd.set(this.pending_price_latest_key(scope_id), key, { source: PENDING_KB_PATCH_SOURCE });
    return key;
  }

  private get_pending_price_update(
    ctx: AgentCommandCtx,
    actor_user_id: string
  ): Record<string, unknown> | undefined {
    const scope_id = this.pending_price_scope_id(ctx, actor_user_id);
    const latest_ref = _xd.get(this.pending_price_latest_key(scope_id));
    if (typeof latest_ref !== "string" || !latest_ref.trim()) return undefined;
    const payload = _xd.get(latest_ref);
    if (!is_plain_object(payload) || has_function(payload)) return undefined;
    return { ...payload };
  }

  private clear_pending_price_update(ctx: AgentCommandCtx, actor_user_id: string): boolean {
    const scope_id = this.pending_price_scope_id(ctx, actor_user_id);
    const latest_key = this.pending_price_latest_key(scope_id);
    const latest_ref = _xd.get(latest_key);
    if (typeof latest_ref === "string" && latest_ref.trim()) {
      _xd.delete(latest_ref, { source: PENDING_KB_PATCH_SOURCE });
      _xd.delete(latest_key, { source: PENDING_KB_PATCH_SOURCE });
      return true;
    }
    return false;
  }

  private forward_ctx(ctx: AgentCommandCtx): Dict {
    const out: Dict = {};
    if (typeof ctx.kernel_cap === "string" && ctx.kernel_cap.trim().length > 0) {
      out.kernel_cap = ctx.kernel_cap;
    }
    if (ctx.actor && is_plain_object(ctx.actor)) {
      out.actor = { ...ctx.actor };
    }
    return out;
  }

  private async get_pending_for_actor(
    xcmd: XCommandData,
    actor: KbActor
  ): Promise<PersistedAdminPendingActionRecord | undefined> {
    const params = this.ensure_kb_params(xcmd._params);
    const action_id = ensure_optional_string(params._action_id) ?? ensure_optional_string(params._session_id);
    const pending = await pending_action_get(this._scope, action_id, actor._actor_user_id);
    if (!pending) return undefined;
    if (pending._expires_at <= Date.now()) {
      await pending_action_consume(this._scope, pending._action_id);
      throw new XError("E_KB_PENDING_EXPIRED", "Pending action expired. Please retry.");
    }
    return pending;
  }

  private async consume_pending_for_actor(
    xcmd: XCommandData,
    actor: KbActor,
    expected_kind: PendingProposal["kind"]
  ): Promise<PersistedAdminPendingActionRecord> {
    const pending = await this.get_pending_for_actor(xcmd, actor);
    if (!pending) {
      throw new XError("E_KB_NO_PENDING", "No pending action.");
    }
    if (pending._kind !== expected_kind) {
      throw new XError("E_KB_NO_PENDING", "Pending action type mismatch.");
    }
    const consumed = await pending_action_consume(this._scope, pending._action_id);
    if (!consumed) {
      throw new XError("E_KB_NO_PENDING", "Pending action no longer exists.");
    }
    return consumed;
  }

  private async build_structured_kb_proposal(text: string): Promise<{
    action: "append" | "patch_section" | "replace";
    content?: string;
    section_title?: string;
    rationale: string;
    lang?: string;
  }> {
    const out = await _x.execute({
      _module: "azure",
      _op: "openai_chat",
      _params: {
        temperature: 0,
        messages: [
          {
            role: "system",
            content:
              "Convert an admin KB update request into strict JSON only. " +
              "Return {\"action\":\"append|patch_section|replace\",\"content\":\"...\",\"section_title\":\"...\",\"rationale\":\"...\",\"lang\":\"es|en\"}. " +
              "Use append unless the request clearly asks to replace the whole KB or patch a named section."
          },
          {
            role: "user",
            content: safe_json_stringify({
              request: text
            })
          }
        ]
      }
    });
    if (!is_plain_object(out) || typeof out.text !== "string" || !out.text.trim()) {
      throw new XError("E_KB_BAD_UPSTREAM", "Unable to interpret KB update request");
    }
    const parsed = parse_json_object(out.text);
    const action_raw = ensure_non_empty_string(parsed.action, "action").toLowerCase();
    if (action_raw !== "append" && action_raw !== "patch_section" && action_raw !== "replace") {
      throw new XError("E_KB_BAD_UPSTREAM", "Unsupported KB proposal action");
    }
    const rationale = ensure_optional_string(parsed.rationale) ?? "admin requested KB update";
    return {
      action: action_raw,
      ...(ensure_optional_string(parsed.content) ? { content: ensure_optional_string(parsed.content) } : {}),
      ...(ensure_optional_string(parsed.section_title) ? { section_title: ensure_optional_string(parsed.section_title) } : {}),
      rationale,
      ...(ensure_optional_string(parsed.lang) ? { lang: ensure_optional_string(parsed.lang) } : {})
    };
  }
}

export default KnowledgeBaseModule;
