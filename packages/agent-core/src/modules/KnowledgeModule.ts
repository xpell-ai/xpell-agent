import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import { XError, XModule, type XCommandData } from "@xpell/node";

import { readCommandCtx, requireKernelCap, requireKernelCapOrActorRole } from "../runtime/guards.js";
import {
  build_kb_doc_key,
  get_kb_doc_xdb,
  get_kb_source_xdb,
  init_kb_xdb,
  list_kb_docs_xdb,
  list_kb_sources_xdb,
  upsert_kb_doc_xdb,
  upsert_kb_source_xdb,
  type AgentKbXdbScope,
  type PersistedKbDocRecord,
  type PersistedKbSourceRecord
} from "./kb-xdb.js";

export const KNOWLEDGE_MODULE_NAME = "kb";

const KB_DEFAULT_LIMIT = 10;
const KB_MAX_LIMIT = 100;
const KB_DEFAULT_SNIPPET_CHARS = 220;
const KB_MAX_SNIPPET_CHARS = 2000;
const KB_DEFAULT_CONTEXT_CHARS = 6000;
const KB_MAX_CONTEXT_CHARS = 24000;

type Dict = Record<string, unknown>;

type KnowledgeModuleOptions = {
  _kb_dir: string;
  _app_id?: string;
  _env?: string;
};

type SourceRecord = {
  id: string;
  filename: string;
  relpath: string;
  abs_path: string;
  bytes: number;
  mtime_ms: number;
};

type CachedKbFile = SourceRecord & {
  content: string;
};

type SearchHit = {
  relpath: string;
  line_start: number;
  line_end: number;
  snippet: string;
  _score: number;
  _match_at: number;
};

type BuildCandidate = {
  relpath: string;
  chunk: string;
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
    throw new XError("E_KB_BAD_PARAMS", `Invalid ${field_name}: expected non-empty string`);
  }
  return value.trim();
}

function ensure_optional_string(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

export class KnowledgeModule extends XModule {
  static _name = KNOWLEDGE_MODULE_NAME;

  private _kb_dir: string;
  private _xdb_scope: AgentKbXdbScope;
  private _sources_by_id = new Map<string, PersistedKbSourceRecord>();
  private _docs_by_key = new Map<string, PersistedKbDocRecord>();
  private _cached_files: CachedKbFile[] = [];
  private _cached_text = "";
  private _cached_bytes = 0;
  private _cached_updated_at = 0;

  constructor(opts: KnowledgeModuleOptions) {
    super({ _name: KNOWLEDGE_MODULE_NAME });
    this._kb_dir = path.resolve(opts._kb_dir);
    this._xdb_scope = {
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

  async _list_sources(xcmd: XCommandData) {
    return this.list_sources_impl(xcmd);
  }
  async _op_list_sources(xcmd: XCommandData) {
    return this.list_sources_impl(xcmd);
  }

  async _get_source(xcmd: XCommandData) {
    return this.get_source_impl(xcmd);
  }
  async _op_get_source(xcmd: XCommandData) {
    return this.get_source_impl(xcmd);
  }

  async _search(xcmd: XCommandData) {
    return this.search_impl(xcmd);
  }
  async _op_search(xcmd: XCommandData) {
    return this.search_impl(xcmd);
  }

  async _build_context(xcmd: XCommandData) {
    return this.build_context_impl(xcmd);
  }
  async _op_build_context(xcmd: XCommandData) {
    return this.build_context_impl(xcmd);
  }

  async _status(xcmd: XCommandData) {
    return this.status_impl(xcmd);
  }
  async _op_status(xcmd: XCommandData) {
    return this.status_impl(xcmd);
  }

  async _list_files(xcmd: XCommandData) {
    return this.list_files_impl(xcmd);
  }
  async _op_list_files(xcmd: XCommandData) {
    return this.list_files_impl(xcmd);
  }

  async _reload(xcmd: XCommandData) {
    return this.reload_impl(xcmd);
  }
  async _op_reload(xcmd: XCommandData) {
    return this.reload_impl(xcmd);
  }

  async _get_text(xcmd: XCommandData) {
    return this.get_text_impl(xcmd);
  }
  async _op_get_text(xcmd: XCommandData) {
    return this.get_text_impl(xcmd);
  }

  async _sources_list(xcmd: XCommandData) {
    return this.sources_list_impl(xcmd);
  }
  async _op_sources_list(xcmd: XCommandData) {
    return this.sources_list_impl(xcmd);
  }

  async _sources_upsert(xcmd: XCommandData) {
    return this.sources_upsert_impl(xcmd);
  }
  async _op_sources_upsert(xcmd: XCommandData) {
    return this.sources_upsert_impl(xcmd);
  }

  async _sources_enable(xcmd: XCommandData) {
    return this.sources_set_enabled_impl(xcmd, true);
  }
  async _op_sources_enable(xcmd: XCommandData) {
    return this.sources_set_enabled_impl(xcmd, true);
  }

  async _sources_disable(xcmd: XCommandData) {
    return this.sources_set_enabled_impl(xcmd, false);
  }
  async _op_sources_disable(xcmd: XCommandData) {
    return this.sources_set_enabled_impl(xcmd, false);
  }

  async _reload_source(xcmd: XCommandData) {
    return this.reload_source_impl(xcmd);
  }
  async _op_reload_source(xcmd: XCommandData) {
    return this.reload_source_impl(xcmd);
  }

  async _reload_all_enabled(xcmd: XCommandData) {
    return this.reload_all_enabled_impl(xcmd);
  }
  async _op_reload_all_enabled(xcmd: XCommandData) {
    return this.reload_all_enabled_impl(xcmd);
  }

  async _docs_list(xcmd: XCommandData) {
    return this.docs_list_impl(xcmd);
  }
  async _op_docs_list(xcmd: XCommandData) {
    return this.docs_list_impl(xcmd);
  }

  async _docs_get(xcmd: XCommandData) {
    return this.docs_get_impl(xcmd);
  }
  async _op_docs_get(xcmd: XCommandData) {
    return this.docs_get_impl(xcmd);
  }

  private async init_on_boot_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    await init_kb_xdb(this._xdb_scope);
    await fs.mkdir(this._kb_dir, { recursive: true });
    await this.hydrate_from_xdb();
    const sources = await this.reload_cache();
    return {
      ok: true,
      sources: sources.length,
      registered_sources: this._sources_by_id.size,
      stored_docs: this._docs_by_key.size,
      kb_dir: this._kb_dir
    };
  }

  private async list_sources_impl(xcmd: XCommandData) {
    this.ensure_params(xcmd._params);
    return this._cached_files.map((source) => this.to_public_source(source));
  }

  private async get_source_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const requested = ensure_optional_string(params.id) ?? ensure_optional_string(params.relpath);
    if (!requested) {
      throw new XError("E_KB_BAD_PARAMS", "Provide id or relpath");
    }

    const source = await this.resolve_source_by_id_or_relpath(requested);
    if (!source) {
      throw new XError("E_KB_NOT_FOUND", `Source not found: ${requested}`);
    }

    return {
      id: source.id,
      relpath: source.relpath,
      content: source.content
    };
  }

  private async search_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const query = ensure_non_empty_string(params.query, "query");
    const limit = this.normalize_limit(params.limit, KB_DEFAULT_LIMIT, KB_MAX_LIMIT);
    const max_snippet_chars = this.normalize_limit(params.max_snippet_chars, KB_DEFAULT_SNIPPET_CHARS, KB_MAX_SNIPPET_CHARS);
    const hits = this.search_sources(this._cached_files, query, max_snippet_chars);
    return hits.slice(0, limit).map((hit) => ({
      relpath: hit.relpath,
      line_start: hit.line_start,
      line_end: hit.line_end,
      snippet: hit.snippet
    }));
  }

  private async build_context_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const query = ensure_optional_string(params.query);
    const max_chars = this.normalize_limit(params.max_chars, KB_DEFAULT_CONTEXT_CHARS, KB_MAX_CONTEXT_CHARS);
    const candidates = query
      ? this.build_query_candidates(this._cached_files, query)
      : this.build_pinned_candidates(this._cached_files);

    const built = this.compose_context(candidates, max_chars);
    return {
      context: built.context,
      sources: built.sources.map((relpath) => ({ relpath }))
    };
  }

  private status_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    return {
      _enabled: true,
      _source: "work_dir",
      _kb_dir: this._kb_dir,
      _files: this._cached_files.length,
      _bytes: this._cached_bytes,
      _updated_at: this._cached_updated_at
    };
  }

  private list_files_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    return {
      _kb_dir: this._kb_dir,
      _files: this._cached_files.map((file) => ({
        _name: file.relpath,
        _bytes: file.bytes,
        _mtime: Math.floor(file.mtime_ms)
      }))
    };
  }

  private async reload_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    const files = await this.reload_cache();
    return {
      _ok: true,
      _files: files.length,
      _bytes: this._cached_bytes,
      _updated_at: this._cached_updated_at
    };
  }

  private get_text_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    const params = this.ensure_params(xcmd._params);
    const max_chars = this.normalize_optional_max_chars(params._max_chars ?? params.max_chars, KB_MAX_CONTEXT_CHARS);
    const text = this._cached_text;
    const bounded = max_chars === undefined ? text : text.slice(0, max_chars);
    return {
      _text: bounded,
      _truncated: max_chars !== undefined ? text.length > max_chars : false,
      _bytes: this._cached_bytes,
      _updated_at: this._cached_updated_at
    };
  }

  private async sources_list_impl(xcmd: XCommandData) {
    this.ensure_params(xcmd._params);
    const items = Array.from(this._sources_by_id.values())
      .sort((left, right) => left._source_id.localeCompare(right._source_id))
      .map((item) => this.to_public_kb_source(item));
    return { items };
  }

  private async sources_upsert_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const source_id = ensure_optional_string(params._source_id) ?? ensure_optional_string(params.source_id);
    const title = ensure_optional_string(params._title) ?? ensure_optional_string(params.title);
    if (!source_id || !title) {
      throw new XError("E_KB_BAD_PARAMS", "_source_id and _title are required");
    }

    const paths_raw = Array.isArray(params._paths) ? params._paths : Array.isArray(params.paths) ? params.paths : [];
    const paths = this.normalize_paths(paths_raw);
    if (paths.length === 0) {
      throw new XError("E_KB_BAD_PARAMS", "_paths must include at least one path");
    }

    const existing = this._sources_by_id.get(source_id) ?? (await get_kb_source_xdb(this._xdb_scope, source_id));
    const now = Date.now();
    const record: PersistedKbSourceRecord = {
      _source_id: source_id,
      _title: title,
      _enabled:
        typeof (params._enabled ?? params.enabled) === "boolean"
          ? ((params._enabled ?? params.enabled) as boolean)
          : existing?._enabled ?? true,
      _paths: paths,
      ...(ensure_optional_string(params._lang ?? params.lang)
        ? { _lang: ensure_optional_string(params._lang ?? params.lang) }
        : existing?._lang
          ? { _lang: existing._lang }
          : {}),
      ...(ensure_optional_string(params._notes ?? params.notes)
        ? { _notes: ensure_optional_string(params._notes ?? params.notes) }
        : existing?._notes
          ? { _notes: existing._notes }
          : {}),
      _created_at: existing?._created_at ?? now,
      _updated_at: now
    };

    await upsert_kb_source_xdb(this._xdb_scope, record);
    this._sources_by_id.set(source_id, record);
    return { source: this.to_public_kb_source(record) };
  }

  private async sources_set_enabled_impl(xcmd: XCommandData, enabled: boolean) {
    const params = this.ensure_params(xcmd._params);
    const source_id = ensure_optional_string(params._source_id) ?? ensure_optional_string(params.source_id);
    if (!source_id) {
      throw new XError("E_KB_BAD_PARAMS", "_source_id is required");
    }

    const existing = this._sources_by_id.get(source_id) ?? (await get_kb_source_xdb(this._xdb_scope, source_id));
    if (!existing) {
      throw new XError("E_KB_NOT_FOUND", `KB source not found: ${source_id}`);
    }

    const record: PersistedKbSourceRecord = {
      ...existing,
      _paths: [...existing._paths],
      _enabled: enabled,
      _updated_at: Date.now()
    };
    await upsert_kb_source_xdb(this._xdb_scope, record);
    this._sources_by_id.set(source_id, record);
    return { source: this.to_public_kb_source(record) };
  }

  private async reload_source_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const source_id = ensure_optional_string(params._source_id) ?? ensure_optional_string(params.source_id);
    if (!source_id) {
      throw new XError("E_KB_BAD_PARAMS", "_source_id is required");
    }

    const source = this._sources_by_id.get(source_id) ?? (await get_kb_source_xdb(this._xdb_scope, source_id));
    if (!source) {
      throw new XError("E_KB_NOT_FOUND", `KB source not found: ${source_id}`);
    }
    this._sources_by_id.set(source_id, {
      ...source,
      _paths: [...source._paths]
    });

    const docs = await this.load_docs_for_source(source);
    for (const doc of docs) {
      await upsert_kb_doc_xdb(this._xdb_scope, doc);
      this._docs_by_key.set(doc._key, doc);
    }

    return {
      source: this.to_public_kb_source(source),
      docs_reloaded: docs.length
    };
  }

  private async reload_all_enabled_impl(xcmd: XCommandData) {
    this.ensure_params(xcmd._params);
    const enabled_sources = Array.from(this._sources_by_id.values())
      .filter((source) => source._enabled)
      .sort((left, right) => left._source_id.localeCompare(right._source_id));
    let docs_reloaded = 0;
    for (const source of enabled_sources) {
      const docs = await this.load_docs_for_source(source);
      for (const doc of docs) {
        await upsert_kb_doc_xdb(this._xdb_scope, doc);
        this._docs_by_key.set(doc._key, doc);
      }
      docs_reloaded += docs.length;
    }
    return {
      sources_reloaded: enabled_sources.length,
      docs_reloaded
    };
  }

  private docs_list_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const source_id = ensure_optional_string(params._source_id) ?? ensure_optional_string(params.source_id);
    const items = Array.from(this._docs_by_key.values())
      .filter((doc) => (source_id ? doc._source_id === source_id : true))
      .sort((left, right) => {
        if (left._source_id !== right._source_id) return left._source_id.localeCompare(right._source_id);
        return left._path.localeCompare(right._path);
      })
      .map((doc) => this.to_public_kb_doc(doc));
    return { items };
  }

  private async docs_get_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const key = ensure_optional_string(params._key) ?? ensure_optional_string(params.key);
    if (!key) {
      throw new XError("E_KB_BAD_PARAMS", "_key is required");
    }
    const doc = this._docs_by_key.get(key) ?? (await get_kb_doc_xdb(this._xdb_scope, key));
    if (!doc) {
      throw new XError("E_KB_NOT_FOUND", `KB doc not found: ${key}`);
    }
    this._docs_by_key.set(key, { ...doc });
    return { doc: this.to_public_kb_doc(doc, true) };
  }

  private ensure_params(value: unknown): Dict {
    if (value === undefined || value === null) return {};
    if (!is_plain_object(value)) {
      throw new XError("E_KB_BAD_PARAMS", "Expected params to be an object");
    }
    if (has_function(value)) {
      throw new XError("E_KB_BAD_PARAMS", "params must be JSON-safe");
    }
    return value;
  }

  private normalize_limit(value: unknown, fallback: number, max: number): number {
    if (value === undefined || value === null) return fallback;
    const parsed = Number.parseInt(String(value), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new XError("E_KB_BAD_PARAMS", "Invalid limit value");
    }
    return Math.min(parsed, max);
  }

  private normalize_optional_max_chars(value: unknown, max: number): number | undefined {
    if (value === undefined || value === null) return undefined;
    const parsed = Number.parseInt(String(value), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new XError("E_KB_BAD_PARAMS", "Invalid max chars value");
    }
    return Math.min(parsed, max);
  }

  private normalize_paths(value: unknown[]): string[] {
    const out: string[] = [];
    for (const item of value) {
      const text = ensure_optional_string(item);
      if (!text) continue;
      out.push(text);
    }
    return out;
  }

  private async hydrate_from_xdb(): Promise<void> {
    const sources = await list_kb_sources_xdb(this._xdb_scope);
    const docs = await list_kb_docs_xdb(this._xdb_scope);
    this._sources_by_id.clear();
    this._docs_by_key.clear();
    for (const source of sources) {
      this._sources_by_id.set(source._source_id, {
        ...source,
        _paths: [...source._paths]
      });
    }
    for (const doc of docs) {
      this._docs_by_key.set(doc._key, { ...doc });
    }
  }

  private async load_docs_for_source(source: PersistedKbSourceRecord): Promise<PersistedKbDocRecord[]> {
    const candidate_paths: string[] = [];
    for (const raw_path of source._paths) {
      const resolved = path.isAbsolute(raw_path) ? path.resolve(raw_path) : path.resolve(this._kb_dir, raw_path);
      await this.collect_md_paths(resolved, candidate_paths);
    }

    candidate_paths.sort((left, right) => left.localeCompare(right));
    const docs: PersistedKbDocRecord[] = [];
    for (const abs_path of candidate_paths) {
      const content = await fs.readFile(abs_path, "utf8");
      const normalized_path = abs_path.replace(/\\/g, "/");
      const doc_id = this.sha1_text(normalized_path);
      const content_sha1 = this.sha1_text(content);
      const existing = this._docs_by_key.get(build_kb_doc_key(source._source_id, doc_id));
      const now = Date.now();
      docs.push({
        _key: build_kb_doc_key(source._source_id, doc_id),
        _source_id: source._source_id,
        _doc_id: doc_id,
        _path: normalized_path,
        ...(source._lang ? { _lang: source._lang } : {}),
        _content: content,
        _content_sha1: content_sha1,
        _created_at: existing?._created_at ?? now,
        _updated_at: now
      });
    }
    return docs;
  }

  private async collect_md_paths(abs_path: string, out: string[]): Promise<void> {
    let stat;
    try {
      stat = await fs.stat(abs_path);
    } catch (err) {
      const code = typeof err === "object" && err !== null ? (err as any).code : undefined;
      if (code === "ENOENT") return;
      throw err;
    }

    if (stat.isDirectory()) {
      const entries = await fs.readdir(abs_path, { withFileTypes: true });
      entries.sort((left, right) => left.name.localeCompare(right.name));
      for (const entry of entries) {
        await this.collect_md_paths(path.join(abs_path, entry.name), out);
      }
      return;
    }

    if (!stat.isFile()) return;
    if (!abs_path.toLowerCase().endsWith(".md")) return;
    out.push(abs_path);
  }

  private sha1_text(value: string): string {
    return createHash("sha1").update(value, "utf8").digest("hex");
  }

  private to_public_kb_source(source: PersistedKbSourceRecord) {
    return {
      _source_id: source._source_id,
      _title: source._title,
      _enabled: source._enabled,
      _paths: [...source._paths],
      ...(source._lang ? { _lang: source._lang } : {}),
      ...(source._notes ? { _notes: source._notes } : {}),
      _created_at: source._created_at,
      _updated_at: source._updated_at
    };
  }

  private to_public_kb_doc(doc: PersistedKbDocRecord, include_content = false) {
    return {
      _key: doc._key,
      _source_id: doc._source_id,
      _doc_id: doc._doc_id,
      _path: doc._path,
      ...(doc._title ? { _title: doc._title } : {}),
      ...(doc._lang ? { _lang: doc._lang } : {}),
      ...(include_content ? { _content: doc._content } : {}),
      _content_sha1: doc._content_sha1,
      _created_at: doc._created_at,
      _updated_at: doc._updated_at
    };
  }

  private async read_sources_index(): Promise<SourceRecord[]> {
    try {
      const stat = await fs.stat(this._kb_dir);
      if (!stat.isDirectory()) {
        throw new XError("E_KB_BAD_STATE", `KB path is not a directory: ${this._kb_dir}`);
      }
    } catch (err) {
      const code = typeof err === "object" && err !== null ? (err as any).code : undefined;
      if (code === "ENOENT") return [];
      throw err;
    }

    const out: SourceRecord[] = [];
    await this.collect_sources_recursive(this._kb_dir, "", out);
    out.sort((left, right) => left.relpath.localeCompare(right.relpath));
    return out;
  }

  private async collect_sources_recursive(abs_dir: string, rel_dir: string, out: SourceRecord[]): Promise<void> {
    const entries = await fs.readdir(abs_dir, { withFileTypes: true });
    entries.sort((left, right) => left.name.localeCompare(right.name));

    for (const entry of entries) {
      const child_abs = path.join(abs_dir, entry.name);
      const child_rel = rel_dir ? `${rel_dir}/${entry.name}` : entry.name;

      if (entry.isDirectory()) {
        await this.collect_sources_recursive(child_abs, child_rel, out);
        continue;
      }
      if (!entry.isFile()) continue;
      if (!entry.name.toLowerCase().endsWith(".md")) continue;

      const stat = await fs.stat(child_abs);
      out.push({
        id: child_rel,
        filename: entry.name,
        relpath: child_rel,
        abs_path: child_abs,
        bytes: stat.size,
        mtime_ms: stat.mtimeMs
      });
    }
  }

  private async resolve_source_by_id_or_relpath(value: string): Promise<CachedKbFile | undefined> {
    const normalized = value.replace(/\\/g, "/");
    return this._cached_files.find((source) => source.id === normalized || source.relpath === normalized);
  }

  private to_public_source(source: SourceRecord) {
    return {
      id: source.id,
      filename: source.filename,
      relpath: source.relpath,
      bytes: source.bytes,
      mtime_ms: source.mtime_ms
    };
  }

  private search_sources(sources: CachedKbFile[], query: string, max_snippet_chars: number): SearchHit[] {
    const query_lower = query.toLowerCase();
    const hits: SearchHit[] = [];
    for (const source of sources) {
      const lines = source.content.split(/\r?\n/g);
      for (let index = 0; index < lines.length; index += 1) {
        const line = lines[index] ?? "";
        const compact = line.replace(/\s+/g, " ").trim();
        if (!compact) continue;
        const compact_lower = compact.toLowerCase();
        const match_at = compact_lower.indexOf(query_lower);
        if (match_at < 0) continue;

        hits.push({
          relpath: source.relpath,
          line_start: index + 1,
          line_end: index + 1,
          snippet: this.build_snippet(compact, match_at, query.length, max_snippet_chars),
          _score: this.count_occurrences(compact_lower, query_lower),
          _match_at: match_at
        });
      }
    }

    hits.sort((left, right) => {
      if (left._score !== right._score) return right._score - left._score;
      if (left._match_at !== right._match_at) return left._match_at - right._match_at;
      const path_cmp = left.relpath.localeCompare(right.relpath);
      if (path_cmp !== 0) return path_cmp;
      return left.line_start - right.line_start;
    });

    return hits;
  }

  private count_occurrences(haystack: string, needle: string): number {
    if (!needle) return 0;
    let count = 0;
    let offset = 0;
    while (offset <= haystack.length) {
      const at = haystack.indexOf(needle, offset);
      if (at < 0) break;
      count += 1;
      offset = at + Math.max(needle.length, 1);
    }
    return count;
  }

  private build_snippet(line: string, match_at: number, query_len: number, max_chars: number): string {
    if (line.length <= max_chars) return line;

    const safe_max = Math.max(32, max_chars - 3);
    const half = Math.max(16, Math.floor((safe_max - query_len) / 2));
    let start = Math.max(0, match_at - half);
    let end = Math.min(line.length, start + safe_max);
    if (end - start < safe_max) {
      start = Math.max(0, end - safe_max);
    }

    const head = start > 0 ? "..." : "";
    const tail = end < line.length ? "..." : "";
    return `${head}${line.slice(start, end)}${tail}`;
  }

  private build_query_candidates(sources: CachedKbFile[], query: string): BuildCandidate[] {
    const hits = this.search_sources(sources, query, KB_DEFAULT_SNIPPET_CHARS);
    return hits.map((hit) => ({
      relpath: hit.relpath,
      chunk: `[${hit.relpath}:${hit.line_start}-${hit.line_end}]\n${hit.snippet}`
    }));
  }

  private build_pinned_candidates(sources: CachedKbFile[]): BuildCandidate[] {
    const pinned_names = ["README.md", "FAQ.md"];
    const by_relpath = new Map<string, CachedKbFile>();
    for (const source of sources) {
      by_relpath.set(source.relpath, source);
    }

    const ordered: CachedKbFile[] = [];
    for (const pinned_name of pinned_names) {
      const pinned = by_relpath.get(pinned_name);
      if (pinned) ordered.push(pinned);
    }

    const pinned_set = new Set(ordered.map((item) => item.relpath));
    const remaining = sources
      .filter((item) => !pinned_set.has(item.relpath))
      .sort((left, right) => {
        const name_cmp = left.filename.localeCompare(right.filename);
        if (name_cmp !== 0) return name_cmp;
        return left.relpath.localeCompare(right.relpath);
      });
    ordered.push(...remaining);

    return ordered.map((source) => ({
      relpath: source.relpath,
      chunk: `[${source.relpath}]\n${source.content}`
    }));
  }

  private async reload_cache(): Promise<CachedKbFile[]> {
    const sources = await this.read_sources_index();
    const cached: CachedKbFile[] = [];
    let total_bytes = 0;

    for (const source of sources) {
      const content = await fs.readFile(source.abs_path, "utf8");
      cached.push({
        ...source,
        content
      });
      total_bytes += source.bytes;
    }

    this._cached_files = cached;
    this._cached_bytes = total_bytes;
    this._cached_text = cached.map((source) => `[${source.relpath}]\n${source.content}`).join("\n\n");
    this._cached_updated_at = Date.now();
    return cached;
  }

  private compose_context(candidates: BuildCandidate[], max_chars: number): { context: string; sources: string[] } {
    let body = "";
    let included_sources: string[] = [];

    for (const candidate of candidates) {
      const next_sources = this.push_unique(included_sources, candidate.relpath);
      const next_body = body ? `${body}\n\n${candidate.chunk}` : candidate.chunk;
      const preview = this.render_context(next_body, next_sources);
      if (preview.length <= max_chars) {
        body = next_body;
        included_sources = next_sources;
        continue;
      }
      if (!body) {
        body = candidate.chunk;
        included_sources = next_sources;
      }
      break;
    }

    const context = this.finalize_context(body, included_sources, max_chars);
    return {
      context,
      sources: included_sources
    };
  }

  private push_unique(list: string[], value: string): string[] {
    if (list.includes(value)) return [...list];
    return [...list, value];
  }

  private render_context(body: string, sources: string[]): string {
    const footer = this.build_footer(sources);
    if (!body.trim()) return footer;
    return `${body}\n\n${footer}`;
  }

  private finalize_context(body: string, sources: string[], max_chars: number): string {
    const footer = this.build_footer_with_limit(sources, max_chars);
    const separator = body.trim().length > 0 ? "\n\n" : "";
    const max_body_chars = max_chars - footer.length - separator.length;

    if (max_body_chars <= 0) {
      return footer.slice(0, max_chars);
    }

    const body_trimmed = this.truncate_text(body, max_body_chars);
    if (!body_trimmed) return footer;
    return `${body_trimmed}${separator}${footer}`;
  }

  private build_footer(sources: string[]): string {
    if (sources.length === 0) {
      return "Sources:\n- (none)";
    }
    return `Sources:\n${sources.map((source) => `- ${source}`).join("\n")}`;
  }

  private build_footer_with_limit(sources: string[], max_chars: number): string {
    let footer = this.build_footer(sources);
    if (footer.length <= max_chars) return footer;

    const trimmed: string[] = [];
    for (const source of sources) {
      const next = [...trimmed, source];
      const candidate = `Sources:\n${next.map((item) => `- ${item}`).join("\n")}\n- ...`;
      if (candidate.length > max_chars) break;
      trimmed.push(source);
    }

    if (trimmed.length === 0) {
      const minimal = "Sources:\n- ...";
      return minimal.length <= max_chars ? minimal : minimal.slice(0, max_chars);
    }

    footer = `Sources:\n${trimmed.map((item) => `- ${item}`).join("\n")}\n- ...`;
    return footer.length <= max_chars ? footer : footer.slice(0, max_chars);
  }

  private truncate_text(value: string, max_chars: number): string {
    if (max_chars <= 0) return "";
    if (value.length <= max_chars) return value;
    if (max_chars <= 3) return value.slice(0, max_chars);
    return `${value.slice(0, max_chars - 3)}...`;
  }
}

export default KnowledgeModule;
