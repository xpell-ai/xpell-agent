import fs from "node:fs/promises";
import path from "node:path";

import { XError, XModule, type XCommandData } from "@xpell/node";

import { readCommandCtx, requireKernelCap } from "../runtime/guards.js";

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
};

type SourceRecord = {
  id: string;
  filename: string;
  relpath: string;
  abs_path: string;
  bytes: number;
  mtime_ms: number;
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

  constructor(opts: KnowledgeModuleOptions) {
    super({ _name: KNOWLEDGE_MODULE_NAME });
    this._kb_dir = path.resolve(opts._kb_dir);
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

  private async init_on_boot_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    await fs.mkdir(this._kb_dir, { recursive: true });
    const sources = await this.read_sources_index();
    return {
      ok: true,
      sources: sources.length,
      kb_dir: this._kb_dir
    };
  }

  private async list_sources_impl(xcmd: XCommandData) {
    this.ensure_params(xcmd._params);
    const sources = await this.read_sources_index();
    return sources.map((source) => this.to_public_source(source));
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

    const content = await fs.readFile(source.abs_path, "utf8");
    return {
      id: source.id,
      relpath: source.relpath,
      content
    };
  }

  private async search_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const query = ensure_non_empty_string(params.query, "query");
    const limit = this.normalize_limit(params.limit, KB_DEFAULT_LIMIT, KB_MAX_LIMIT);
    const max_snippet_chars = this.normalize_limit(params.max_snippet_chars, KB_DEFAULT_SNIPPET_CHARS, KB_MAX_SNIPPET_CHARS);
    const sources = await this.read_sources_index();
    const hits = await this.search_sources(sources, query, max_snippet_chars);
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
    const sources = await this.read_sources_index();

    const candidates = query
      ? await this.build_query_candidates(sources, query)
      : await this.build_pinned_candidates(sources);

    const built = this.compose_context(candidates, max_chars);
    return {
      context: built.context,
      sources: built.sources.map((relpath) => ({ relpath }))
    };
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

  private async resolve_source_by_id_or_relpath(value: string): Promise<SourceRecord | undefined> {
    const normalized = value.replace(/\\/g, "/");
    const sources = await this.read_sources_index();
    return sources.find((source) => source.id === normalized || source.relpath === normalized);
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

  private async search_sources(sources: SourceRecord[], query: string, max_snippet_chars: number): Promise<SearchHit[]> {
    const query_lower = query.toLowerCase();
    const hits: SearchHit[] = [];
    for (const source of sources) {
      const content = await fs.readFile(source.abs_path, "utf8");
      const lines = content.split(/\r?\n/g);
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

  private async build_query_candidates(sources: SourceRecord[], query: string): Promise<BuildCandidate[]> {
    const hits = await this.search_sources(sources, query, KB_DEFAULT_SNIPPET_CHARS);
    return hits.map((hit) => ({
      relpath: hit.relpath,
      chunk: `[${hit.relpath}:${hit.line_start}-${hit.line_end}]\n${hit.snippet}`
    }));
  }

  private async build_pinned_candidates(sources: SourceRecord[]): Promise<BuildCandidate[]> {
    const pinned_names = ["README.md", "FAQ.md"];
    const by_relpath = new Map<string, SourceRecord>();
    for (const source of sources) {
      by_relpath.set(source.relpath, source);
    }

    const ordered: SourceRecord[] = [];
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

    const candidates: BuildCandidate[] = [];
    for (const source of ordered) {
      const content = await fs.readFile(source.abs_path, "utf8");
      candidates.push({
        relpath: source.relpath,
        chunk: `[${source.relpath}]\n${content}`
      });
    }
    return candidates;
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
