import { XError, XModule, _x, _xlog, type XCommandData } from "@xpell/node";

import { readCommandCtx } from "../runtime/guards.js";
import { extract_section, list_top_headings } from "../kb/kb_markdown.js";
import { BROADCASTS_MODULE_NAME } from "./BroadcastsModule.js";
import { CONVERSATIONS_MODULE_NAME } from "./ConversationsModule.js";
import { KNOWLEDGE_MODULE_NAME } from "./KnowledgeModule.js";
import { USERS_MODULE_NAME } from "./UsersModule.js";

export const ADMIN_COMMANDS_MODULE_NAME = "admin_cmd";
const KB_SHOW_MAX_CHARS = 3200;

type Dict = Record<string, unknown>;

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
    throw new XError("E_ADMIN_CMD_BAD_PARAMS", `Invalid ${field_name}: expected non-empty string`);
  }
  return value.trim();
}

function ensure_optional_string(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function truncate_text(value: string, max_chars: number): { text: string; truncated: boolean } {
  const trimmed = value.trim();
  if (trimmed.length <= max_chars) {
    return { text: trimmed, truncated: false };
  }
  return {
    text: `${trimmed.slice(0, Math.max(0, max_chars - 3))}...`,
    truncated: true
  };
}

function parse_command_text(text: string): { _is_command: boolean; _cmd?: string; _args?: string } {
  const trimmed = text.trim();
  if (!trimmed.startsWith("/")) {
    return { _is_command: false };
  }

  const without_prefix = trimmed.slice(1).trim();
  if (!without_prefix) {
    return {
      _is_command: true,
      _cmd: "",
      _args: ""
    };
  }

  const space_idx = without_prefix.indexOf(" ");
  if (space_idx < 0) {
    return {
      _is_command: true,
      _cmd: without_prefix.toLowerCase(),
      _args: ""
    };
  }

  return {
    _is_command: true,
    _cmd: without_prefix.slice(0, space_idx).trim().toLowerCase(),
    _args: without_prefix.slice(space_idx + 1).trim()
  };
}

function normalize_command_name(value: string): string {
  const trimmed = value.trim().toLowerCase();
  if (!trimmed) return "";
  const at_index = trimmed.indexOf("@");
  return at_index >= 0 ? trimmed.slice(0, at_index).trim() : trimmed;
}

export class AdminCommandsModule extends XModule {
  static _name = ADMIN_COMMANDS_MODULE_NAME;

  constructor() {
    super({ _name: ADMIN_COMMANDS_MODULE_NAME });
  }

  async _is_command(xcmd: XCommandData) {
    return this.is_command_impl(xcmd);
  }
  async _op_is_command(xcmd: XCommandData) {
    return this.is_command_impl(xcmd);
  }

  async _handle_message(xcmd: XCommandData) {
    return this.handle_message_impl(xcmd);
  }
  async _op_handle_message(xcmd: XCommandData) {
    return this.handle_message_impl(xcmd);
  }

  async _conv_summary(xcmd: XCommandData) {
    return this.conv_summary_impl(xcmd);
  }
  async _op_conv_summary(xcmd: XCommandData) {
    return this.conv_summary_impl(xcmd);
  }

  private is_command_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const text = ensure_non_empty_string(params._text, "_text");
    return parse_command_text(text);
  }

  private async conv_summary_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const role = typeof ctx.actor?.role === "string" ? ctx.actor.role : "";
    if (role !== "admin" && role !== "owner") {
      throw new XError("E_AGENT_FORBIDDEN", "Admin command access required");
    }

    const out = await _x.execute({
      _module: CONVERSATIONS_MODULE_NAME,
      _op: "list_threads",
      _params: {
        limit: 5,
        _ctx: this.forward_ctx(ctx)
      }
    });
    const threads = is_plain_object(out) && Array.isArray(out.threads) ? out.threads.filter((entry): entry is Dict => is_plain_object(entry)) : [];
    if (threads.length === 0) {
      return { _reply_text: "Summary: no conversations yet." };
    }

    const lines = ["Summary:"];
    for (const thread of threads) {
      const channel = ensure_optional_string(thread.channel) ?? "unknown";
      const channel_thread_id = ensure_optional_string(thread.channel_thread_id) ?? "n/a";
      const status = ensure_optional_string(thread.status) ?? "open";
      lines.push(`- ${channel}:${channel_thread_id} (${status})`);
    }
    return { _reply_text: lines.join("\n") };
  }

  private async handle_message_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const role = typeof ctx.actor?.role === "string" ? ctx.actor.role : "";
    if (role !== "admin" && role !== "owner") {
      throw new XError("E_AGENT_FORBIDDEN", "Admin command access required");
    }

    const params = this.ensure_params(xcmd._params);
    const text = ensure_non_empty_string(params._text, "_text");
    const parsed = parse_command_text(text);
    const cmd = normalize_command_name(ensure_optional_string(parsed._cmd) ?? "");

    let _reply_text = "Unknown admin command. Try /help";
    if (cmd === "start") {
      _reply_text = [
        "Welcome.",
        "You can ask normal questions about Ruta1, and I will answer from the knowledge base.",
        "Use /help to see admin commands."
      ].join("\n");
    } else if (cmd === "help") {
      _reply_text = [
        "Admin commands:",
        "/start",
        "/help",
        "/summary",
        "/users",
        "/users telegram",
        "/broadcast status",
        "/broadcast send <id>",
        "/kb",
        "/kb_update",
        "/kb_preview",
        "/kb_apply",
        "/kb_cancel",
        "/kb_history"
      ].join("\n");
    } else if (cmd === "summary") {
      _reply_text = "Summary (stub): not implemented yet";
    } else if (cmd === "users") {
      try {
        _reply_text = await this.handle_users_command(parsed._args ?? "", ctx);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        _reply_text = `Users command failed: ${message}`;
      }
    } else if (cmd === "broadcast") {
      try {
        _reply_text = await this.handle_broadcast_command(parsed._args ?? "", ctx);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        _reply_text = `Broadcast command failed: ${message}`;
      }
    } else if (cmd === "kb" || cmd === "kb_update" || cmd === "kb_preview" || cmd === "kb_apply" || cmd === "kb_cancel" || cmd === "kb_history") {
      try {
        if (cmd === "kb") {
          _reply_text = await this.handle_kb_command(parsed._args ?? "", ctx);
        } else if (cmd === "kb_update") {
          _reply_text = await this.handle_kb_update_command(parsed._args ?? "", ctx);
        } else if (cmd === "kb_preview") {
          const out = await this.exec_kb_preview(ctx);
          _reply_text = out._preview_text;
        } else if (cmd === "kb_apply") {
          const out = await this.exec_kb_apply(ctx);
          _reply_text = out._reply_text;
        } else if (cmd === "kb_cancel") {
          const out = await this.exec_kb_cancel(ctx);
          _reply_text = out._reply_text;
        } else if (cmd === "kb_history") {
          _reply_text = await this.handle_kb_history_command(ctx);
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        _reply_text = `KB command failed: ${message}`;
      }
    }

    return { _reply_text };
  }

  private async handle_users_command(
    args: string,
    ctx: ReturnType<typeof readCommandCtx>
  ): Promise<string> {
    const trimmed = args.trim();
    const lower = trimmed.toLowerCase();
    if (!trimmed) {
      return this.exec_users_list(ctx);
    }
    if (lower === "telegram") {
      return this.exec_users_debug_identities(ctx, "telegram");
    }
    return "Users commands:\n- /users\n- /users telegram";
  }

  private async handle_broadcast_command(
    args: string,
    ctx: ReturnType<typeof readCommandCtx>
  ): Promise<string> {
    const trimmed = args.trim();
    const [subcommand_raw, ...rest_parts] = trimmed.length > 0 ? trimmed.split(/\s+/g) : [""];
    const subcommand = subcommand_raw.trim().toLowerCase();
    const subcommand_args = rest_parts.join(" ").trim();

    if (!subcommand || subcommand === "help") {
      return [
        "Broadcast commands:",
        "- /broadcast preview <text>",
        "- /broadcast <text>",
        "- /broadcast --include-admins <text>",
        "- /broadcast status",
        "- /broadcast send <id>"
      ].join("\n");
    }

    if (subcommand === "status") {
      return this.exec_broadcast_list_recent(ctx);
    }

    if (subcommand === "send") {
      const broadcast_id = ensure_non_empty_string(subcommand_args, "broadcast_id");
      return this.exec_broadcast_send_batch(ctx, broadcast_id);
    }

    const include_admins = trimmed.startsWith("--include-admins ");
    const normalized_args = include_admins ? trimmed.replace(/^--include-admins\s+/i, "").trim() : trimmed;
    const preview_match = normalized_args.match(/^preview(?:\s+(.+))?$/i);
    if (preview_match) {
      return this.exec_broadcast_preview(ctx, {
        roles: include_admins ? ["customer", "admin"] : ["customer"],
        channel: "telegram"
      });
    }

    const message_text = ensure_non_empty_string(normalized_args, "_message_text");
    return this.exec_broadcast_send(ctx, message_text, {
      roles: include_admins ? ["customer", "admin"] : ["customer"],
      channel: "telegram"
    });

    return "Unknown broadcast command. Try /broadcast status";
  }

  private async handle_kb_update_command(
    args: string,
    ctx: ReturnType<typeof readCommandCtx>
  ): Promise<string> {
    const trimmed = args.trim();
    if (!trimmed) {
      const out = await this.exec_kb_set_awaiting_input(ctx);
      return out._reply_text;
    }
    const propose = await this.exec_kb_propose(ctx, trimmed);
    return propose._preview_text;
  }

  private async handle_kb_history_command(ctx: ReturnType<typeof readCommandCtx>): Promise<string> {
    const out = await this.exec_kb_history(ctx, 5);
    if (out._items.length === 0) {
      return "KB history:\n- (none)";
    }
    return [
      "KB history:",
      ...out._items.map(
        (item) =>
          `- ${new Date(item._updated_at).toISOString()} ${item._actor_user_id} version=${item._version_id}`
      )
    ].join("\n");
  }

  private async handle_kb_command(args: string, ctx: ReturnType<typeof readCommandCtx>): Promise<string> {
    const normalized = args.trim();
    const [subcommand_raw, ...rest_parts] = normalized.length > 0 ? normalized.split(/\s+/g) : [""];
    const subcommand = subcommand_raw.trim().toLowerCase();
    const subcommand_args = rest_parts.join(" ").trim();

    if (!subcommand || subcommand === "help") {
      const status = await this.exec_kb_status(ctx);
      const latest = await this.exec_kb_history(ctx, 1);
      const current_version = latest._items.length > 0 ? latest._items[0]._version_id : "initial";
      return [
        `KB enabled: ${status._enabled ? "yes" : "no"}`,
        `Current version: ${current_version}`,
        "KB commands:",
        "- /kb status",
        "- /kb list",
        "- /kb reload",
        "- /kb show",
        "- /kb_update <text>",
        "- /kb_preview",
        "- /kb_apply",
        "- /kb_cancel",
        "- /kb_history"
      ].join("\n");
    }

    if (subcommand === "status") {
      const out = await this.exec_kb_status(ctx);
      return [
        `KB enabled: ${out._enabled ? "yes" : "no"}`,
        `Source: ${out._source}`,
        `Dir: ${out._kb_dir}`,
        `Files: ${out._files}`,
        `Bytes: ${out._bytes}`,
        `Updated: ${new Date(out._updated_at).toISOString()}`
      ].join("\n");
    }

    if (subcommand === "list") {
      const out = await this.exec_kb_list_files(ctx);
      if (out._files.length === 0) {
        return "KB files:\n- (none)";
      }
      return [
        "KB files:",
        ...out._files.map((file) => `- ${file._name} (${file._bytes} bytes)`)
      ].join("\n");
    }

    if (subcommand === "reload") {
      const out = await this.exec_kb_reload(ctx);
      return `✅ KB reloaded: ${out._files} files, ${out._bytes} bytes`;
    }

    if (subcommand === "show") {
      _xlog.log("[kb-route]", {
        route: "slash",
        resolved_intent: "admin.kb.show",
        called_op: "kb.show"
      });
      const section_title = this.parse_kb_show_section_arg(subcommand_args);
      const out = await this.exec_kb_show(ctx);
      const body = out.content.trim().length > 0 ? out.content : "";
      const header = `[KB:on] ${out._kb_file}`;
      if (!section_title) {
        const headings = list_top_headings(body);
        if (headings.length === 0) {
          return `${header}\nSections:\n- (none)`;
        }
        return [header, "Sections:", ...headings.slice(0, 10).map((heading) => `- ${heading}`)].join("\n");
      }

      const section = extract_section(body, section_title);
      if (!section.found || typeof section.content !== "string" || section.content.trim().length === 0) {
        const headings = list_top_headings(body);
        const suggestions = headings.slice(0, 10).map((heading) => `- ${heading}`);
        return [
          `KB section not found: ${section_title}. Try one of:`,
          ...(suggestions.length > 0 ? suggestions : ["- (no sections found)"])
        ].join("\n");
      }

      const preview = truncate_text(section.content, KB_SHOW_MAX_CHARS);
      const lines = [`${header}`, `[section: ${section.title ?? section_title}]`, "", preview.text];
      if (preview.truncated) {
        lines.push("", "(truncated)");
      }
      return lines.join("\n");
    }

    if (subcommand === "preview") {
      const out = await this.exec_kb_preview(ctx);
      return out._preview_text;
    }

    if (subcommand === "apply") {
      const out = await this.exec_kb_apply(ctx);
      return out._reply_text;
    }

    if (subcommand === "cancel") {
      const out = await this.exec_kb_cancel(ctx);
      return out._reply_text;
    }

    if (subcommand === "history") {
      return this.handle_kb_history_command(ctx);
    }

    return "Unknown KB command. Try /kb help";
  }

  private async exec_kb_status(ctx: ReturnType<typeof readCommandCtx>): Promise<{
    _enabled: boolean;
    _source: string;
    _kb_dir: string;
    _files: number;
    _bytes: number;
    _updated_at: number;
  }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "status",
      _params: { _ctx: this.forward_ctx(ctx) }
    });
    if (!is_plain_object(out)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "kb.status returned invalid payload");
    }
    return {
      _enabled: out._enabled === true,
      _source: ensure_non_empty_string(out._source, "_source"),
      _kb_dir: ensure_non_empty_string(out._kb_dir, "_kb_dir"),
      _files: this.ensure_number(out._files, "_files"),
      _bytes: this.ensure_number(out._bytes, "_bytes"),
      _updated_at: this.ensure_number(out._updated_at, "_updated_at")
    };
  }

  private async exec_kb_list_files(ctx: ReturnType<typeof readCommandCtx>): Promise<{
    _kb_dir: string;
    _files: Array<{ _name: string; _bytes: number; _mtime: number }>;
  }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "list_files",
      _params: { _ctx: this.forward_ctx(ctx) }
    });
    if (!is_plain_object(out) || !Array.isArray(out._files)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "kb.list_files returned invalid payload");
    }
    const files = out._files
      .filter((entry): entry is Dict => is_plain_object(entry))
      .map((entry) => ({
        _name: ensure_non_empty_string(entry._name, "_name"),
        _bytes: this.ensure_number(entry._bytes, "_bytes"),
        _mtime: this.ensure_number(entry._mtime, "_mtime")
      }));
    return {
      _kb_dir: ensure_non_empty_string(out._kb_dir, "_kb_dir"),
      _files: files
    };
  }

  private async exec_kb_reload(ctx: ReturnType<typeof readCommandCtx>): Promise<{
    _ok: boolean;
    _files: number;
    _bytes: number;
    _updated_at: number;
  }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "reload",
      _params: { _ctx: this.forward_ctx(ctx) }
    });
    if (!is_plain_object(out)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "kb.reload returned invalid payload");
    }
    return {
      _ok: out._ok === true,
      _files: this.ensure_number(out._files, "_files"),
      _bytes: this.ensure_number(out._bytes, "_bytes"),
      _updated_at: this.ensure_number(out._updated_at, "_updated_at")
    };
  }

  private async exec_kb_show(
    ctx: ReturnType<typeof readCommandCtx>,
    kb_file?: string
  ): Promise<{ kb_id: string; lang: string; content: string; updated_at: number; _kb_file: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "show",
      _params: {
        ...(kb_file ? { _kb_file: kb_file } : {}),
        _ctx: this.forward_ctx(ctx)
      }
    });
    if (!is_plain_object(out)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "kb.show returned invalid payload");
    }
    return {
      kb_id: ensure_non_empty_string(out.kb_id, "kb_id"),
      lang: ensure_non_empty_string(out.lang, "lang"),
      content: typeof out.content === "string" ? out.content : "",
      updated_at: this.ensure_number(out.updated_at, "updated_at"),
      _kb_file: ensure_non_empty_string(out._kb_file, "_kb_file")
    };
  }

  private parse_kb_show_section_arg(raw: string): string | undefined {
    const trimmed = raw.trim();
    if (!trimmed) return undefined;
    const match = trimmed.match(/^section\s+(.+)$/i);
    if (!match) return undefined;
    return ensure_non_empty_string(match[1], "section_title");
  }

  private async exec_kb_propose(
    ctx: ReturnType<typeof readCommandCtx>,
    text: string
  ): Promise<{ _preview_text: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "propose",
      _params: {
        _text: text,
        _channel: ensure_optional_string(ctx.actor?.channel) ?? "telegram",
        _from_user_id: ensure_non_empty_string(ctx.actor?.user_id, "actor.user_id"),
        _ctx: this.forward_ctx(ctx)
      }
    });
    if (!is_plain_object(out)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "kb.propose returned invalid payload");
    }
    return {
      _preview_text: ensure_non_empty_string(out._preview_text, "_preview_text")
    };
  }

  private async exec_kb_set_awaiting_input(
    ctx: ReturnType<typeof readCommandCtx>
  ): Promise<{ _reply_text: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "set_awaiting_input",
      _params: {
        _ctx: this.forward_ctx({
          ...ctx,
          actor: {
            ...(ctx.actor ?? {}),
            channel: ensure_optional_string(ctx.actor?.channel) ?? "telegram"
          }
        })
      }
    });
    if (!is_plain_object(out)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "kb.set_awaiting_input returned invalid payload");
    }
    return {
      _reply_text: ensure_non_empty_string(out._reply_text, "_reply_text")
    };
  }

  private async exec_kb_preview(
    ctx: ReturnType<typeof readCommandCtx>
  ): Promise<{ _preview_text: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "preview",
      _params: { _ctx: this.forward_ctx(ctx) }
    });
    if (!is_plain_object(out)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "kb.preview returned invalid payload");
    }
    return {
      _preview_text: ensure_non_empty_string(out._preview_text, "_preview_text")
    };
  }

  private async exec_kb_apply(
    ctx: ReturnType<typeof readCommandCtx>
  ): Promise<{ _reply_text: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "apply",
      _params: { _ctx: this.forward_ctx(ctx) }
    });
    if (!is_plain_object(out)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "kb.apply returned invalid payload");
    }
    return {
      _reply_text: ensure_non_empty_string(out._reply_text, "_reply_text")
    };
  }

  private async exec_kb_cancel(
    ctx: ReturnType<typeof readCommandCtx>
  ): Promise<{ _reply_text: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "cancel",
      _params: { _ctx: this.forward_ctx(ctx) }
    });
    if (!is_plain_object(out)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "kb.cancel returned invalid payload");
    }
    return {
      _reply_text: ensure_non_empty_string(out._reply_text, "_reply_text")
    };
  }

  private async exec_kb_history(
    ctx: ReturnType<typeof readCommandCtx>,
    limit: number
  ): Promise<{
    _items: Array<{ _actor_user_id: string; _version_id: string; _updated_at: number }>;
  }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "history",
      _params: { _limit: limit, _ctx: this.forward_ctx(ctx) }
    });
    if (!is_plain_object(out) || !Array.isArray(out._items)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "kb.history returned invalid payload");
    }
    return {
      _items: out._items
        .filter((entry): entry is Dict => is_plain_object(entry))
        .map((entry) => ({
          _actor_user_id: ensure_non_empty_string(entry._actor_user_id, "_actor_user_id"),
          _version_id: ensure_non_empty_string(entry._version_id, "_version_id"),
          _updated_at: this.ensure_number(entry._updated_at, "_updated_at")
        }))
    };
  }

  private async exec_broadcast_list_recent(ctx: ReturnType<typeof readCommandCtx>): Promise<string> {
    const out = await _x.execute({
      _module: BROADCASTS_MODULE_NAME,
      _op: "list_recent",
      _params: {
        _limit: 5,
        _ctx: this.forward_ctx(ctx)
      }
    });
    if (!is_plain_object(out) || !Array.isArray(out.items)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "broadcast.list_recent returned invalid payload");
    }
    const items = out.items.filter((entry): entry is Dict => is_plain_object(entry));
    if (items.length === 0) {
      return "Broadcasts:\n- (none)";
    }
    return [
      "Broadcasts:",
      ...items.map((entry) => {
        const broadcast_id = ensure_non_empty_string(entry._id, "_id");
        const status = ensure_non_empty_string(entry._status, "_status");
        const stats = is_plain_object(entry._stats) ? entry._stats : {};
        const sent = typeof stats.sent === "number" && Number.isFinite(stats.sent) ? Math.floor(stats.sent) : 0;
        const failed = typeof stats.failed === "number" && Number.isFinite(stats.failed) ? Math.floor(stats.failed) : 0;
        const total = typeof stats.total === "number" && Number.isFinite(stats.total) ? Math.floor(stats.total) : 0;
        return `- ${broadcast_id} [${status}] sent=${sent} failed=${failed} total=${total}`;
      })
    ].join("\n");
  }

  private async exec_broadcast_send_batch(
    ctx: ReturnType<typeof readCommandCtx>,
    broadcast_id: string
  ): Promise<string> {
    const out = await _x.execute({
      _module: BROADCASTS_MODULE_NAME,
      _op: "broadcast_send_batch",
      _params: {
        _broadcast_id: broadcast_id,
        _limit: 25,
        _ctx: this.forward_ctx(ctx)
      }
    });
    if (!is_plain_object(out)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "broadcast.broadcast_send_batch returned invalid payload");
    }
    const sent = this.ensure_number(out.sent, "sent");
    const failed = this.ensure_number(out.failed, "failed");
    const remaining = this.ensure_number(out.remaining, "remaining");
    return `Broadcast ${broadcast_id}: sent=${sent} failed=${failed} remaining=${remaining}`;
  }

  private async exec_broadcast_preview(
    ctx: ReturnType<typeof readCommandCtx>,
    audience: { roles: string[]; channel: string }
  ): Promise<string> {
    const out = await _x.execute({
      _module: BROADCASTS_MODULE_NAME,
      _op: "preview",
      _params: {
        _audience: audience,
        _limit: 25,
        _ctx: this.forward_ctx(ctx)
      }
    });
    if (!is_plain_object(out) || !is_plain_object(out.audience) || !Array.isArray(out.recipients)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "broadcast.preview returned invalid payload");
    }
    const items = out.recipients.filter((entry): entry is Dict => is_plain_object(entry));
    const lines = [
      `Broadcast preview: channel=${ensure_non_empty_string(out.audience.channel, "audience.channel")} roles=${Array.isArray(out.audience.roles) ? out.audience.roles.join(",") : ""}`,
      `Recipients: ${items.length}`
    ];
    for (const entry of items.slice(0, 10)) {
      const user_id = ensure_non_empty_string(entry.user_id, "user_id");
      const role = ensure_non_empty_string(entry.role, "role");
      const display_name = ensure_non_empty_string(entry.display_name, "display_name");
      const channel_user_id = ensure_non_empty_string(entry.channel_user_id, "channel_user_id");
      lines.push(`- ${user_id} | ${role} | ${display_name} | ${channel_user_id}`);
    }
    if (items.length > 10) {
      lines.push(`- ...(${items.length - 10} more)`);
    }
    const preview = truncate_text(lines.join("\n"), 3500);
    return preview.truncated ? `${preview.text}\n...(truncated)` : preview.text;
  }

  private async exec_broadcast_send(
    ctx: ReturnType<typeof readCommandCtx>,
    text: string,
    audience: { roles: string[]; channel: string }
  ): Promise<string> {
    const out = await _x.execute({
      _module: BROADCASTS_MODULE_NAME,
      _op: "send",
      _params: {
        _message: { text },
        _audience: audience,
        _limit: 25,
        _ctx: this.forward_ctx(ctx)
      }
    });
    if (!is_plain_object(out) || !Array.isArray(out.results)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "broadcast.send returned invalid payload");
    }
    const total = this.ensure_number(out.total, "total");
    const sent_ok = this.ensure_number(out.sent_ok, "sent_ok");
    const sent_err = this.ensure_number(out.sent_err, "sent_err");
    const results = out.results.filter((entry): entry is Dict => is_plain_object(entry));
    const lines = [`Broadcast sent: ok=${sent_ok} err=${sent_err} total=${total}`];
    for (const entry of results.slice(0, 10)) {
      const user_id = ensure_non_empty_string(entry.user_id, "user_id");
      const channel_user_id = ensure_non_empty_string(entry.channel_user_id, "channel_user_id");
      const ok = entry.ok === true;
      const error = ensure_optional_string(entry.error);
      lines.push(`- ${user_id} | ${channel_user_id} | ${ok ? "ok" : `err: ${error ?? "unknown"}`}`);
    }
    if (results.length > 10) {
      lines.push(`- ...(${results.length - 10} more)`);
    }
    const preview = truncate_text(lines.join("\n"), 3500);
    return preview.truncated ? `${preview.text}\n...(truncated)` : preview.text;
  }

  private async exec_users_list(ctx: ReturnType<typeof readCommandCtx>): Promise<string> {
    const out = await _x.execute({
      _module: USERS_MODULE_NAME,
      _op: "list",
      _params: {
        _limit: 50,
        _ctx: this.forward_ctx(ctx)
      }
    });
    if (!is_plain_object(out) || !Array.isArray(out.items)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "users.list returned invalid payload");
    }
    const items = out.items.filter((entry): entry is Dict => is_plain_object(entry));
    if (items.length === 0) {
      return "Users:\n- (none)";
    }
    const lines = [
      "Users:",
      ...items.map((entry) => {
        const id = ensure_non_empty_string(entry._id, "_id");
        const role = ensure_non_empty_string(entry._role, "_role");
        const display_name = ensure_non_empty_string(entry._display_name, "_display_name");
        return `- ${id} | ${role} | ${display_name}`;
      })
    ];
    return truncate_text(lines.join("\n"), 3500).text;
  }

  private async exec_users_debug_identities(
    ctx: ReturnType<typeof readCommandCtx>,
    channel: string
  ): Promise<string> {
    const out = await _x.execute({
      _module: USERS_MODULE_NAME,
      _op: "debug_identities",
      _params: {
        _channel: channel,
        _ctx: this.forward_ctx(ctx)
      }
    });
    if (!Array.isArray(out)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", "users.debug_identities returned invalid payload");
    }
    const items = out.filter((entry): entry is Dict => is_plain_object(entry));
    if (items.length === 0) {
      return `Users (${channel}):\n- (none)`;
    }
    const lines = ["Users (telegram):"];
    for (const entry of items) {
      const user_id = ensure_non_empty_string(entry.user_id, "user_id");
      const role = ensure_non_empty_string(entry.role, "role");
      const display_name = ensure_non_empty_string(entry.display_name, "display_name");
      const identities = Array.isArray(entry.identities)
        ? entry.identities.filter((identity): identity is Dict => is_plain_object(identity))
        : [];
      const chats = identities
        .map((identity) => ensure_optional_string(identity.channel_user_id))
        .filter((value): value is string => typeof value === "string" && value.length > 0);
      lines.push(`- ${user_id} | ${role} | ${display_name}${chats.length > 0 ? ` | ${channel}: ${chats.join(",")}` : ""}`);
    }
    const preview = truncate_text(lines.join("\n"), 3500);
    return preview.truncated ? `${preview.text}\n...(truncated)` : preview.text;
  }

  private ensure_number(value: unknown, field_name: string): number {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      throw new XError("E_ADMIN_CMD_UPSTREAM", `Invalid ${field_name}`);
    }
    return Math.floor(value);
  }

  private forward_ctx(ctx: ReturnType<typeof readCommandCtx>): Dict {
    const out: Dict = {};
    if (typeof ctx.kernel_cap === "string" && ctx.kernel_cap.trim().length > 0) {
      out.kernel_cap = ctx.kernel_cap;
    }
    if (ctx.actor && is_plain_object(ctx.actor)) {
      out.actor = { ...ctx.actor };
    }
    return out;
  }

  private ensure_params(value: unknown): Dict {
    if (value === undefined || value === null) return {};
    if (!is_plain_object(value)) {
      throw new XError("E_ADMIN_CMD_BAD_PARAMS", "Expected params to be an object");
    }
    if (has_function(value)) {
      throw new XError("E_ADMIN_CMD_BAD_PARAMS", "params must be JSON-safe");
    }
    return value;
  }
}

export default AdminCommandsModule;
