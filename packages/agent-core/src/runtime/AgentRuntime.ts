import { randomBytes, randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import type { Server as HttpServer } from "node:http";

import {
  XDB,
  XDBStorageFS,
  XError,
  XNode,
  _x,
  _xlog,
  createWormholesRestRouter,
  createWormholesWSServer,
  type WHGatewayOptions
} from "@xpell/node";

import { AgentModule } from "../modules/AgentModule.js";
import { ChannelsModule } from "../modules/ChannelsModule.js";
import { ConversationsModule } from "../modules/ConversationsModule.js";
import { KnowledgeModule, KNOWLEDGE_MODULE_NAME } from "../modules/KnowledgeModule.js";
import { SettingsModule } from "../modules/SettingsModule.js";
import { SkillManagerModule, SKILL_MANAGER_MODULE_NAME } from "../modules/SkillManagerModule.js";
import { USERS_MODULE_NAME, UsersModule } from "../modules/UsersModule.js";
import { setKernelCapSecret } from "./guards.js";
import { assert_xcmd_shape, context_from_transport_ctx, inject_server_ctx, type AgentXCmd } from "../types/envelopes.js";

const DEFAULT_PORT = 3090;
const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_WH_PATH = "/wh/v2";
const RUNTIME_VERSION = "0.1.0-alpha.0";
const DEFAULT_AGENT_ID = "xbot";
const DEFAULT_AGENT_ENV = "default";
const DEFAULT_WORK_DIR = "work";
const XDB_ENTITIES_FOLDER = "entities";
const XDB_CACHE_FOLDER = "cache";
const XDB_BACKUP_FOLDER = "backup";
const XDB_OBJECTS_FOLDER = "objects";
const STATIC_MIME_TYPES: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".mjs": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".ico": "image/x-icon",
  ".txt": "text/plain; charset=utf-8"
};

type Dict = Record<string, unknown>;

type AgentRuntimeOptions = {
  _port?: number;
  _host?: string;
  _wh_path?: string;
  _version?: string;
  _public_dir?: string;
  _agent_id?: string;
  _agent_env?: string;
  _work_dir?: string;
  _config_path?: string;
};

function create_xdb_storage(xdb_root: string): XDBStorageFS {
  const root = path.resolve(xdb_root) + path.sep;
  return new XDBStorageFS({
    xdbFolder: root,
    dataFolder: path.join(root, XDB_ENTITIES_FOLDER) + path.sep,
    cacheFolder: path.join(root, XDB_CACHE_FOLDER) + path.sep,
    backupFolder: path.join(root, XDB_BACKUP_FOLDER) + path.sep,
    objectsFolder: path.join(root, XDB_OBJECTS_FOLDER) + path.sep
  });
}

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function trim_non_empty(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function content_type_from_path(file_path: string): string {
  const ext = path.extname(file_path).toLowerCase();
  return STATIC_MIME_TYPES[ext] ?? "application/octet-stream";
}

function safe_decode_uri(pathname: string): string {
  try {
    return decodeURIComponent(pathname || "/");
  } catch {
    return "/";
  }
}

export class AgentRuntime {
  private _port: number;
  private _host: string;
  private _wh_path: string;
  private _version: string;
  private _agent_id: string;
  private _agent_env: string;
  private _work_dir: string;
  private _kb_dir: string;
  private _xdb_root: string;
  private _config_path: string;
  private _public_dir: string;
  private _started_at_ms: number;
  private _kernel_cap: string;

  private _xnode_server?: XNode;
  private _ws_server?: { close: (cb?: (err?: Error) => void) => void };
  private _agent_module?: AgentModule;
  private _users_module?: UsersModule;
  private _conversations_module?: ConversationsModule;
  private _channels_module?: ChannelsModule;
  private _settings_module?: SettingsModule;
  private _knowledge_module?: KnowledgeModule;
  private _skill_manager_module?: SkillManagerModule;
  private _started = false;

  constructor(opts: AgentRuntimeOptions = {}) {
    const env_port = Number.parseInt(process.env.AGENT_PORT ?? "", 10);
    const env_host = process.env.AGENT_HOST ?? "";

    this._port = Number.isFinite(env_port) && env_port > 0 ? env_port : opts._port ?? DEFAULT_PORT;
    this._host = env_host.trim() || opts._host || DEFAULT_HOST;
    this._wh_path = opts._wh_path ?? DEFAULT_WH_PATH;
    this._version = opts._version ?? RUNTIME_VERSION;
    this._agent_id = opts._agent_id ?? DEFAULT_AGENT_ID;
    this._agent_env = process.env.AGENT_ENV?.trim() || opts._agent_env || DEFAULT_AGENT_ENV;
    this._work_dir = path.resolve(process.env.AGENT_WORK_DIR?.trim() || opts._work_dir || path.resolve(process.cwd(), DEFAULT_WORK_DIR));
    this._kb_dir = path.join(this._work_dir, "kb");
    this._xdb_root = path.join(this._work_dir, "xdb");
    this._config_path = path.resolve(opts._config_path ?? path.resolve(process.cwd(), "agent.config.json"));
    this._public_dir = process.env.AGENT_PUBLIC_DIR?.trim() || opts._public_dir || path.resolve(process.cwd(), "public");
    this._started_at_ms = Date.now();
    this._kernel_cap = `kcap_${randomBytes(32).toString("hex")}`;
  }

  async start(): Promise<void> {
    if (this._started) return;

    _x._verbose = true;
    setKernelCapSecret(this._kernel_cap);
    _xlog.log("[agent-core] kernel capability initialized");

    await this.ensure_xdb_paths();
    XDB.init({ storage: create_xdb_storage(this._xdb_root) });
    _x.loadModule(XDB);
    _xlog.log(`[agent-core] module loaded: xdb root=${this._xdb_root}`);

    this._agent_module = new AgentModule({
      _version: this._version,
      _started_at_ms: this._started_at_ms,
      _app_id: this._agent_id,
      _env: this._agent_env
    });
    this._users_module = new UsersModule({ _app_id: this._agent_id, _env: this._agent_env });
    this._conversations_module = new ConversationsModule({ _app_id: this._agent_id, _env: this._agent_env });
    this._channels_module = new ChannelsModule({ _app_id: this._agent_id, _env: this._agent_env });
    this._skill_manager_module = new SkillManagerModule({
      agent_id: this._agent_id,
      version: this._version,
      config_path: this._config_path,
      repo_root: path.resolve(process.cwd(), "../.."),
      package_root: process.cwd(),
      kernel_cap: this._kernel_cap
    });
    this._settings_module = new SettingsModule({
      _work_dir: this._work_dir,
      _resolve_skill_meta: (skill_id) => this._skill_manager_module?.resolve_skill_settings_meta(skill_id)
    });
    this._knowledge_module = new KnowledgeModule({
      _kb_dir: this._kb_dir
    });

    _x.loadModule(this._agent_module);
    _xlog.log("[agent-core] module loaded: agent");
    _x.loadModule(this._users_module);
    _xlog.log("[agent-core] module loaded: users");
    _x.loadModule(this._conversations_module);
    _xlog.log("[agent-core] module loaded: conv");
    _x.loadModule(this._channels_module);
    _xlog.log("[agent-core] module loaded: channels");
    _x.loadModule(this._settings_module);
    _xlog.log("[agent-core] module loaded: settings");
    _x.loadModule(this._knowledge_module);
    _xlog.log("[agent-core] module loaded: kb");
    _x.loadModule(this._skill_manager_module);
    _xlog.log("[agent-core] module loaded: skills");

    const system_ctx = {
      kernel_cap: this._kernel_cap,
      actor: {
        role: "system" as const,
        source: "runtime:start"
      }
    };

    await _x.execute({
      _module: "agent",
      _op: "init_on_boot",
      _params: { _ctx: system_ctx }
    });
    _xlog.log("[agent-core] agent init_on_boot complete");

    await _x.execute({
      _module: USERS_MODULE_NAME,
      _op: "init_on_boot",
      _params: { _ctx: system_ctx }
    });
    _xlog.log("[agent-core] users init_on_boot complete");

    await _x.execute({
      _module: "conv",
      _op: "init_on_boot",
      _params: { _ctx: system_ctx }
    });
    _xlog.log("[agent-core] conv init_on_boot complete");

    await _x.execute({
      _module: "channels",
      _op: "init_on_boot",
      _params: { _ctx: system_ctx }
    });
    _xlog.log("[agent-core] channels init_on_boot complete");

    await _x.execute({
      _module: "settings",
      _op: "init_on_boot",
      _params: { _ctx: system_ctx }
    });
    _xlog.log("[agent-core] settings init_on_boot complete");

    await fs.mkdir(this._kb_dir, { recursive: true });
    await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "init_on_boot",
      _params: { _ctx: system_ctx }
    });
    _xlog.log("[agent-core] kb init_on_boot complete");

    await _x.execute({
      _module: SKILL_MANAGER_MODULE_NAME,
      _op: "reload_enabled",
      _params: { _ctx: system_ctx }
    });

    await this.start_transport();

    this._started = true;
    _xlog.log(`[agent-core] runtime started host=${this._host} port=${this._port} path=${this._wh_path}`);
    _xlog.log(`[agent-core] skill config path=${this._config_path}`);
    _xlog.log(`[agent-core] static public dir=${this._public_dir}`);
  }

  private async start_transport(): Promise<void> {
    // @xpell/node canonical transport API used in runtime:
    // - XNode.start(): HTTP lifecycle
    // - createWormholesRestRouter(): /wh/v2 REST gateway
    // - createWormholesWSServer(): WS gateway on same server/path
    const gateway_opts = this.create_gateway_options();
    const xnode = new XNode();

    await xnode.start({
      work_folder: this._work_dir,
      host: this._host,
      port: this._port,
      web_settings: {
        // We mount Wormholes REST + WS explicitly to inject sid shim + custom auth + static hosting order.
        "enable-wormhole": false
      } as any,
      routes: (app: any) => {
        this.mount_transport_routes(app, gateway_opts);
      }
    });

    const http_server = this.resolve_http_server_from_xnode_private_fields(xnode);
    this._ws_server = createWormholesWSServer(http_server, {
      ...gateway_opts,
      _path: this._wh_path,
      _log_connect: true,
      _log_messages: false
    });
    _xlog.log(`[agent-core] wormholes ws enabled path=${this._wh_path}`);

    this._xnode_server = xnode;
  }

  private create_gateway_options(): WHGatewayOptions {
    return {
      _node: "agent-core",
      _xpell: this._version,
      _caps: ["reqres", "rest", "ping", "ws"],
      _require_auth: false,
      _authorize_req: async (cmd: any, ctx: any) => {
        this.authorize_transport_cmd(cmd as AgentXCmd, ctx);
      }
    };
  }

  private mount_transport_routes(app: any, gateway_opts: WHGatewayOptions): void {
    app.use((req: any, res: any, next: any) => {
      this.apply_cors_headers(res);
      if ((req.method ?? "").toUpperCase() === "OPTIONS") {
        this.send_status(res, 204);
        return;
      }
      next();
    });

    // REST compatibility: existing clients send _sid on the REQ envelope.
    // Canonical REST router reads x-wormholes-sid from headers.
    app.use(this._wh_path, (req: any, _res: any, next: any) => {
      const sid_header = trim_non_empty(req?.headers?.["x-wormholes-sid"]);
      if (sid_header) {
        next();
        return;
      }
      const sid_body = is_plain_object(req?.body) ? trim_non_empty(req.body._sid) : undefined;
      if (sid_body) {
        req.headers["x-wormholes-sid"] = sid_body;
      }
      next();
    });

    app.use(
      createWormholesRestRouter({
        ...gateway_opts,
        _base_path: this._wh_path,
        _log: true
      })
    );

    // Static hosting is mounted into XNode router middleware (no second server).
    app.use((req: any, res: any, next: any) => {
      this.serve_public_asset(req, res)
        .then((served) => {
          if (!served) next();
        })
        .catch(next);
    });
  }

  private authorize_transport_cmd(cmd: AgentXCmd, wh_ctx: any): void {
    assert_xcmd_shape(cmd);

    const transport_client = trim_non_empty(wh_ctx?._route?._from?._client) ?? "";
    const sid = trim_non_empty(wh_ctx?._sid) ?? trim_non_empty((cmd as any)?._params?._sid);
    const wid = trim_non_empty(wh_ctx?._meta?._wid) ?? `wid_${randomUUID().replace(/-/g, "").slice(0, 24)}`;

    const server_ctx = context_from_transport_ctx({
      _wid: wid,
      ...(sid ? { _sid: sid } : {})
    });

    const should_resolve_actor = Boolean(sid && (transport_client === "rest" || sid.startsWith("sid_")));
    const actor = should_resolve_actor ? this.resolve_actor_from_session(sid) : undefined;

    inject_server_ctx(cmd, {
      ...server_ctx,
      ...(actor ? { actor } : {})
    });

    this._skill_manager_module?.assert_module_command_allowed(cmd._module);
  }

  // Temporary bridge until @xpell/node exposes a public getter (for example xnode.getHttpServer()).
  // Do not rely on this outside agent-core.
  private resolve_http_server_from_xnode_private_fields(xnode: XNode): HttpServer {
    const web_server = (xnode as any)?._web_server;
    const http_server = web_server?._secured_web_server ?? web_server?._web_server;
    if (!http_server) {
      if (is_plain_object(web_server)) {
        throw new XError("E_AGENT_TRANSPORT", "XNode server instance is not available", {
          _web_server_keys: Object.keys(web_server)
        });
      }
      throw new XError("E_AGENT_TRANSPORT", "XNode server instance is not available");
    }
    return http_server as HttpServer;
  }

  private apply_cors_headers(res: any): void {
    res.setHeader("access-control-allow-origin", "*");
    res.setHeader("access-control-allow-methods", "GET,POST,OPTIONS");
    res.setHeader(
      "access-control-allow-headers",
      "content-type, authorization, x-wormholes-sid, x-request-id, x-correlation-id"
    );
    res.setHeader("access-control-max-age", "600");
  }

  private resolve_actor_from_session(session_token?: string): { user_id: string; role: "owner" | "admin"; source: string } | undefined {
    if (!session_token || session_token.trim().length === 0) return undefined;
    const actor = this._users_module?.resolve_session_actor(session_token.trim());
    if (!actor) {
      throw new XError("E_AGENT_FORBIDDEN", "Invalid session token");
    }
    return {
      user_id: actor.user_id,
      role: actor.role,
      source: "session"
    };
  }

  private async ensure_xdb_paths(): Promise<void> {
    await fs.mkdir(path.join(this._xdb_root, XDB_ENTITIES_FOLDER), { recursive: true });
    await fs.mkdir(path.join(this._xdb_root, XDB_CACHE_FOLDER), { recursive: true });
    await fs.mkdir(path.join(this._xdb_root, XDB_BACKUP_FOLDER), { recursive: true });
    await fs.mkdir(path.join(this._xdb_root, XDB_OBJECTS_FOLDER), { recursive: true });
  }

  private async serve_public_asset(req: any, res: any): Promise<boolean> {
    const method = String(req?.method ?? "").toUpperCase();
    if (method !== "GET" && method !== "HEAD") return false;

    const request_path = this.read_request_path(req);
    if (request_path === this._wh_path || request_path.startsWith(`${this._wh_path}/`)) {
      return false;
    }
    if (request_path.startsWith("/api/")) {
      return false;
    }

    const clean_path = safe_decode_uri(request_path);
    const normalized_path = clean_path === "/" ? "/index.html" : clean_path;
    const relative_path = normalized_path.replace(/^\/+/, "");
    const root_path = path.resolve(this._public_dir);
    const root_with_sep = root_path.endsWith(path.sep) ? root_path : `${root_path}${path.sep}`;
    const target_path = path.resolve(root_path, relative_path);

    if (target_path !== root_path && !target_path.startsWith(root_with_sep)) {
      this.send_text(res, 403, "forbidden", "text/plain; charset=utf-8", method);
      return true;
    }

    const direct = await this.read_file_if_exists(target_path);
    if (direct) {
      this.send_bytes(res, 200, direct.path, direct.data, method);
      return true;
    }

    const fallback = await this.read_file_if_exists(path.resolve(this._public_dir, "index.html"));
    if (!fallback) return false;

    this.send_bytes(res, 200, fallback.path, fallback.data, method);
    return true;
  }

  private read_request_path(req: any): string {
    const req_path = trim_non_empty(req?.path);
    if (req_path) return req_path;
    const req_url = trim_non_empty(req?.url) ?? "/";
    try {
      return new URL(req_url, "http://localhost").pathname || "/";
    } catch {
      return "/";
    }
  }

  private async read_file_if_exists(file_path: string): Promise<{ path: string; data: Buffer } | null> {
    try {
      const stat = await fs.stat(file_path);
      if (stat.isFile()) {
        const data = await fs.readFile(file_path);
        return { path: file_path, data };
      }

      if (stat.isDirectory()) {
        const index_path = path.join(file_path, "index.html");
        const index_stat = await fs.stat(index_path);
        if (!index_stat.isFile()) return null;
        const data = await fs.readFile(index_path);
        return { path: index_path, data };
      }

      return null;
    } catch {
      return null;
    }
  }

  private send_text(res: any, status_code: number, payload: string, content_type: string, method: string): void {
    if (typeof res.status === "function") {
      res.status(status_code);
    } else {
      res.statusCode = status_code;
    }
    res.setHeader("content-type", content_type);
    res.setHeader("cache-control", "no-store");
    if (method === "HEAD") {
      res.end();
      return;
    }
    if (typeof res.send === "function") {
      res.send(payload);
      return;
    }
    res.end(payload);
  }

  private send_bytes(res: any, status_code: number, file_path: string, payload: Buffer, method: string): void {
    if (typeof res.status === "function") {
      res.status(status_code);
    } else {
      res.statusCode = status_code;
    }
    res.setHeader("content-type", content_type_from_path(file_path));
    res.setHeader("cache-control", "no-store");
    if (method === "HEAD") {
      res.end();
      return;
    }
    if (typeof res.send === "function") {
      res.send(payload);
      return;
    }
    res.end(payload);
  }

  private send_status(res: any, status_code: number): void {
    if (typeof res.status === "function") {
      res.status(status_code);
      res.end();
      return;
    }
    res.statusCode = status_code;
    res.end();
  }

  async stop(): Promise<void> {
    if (!this._started) return;

    await this.close_ws_server();
    await this.close_http_server((this._xnode_server as any)?._web_server?._secured_web_server);
    await this.close_http_server((this._xnode_server as any)?._web_server?._web_server);

    this._started = false;
    this._ws_server = undefined;
    this._xnode_server = undefined;
  }

  private async close_ws_server(): Promise<void> {
    const ws_server = this._ws_server;
    if (!ws_server || typeof ws_server.close !== "function") return;

    await new Promise<void>((resolve, reject) => {
      ws_server.close((err?: Error) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  private async close_http_server(server: any): Promise<void> {
    if (!server || typeof server.close !== "function") return;
    if (server.listening === false) return;

    await new Promise<void>((resolve, reject) => {
      server.close((err: Error | undefined) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
}

export default AgentRuntime;
