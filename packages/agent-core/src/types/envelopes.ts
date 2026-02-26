import { XError } from "@xpell/node";

export type AgentServerCtx = {
  _wid: string;
  _sid?: string;
  kernel_cap?: string;
  actor?: {
    user_id?: string;
    role?: "owner" | "admin" | "customer" | "system";
    channel?: string;
    source?: string;
  };
};

type Dict = Record<string, unknown>;

export type AgentXCmd = {
  _module: string;
  _op: string;
  _params?: Record<string, unknown>;
  _ctx?: Record<string, unknown>;
};

export type AgentReqEnvelope = {
  _v: number;
  _id: string;
  _kind: "REQ";
  _sid?: string;
  _payload: AgentXCmd;
};

export type AgentResEnvelope = {
  _v: 2;
  _id: string;
  _kind: "RES";
  _rid: string;
  _sid?: string;
  _ts: number;
  _payload: {
    _ok: boolean;
    _ts: number;
    _pt: number;
    _result: unknown;
  };
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
    throw new XError("E_AGENT_BAD_ENVELOPE", `Invalid ${field_name}`);
  }
  return value.trim();
}

export function assert_req_envelope(env: unknown): asserts env is AgentReqEnvelope {
  if (!is_plain_object(env)) {
    throw new XError("E_AGENT_BAD_ENVELOPE", "Envelope must be a JSON object");
  }
  if (env._v !== 2) {
    throw new XError("E_AGENT_BAD_ENVELOPE", `Unsupported envelope version: ${String((env as any)._v)}`);
  }
  if (env._kind !== "REQ") {
    throw new XError("E_AGENT_BAD_ENVELOPE", `Expected REQ envelope. got=${String((env as any)._kind)}`);
  }
  if (typeof (env as any)._id !== "string" || (env as any)._id.trim().length < 6) {
    throw new XError("E_AGENT_BAD_ENVELOPE", "REQ envelope _id must be a non-empty string");
  }
  if (!is_plain_object((env as any)._payload)) {
    throw new XError("E_AGENT_BAD_ENVELOPE", "REQ payload must be an object");
  }
}

export function assert_xcmd_shape(xcmd: unknown): asserts xcmd is AgentXCmd {
  if (!is_plain_object(xcmd)) {
    throw new XError("E_AGENT_BAD_XCMD", "XCmd must be an object");
  }

  ensure_non_empty_string(xcmd._module, "_module");
  ensure_non_empty_string(xcmd._op, "_op");

  if (xcmd._params !== undefined && !is_plain_object(xcmd._params)) {
    throw new XError("E_AGENT_BAD_XCMD", "XCmd._params must be an object when provided");
  }

  if (has_function(xcmd)) {
    throw new XError("E_AGENT_BAD_XCMD", "XCmd must be JSON-safe (functions are not allowed)");
  }
}

export function context_from_transport_ctx(ctx: { _wid?: unknown; _sid?: unknown }): AgentServerCtx {
  const wid = typeof ctx?._wid === "string" && ctx._wid.trim().length > 0 ? ctx._wid.trim() : "";
  if (!wid) {
    throw new XError("E_AGENT_BAD_CONTEXT", "Missing transport context _wid");
  }

  const sid = typeof ctx?._sid === "string" && ctx._sid.trim().length > 0 ? ctx._sid.trim() : undefined;
  return { _wid: wid, ...(sid ? { _sid: sid } : {}) };
}

export function inject_server_ctx(xcmd: AgentXCmd, server_ctx: AgentServerCtx): void {
  const mutable_cmd = xcmd as AgentXCmd & { _ctx?: Dict };
  const params = is_plain_object(mutable_cmd._params) ? (mutable_cmd._params as Dict) : {};

  const next_ctx: Dict = {
    _wid: server_ctx._wid,
    ...(server_ctx._sid ? { _sid: server_ctx._sid } : {}),
    ...(server_ctx.kernel_cap ? { kernel_cap: server_ctx.kernel_cap } : {}),
    ...(server_ctx.actor ? { actor: { ...server_ctx.actor } } : {})
  };

  // Transport is untrusted: never preserve client-provided actor/kernel fields.
  mutable_cmd._ctx = { ...next_ctx };

  params._ctx = { ...next_ctx };
  params._wid = server_ctx._wid;
  if (server_ctx._sid) params._sid = server_ctx._sid;

  mutable_cmd._params = params as Record<string, any>;
}
