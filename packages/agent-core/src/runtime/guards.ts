import { XError, type XCommandData } from "@xpell/node";

type Dict = Record<string, unknown>;

export type AgentActorRole = "owner" | "admin" | "customer" | "system";

export type AgentActor = {
  user_id?: string;
  role?: AgentActorRole;
  channel?: string;
  source?: string;
};

export type AgentCommandCtx = {
  _wid?: string;
  _sid?: string;
  kernel_cap?: string;
  actor?: AgentActor;
};

let _kernel_cap_secret = "";

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function to_ctx(value: unknown): AgentCommandCtx {
  if (!is_plain_object(value)) return {};
  const actor_raw = is_plain_object(value.actor) ? value.actor : undefined;
  return {
    ...(typeof value._wid === "string" ? { _wid: value._wid } : {}),
    ...(typeof value._sid === "string" ? { _sid: value._sid } : {}),
    ...(typeof value.kernel_cap === "string" ? { kernel_cap: value.kernel_cap } : {}),
    ...(actor_raw
      ? {
          actor: {
            ...(typeof actor_raw.user_id === "string" ? { user_id: actor_raw.user_id } : {}),
            ...(typeof actor_raw.role === "string" ? { role: actor_raw.role as AgentActorRole } : {}),
            ...(typeof actor_raw.channel === "string" ? { channel: actor_raw.channel } : {}),
            ...(typeof actor_raw.source === "string" ? { source: actor_raw.source } : {})
          }
        }
      : {})
  };
}

export function setKernelCapSecret(kernel_cap: string): void {
  if (typeof kernel_cap !== "string" || kernel_cap.trim().length < 16) {
    throw new XError("E_AGENT_GUARD_INIT", "Invalid kernel capability secret");
  }
  _kernel_cap_secret = kernel_cap.trim();
}

export function readCommandCtx(xcmd: XCommandData): AgentCommandCtx {
  const root_ctx = to_ctx((xcmd as any)?._ctx);
  const params_ctx = to_ctx((xcmd as any)?._params?._ctx);
  return {
    ...root_ctx,
    ...params_ctx,
    ...(params_ctx.actor ? { actor: params_ctx.actor } : root_ctx.actor ? { actor: root_ctx.actor } : {})
  };
}

export function requireKernelCap(ctx: AgentCommandCtx): void {
  if (!_kernel_cap_secret) {
    throw new XError("E_AGENT_GUARD_INIT", "Kernel cap secret is not initialized");
  }
  if (typeof ctx.kernel_cap !== "string" || ctx.kernel_cap.length === 0) {
    throw new XError("E_AGENT_FORBIDDEN", "Missing kernel capability");
  }
  if (ctx.kernel_cap !== _kernel_cap_secret) {
    throw new XError("E_AGENT_FORBIDDEN", "Invalid kernel capability");
  }
}

export function requireActorRole(ctx: AgentCommandCtx, role: AgentActorRole): void {
  const current_role = typeof ctx.actor?.role === "string" ? (ctx.actor.role as AgentActorRole) : undefined;
  if (!current_role) {
    throw new XError("E_AGENT_FORBIDDEN", `Missing required actor role: ${role}`);
  }

  const rank: Record<AgentActorRole, number> = {
    customer: 1,
    admin: 2,
    owner: 3,
    system: 4
  };

  if (rank[current_role] < rank[role]) {
    throw new XError("E_AGENT_FORBIDDEN", `Missing required actor role: ${role}`);
  }
}

export function requireKernelCapOrActorRole(ctx: AgentCommandCtx, role: AgentActorRole): void {
  try {
    requireKernelCap(ctx);
    return;
  } catch {
    requireActorRole(ctx, role);
  }
}
