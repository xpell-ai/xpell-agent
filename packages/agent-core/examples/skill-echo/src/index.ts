import { XError, XModule, type XCommandData } from "@xpell/node";

type SkillContext = {
  skill: { id: string; version: string };
  registerModule(moduleInstance: XModule): void;
  execute(moduleName: string, op: string, params?: Record<string, unknown>): Promise<unknown>;
  log(level: "debug" | "info" | "warn" | "error", msg: string, meta?: unknown): void;
};

function ensure_non_empty_string(value: unknown, field_name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new XError("E_ECHO_BAD_PARAMS", `Invalid ${field_name}`);
  }
  return value.trim();
}

class EchoModule extends XModule {
  static _name = "echo";

  private _enabled = true;
  private _message_seq = 0;

  constructor() {
    super({ _name: "echo" });
  }

  async _say(xcmd: XCommandData) {
    return this.say_impl(xcmd);
  }
  async _op_say(xcmd: XCommandData) {
    return this.say_impl(xcmd);
  }

  async _send(xcmd: XCommandData) {
    return this.send_impl(xcmd);
  }
  async _op_send(xcmd: XCommandData) {
    return this.send_impl(xcmd);
  }

  async _disable(_xcmd: XCommandData) {
    this._enabled = false;
    return { ok: true };
  }
  async _op_disable(xcmd: XCommandData) {
    return this._disable(xcmd);
  }

  async _enable(_xcmd: XCommandData) {
    this._enabled = true;
    return { ok: true };
  }
  async _op_enable(xcmd: XCommandData) {
    return this._enable(xcmd);
  }

  private say_impl(xcmd: XCommandData) {
    if (!this._enabled) {
      throw new XError("E_ECHO_DISABLED", "echo module is disabled");
    }
    const params = (xcmd._params ?? {}) as Record<string, unknown>;
    const text = ensure_non_empty_string(params.text, "text");
    return { text };
  }

  private send_impl(xcmd: XCommandData) {
    if (!this._enabled) {
      throw new XError("E_ECHO_DISABLED", "echo module is disabled");
    }
    const params = (xcmd._params ?? {}) as Record<string, unknown>;
    const text = ensure_non_empty_string(params.text, "text");
    const channel_thread_id = ensure_non_empty_string(params.channel_thread_id, "channel_thread_id");
    this._message_seq += 1;
    return {
      accepted: true,
      channel_thread_id,
      text,
      channel_message_id: `echo_out_${this._message_seq.toString().padStart(6, "0")}`
    };
  }
}

export const skill = {
  id: "xpell-agent-skill-echo",
  version: "0.1.0-alpha.0",
  name: "Echo Skill",
  description: "Minimal local example skill for validating SkillManager loading and capability gates.",
  capabilities: {
    kernel_ops: ["channels.configure"],
    channels: ["echo"],
    network: false
  },
  async onEnable(ctx: SkillContext): Promise<void> {
    ctx.registerModule(new EchoModule());
    ctx.log("info", "echo module registered", { skill: ctx.skill.id });

    try {
      await ctx.execute("channels", "list", {});
      await ctx.execute("channels", "register", {
        channel: "echo",
        connector_module: "echo"
      });
      await ctx.execute("channels", "configure", {
        channel: "echo",
        config: { mode: "echo" }
      });
      ctx.log("info", "channels registration/configuration completed", { channel: "echo" });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      ctx.log("warn", "channels module unavailable, skipping channel setup", { error: message });
    }
  },
  async onDisable(ctx: SkillContext): Promise<void> {
    ctx.log("info", "echo skill disabled", { skill: ctx.skill.id });
  }
};

export default skill;
