import { XError, XModule } from "@xpell/node";

function ensure_non_empty_string(value, field_name) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new XError("E_ECHO_BAD_PARAMS", `Invalid ${field_name}`);
  }
  return value.trim();
}

class EchoModule extends XModule {
  static _name = "echo";

  _enabled = true;
  _message_seq = 0;

  constructor() {
    super({ _name: "echo" });
  }

  async _say(xcmd) {
    return this.say_impl(xcmd);
  }
  async _op_say(xcmd) {
    return this.say_impl(xcmd);
  }

  async _send(xcmd) {
    return this.send_impl(xcmd);
  }
  async _op_send(xcmd) {
    return this.send_impl(xcmd);
  }

  async _disable(_xcmd) {
    this._enabled = false;
    return { ok: true };
  }
  async _op_disable(xcmd) {
    return this._disable(xcmd);
  }

  async _enable(_xcmd) {
    this._enabled = true;
    return { ok: true };
  }
  async _op_enable(xcmd) {
    return this._enable(xcmd);
  }

  say_impl(xcmd) {
    if (!this._enabled) {
      throw new XError("E_ECHO_DISABLED", "echo module is disabled");
    }
    const params = xcmd._params ?? {};
    const text = ensure_non_empty_string(params.text, "text");
    return { text };
  }

  send_impl(xcmd) {
    if (!this._enabled) {
      throw new XError("E_ECHO_DISABLED", "echo module is disabled");
    }
    const params = xcmd._params ?? {};
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
  async onEnable(ctx) {
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
  async onDisable(ctx) {
    ctx.log("info", "echo skill disabled", { skill: ctx.skill.id });
  }
};

export default skill;
