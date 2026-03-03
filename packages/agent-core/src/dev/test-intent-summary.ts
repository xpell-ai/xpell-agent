import { XModule, _x } from "@xpell/node";

import { AgentRuntime } from "../runtime/AgentRuntime.js";

class DevTelegramStubModule extends XModule {
  static _name = "dev_telegram_stub";

  constructor() {
    super({ _name: DevTelegramStubModule._name });
  }

  async _send(): Promise<{ channel_message_id: string; ok: true }> {
    return {
      channel_message_id: "dev_msg_1",
      ok: true
    };
  }

  async _op_send() {
    return this._send();
  }
}

async function main(): Promise<void> {
  const runtime = new AgentRuntime({ _port: 0 });
  (runtime as any).start_transport = async () => {};
  await runtime.start();

  const kernel_cap = String((runtime as any)._kernel_cap ?? "");
  if (!kernel_cap) {
    throw new Error("Missing kernel_cap");
  }

  _x.loadModule(new DevTelegramStubModule());

  const system_ctx = {
    kernel_cap,
    actor: {
      role: "system",
      source: "dev:test-intent-summary"
    }
  };

  await _x.execute({
    _module: "channels",
    _op: "register",
    _params: {
      channel: "telegram",
      connector_module: "dev_telegram_stub",
      _ctx: system_ctx
    }
  });

  const upserted = (await _x.execute({
    _module: "users",
    _op: "upsert_from_channel_identity",
    _params: {
      channel_id: "telegram",
      external_user_id: "dev-admin-1",
      display_name: "Dev Admin"
    }
  })) as { user_id: string };

  await _x.execute({
    _module: "users",
    _op: "set_role",
    _params: {
      _user_id: upserted.user_id,
      _role: "admin",
      _ctx: {
        kernel_cap,
        actor: {
          role: "owner",
          user_id: "dev-owner",
          source: "dev:test-intent-summary"
        }
      }
    }
  });

  const routed = await _x.execute({
    _module: "channels",
    _op: "route_inbound_message",
    _params: {
      _channel: "telegram",
      _channel_user_id: "dev-admin-1",
      _text: "summarize today",
      _ctx: system_ctx
    }
  });

  const summary = await _x.execute({
    _module: "conv",
    _op: "summary_today",
    _params: {
      _ctx: {
        kernel_cap,
        actor: {
          role: "admin",
          user_id: upserted.user_id,
          source: "dev:test-intent-summary"
        }
      }
    }
  });

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        selected_intent: "admin.conv.summary_today",
        routed,
        summary
      },
      null,
      2
    )
  );
}

void main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err instanceof Error ? err.stack || err.message : String(err));
  process.exitCode = 1;
});
