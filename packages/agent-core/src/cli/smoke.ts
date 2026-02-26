import process from "node:process";

import { XError, XModule, _x, type XCommandData } from "@xpell/node";

import { AgentRuntime } from "../runtime/AgentRuntime.js";

const TELEGRAM_STUB_MODULE = "telegram";

type Dict = Record<string, unknown>;

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function ensure_non_empty_string(value: unknown, field_name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new XError("E_SMOKE_BAD_PARAMS", `Invalid ${field_name}`);
  }
  return value.trim();
}

class TelegramConnectorStubModule extends XModule {
  static _name = TELEGRAM_STUB_MODULE;

  private _seq = 0;

  constructor() {
    super({ _name: TELEGRAM_STUB_MODULE });
  }

  async _send(xcmd: XCommandData) {
    return this.send_impl(xcmd);
  }
  async _op_send(xcmd: XCommandData) {
    return this.send_impl(xcmd);
  }

  private send_impl(xcmd: XCommandData) {
    const params = is_plain_object(xcmd._params) ? xcmd._params : {};
    const channel_thread_id = ensure_non_empty_string(params.channel_thread_id, "channel_thread_id");
    const text = ensure_non_empty_string(params.text, "text");

    this._seq += 1;
    return {
      accepted: true,
      channel_thread_id,
      text,
      channel_message_id: `tg_out_${this._seq.toString().padStart(6, "0")}`
    };
  }
}

async function exec_xcmd<T>(module_name: string, op: string, params: Record<string, unknown>): Promise<T> {
  const result = await _x.execute({
    _module: module_name,
    _op: op,
    _params: params
  });
  return result as T;
}

async function main(): Promise<void> {
  const runtime = new AgentRuntime({ _port: 0 });

  try {
    await runtime.start();
    _x.loadModule(new TelegramConnectorStubModule());

    await exec_xcmd("channels", "register", {
      channel: "telegram",
      connector_module: TELEGRAM_STUB_MODULE
    });

    const inbound = await exec_xcmd<{ thread_id: string; user_id: string; accepted: boolean }>(
      "channels",
      "route_inbound_message",
      {
        channel: "telegram",
        channel_thread_id: "chat_001",
        channel_user_id: "user_9001",
        text: "hello agent",
        channel_message_id: "in_0001",
        profile: {
          display_name: "Customer One"
        },
        raw: {
          source: "smoke"
        }
      }
    );

    const outbound = await exec_xcmd<{ accepted: boolean; channel_message_id?: string }>("channels", "send_message", {
      channel: "telegram",
      channel_thread_id: "chat_001",
      text: "hello customer"
    });

    const thread_state = await exec_xcmd<{
      thread: { thread_id: string; user_id: string };
      messages: Array<{ message_id: string; direction: string; sender: string; text: string; channel_message_id?: string }>;
    }>("conv", "get_thread", {
      thread_id: inbound.thread_id,
      limit_messages: 20
    });

    console.log(
      JSON.stringify(
        {
          routed: inbound,
          outbound,
          thread: thread_state.thread,
          messages: thread_state.messages
        },
        null,
        2
      )
    );
  } finally {
    await runtime.stop();
  }
}

main().catch((err) => {
  const message = err instanceof Error ? err.message : String(err);
  console.error(`[agent:smoke] ${message}`);
  process.exitCode = 1;
});
