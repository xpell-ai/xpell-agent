import { XError } from "@xpell/node";

import type { TelegramApiResponse, TelegramBotInfo, TelegramMessage, TelegramParseMode, TelegramUpdate } from "./types.js";

type TelegramApiMethod = "getUpdates" | "sendMessage" | "deleteWebhook" | "getMe";

function ensure_bot_token(bot_token: unknown): string {
  if (typeof bot_token !== "string" || bot_token.trim().length < 10) {
    throw new XError("E_TG_BAD_CONFIG", "bot_token is missing or invalid");
  }
  return bot_token.trim();
}

function ensure_non_empty_string(value: unknown, field_name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new XError("E_TG_BAD_PARAMS", `Invalid ${field_name}`);
  }
  return value.trim();
}

function parse_chat_id(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) return String(Math.trunc(value));
  if (typeof value === "string" && value.trim().length > 0) return value.trim();
  throw new XError("E_TG_BAD_PARAMS", "Invalid chat_id");
}

async function call_telegram_api<T>(
  bot_token_input: unknown,
  method: TelegramApiMethod,
  payload: Record<string, unknown>
): Promise<T> {
  const bot_token = ensure_bot_token(bot_token_input);
  const url = `https://api.telegram.org/bot${bot_token}/${method}`;

  let response: Response;
  try {
    response = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "application/json"
      },
      body: JSON.stringify(payload)
    });
  } catch (err) {
    throw new XError("E_TG_NETWORK", "Telegram API request failed", { _cause: err });
  }

  let body: TelegramApiResponse<T>;
  try {
    body = (await response.json()) as TelegramApiResponse<T>;
  } catch (err) {
    throw new XError("E_TG_API", "Telegram API returned invalid JSON", { _cause: err });
  }

  if (!response.ok || body.ok !== true || body.result === undefined) {
    throw new XError("E_TG_API", body.description ?? `Telegram API error for method ${method}`, {
      _status: response.status,
      _method: method,
      _error_code: body.error_code
    });
  }

  return body.result;
}

export function clamp_poll_timeout(timeout_sec: unknown): number {
  const parsed = Number.parseInt(String(timeout_sec ?? 30), 10);
  if (!Number.isFinite(parsed)) return 30;
  return Math.max(5, Math.min(parsed, 60));
}

export async function telegram_get_updates(params: {
  bot_token: string;
  offset?: number;
  timeout_sec?: number;
}): Promise<TelegramUpdate[]> {
  const timeout = clamp_poll_timeout(params.timeout_sec);

  return call_telegram_api<TelegramUpdate[]>(params.bot_token, "getUpdates", {
    timeout,
    ...(typeof params.offset === "number" ? { offset: params.offset } : {}),
    allowed_updates: ["message", "edited_message"]
  });
}

export async function telegram_send_message(params: {
  bot_token: string;
  chat_id: string | number;
  text: string;
  parse_mode?: TelegramParseMode;
}): Promise<TelegramMessage> {
  const chat_id = parse_chat_id(params.chat_id);
  const text = ensure_non_empty_string(params.text, "text");

  return call_telegram_api<TelegramMessage>(params.bot_token, "sendMessage", {
    chat_id,
    text,
    ...(params.parse_mode ? { parse_mode: params.parse_mode } : {})
  });
}

export async function telegram_set_webhook_stub(params: {
  bot_token: string;
  url: string;
  secret_token?: string;
}): Promise<{ ok: false; reason: string; configured: { url: string; has_secret: boolean } }> {
  ensure_bot_token(params.bot_token);
  const url = ensure_non_empty_string(params.url, "url");

  return {
    ok: false,
    reason: "webhook_not_implemented_in_alpha",
    configured: {
      url,
      has_secret: typeof params.secret_token === "string" && params.secret_token.trim().length > 0
    }
  };
}

export async function telegram_delete_webhook(params: {
  bot_token: string;
  drop_pending_updates?: boolean;
}): Promise<boolean> {
  return call_telegram_api<boolean>(params.bot_token, "deleteWebhook", {
    drop_pending_updates: params.drop_pending_updates === true
  });
}

export async function telegram_get_me(params: {
  bot_token: string;
}): Promise<TelegramBotInfo> {
  return call_telegram_api<TelegramBotInfo>(params.bot_token, "getMe", {});
}
