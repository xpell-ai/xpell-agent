import { XError } from "@xpell/node";

import type { NormalizedTelegramInbound, TelegramMessage, TelegramUpdate } from "./types.js";

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

function ensure_number(value: unknown, field_name: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new XError("E_TG_BAD_UPDATE", `Invalid ${field_name}`);
  }
  return value;
}

function ensure_object(value: unknown, field_name: string): Dict {
  if (!is_plain_object(value)) {
    throw new XError("E_TG_BAD_UPDATE", `Invalid ${field_name}`);
  }
  if (has_function(value)) {
    throw new XError("E_TG_BAD_UPDATE", `${field_name} must be JSON-safe`);
  }
  return value;
}

function ensure_string_or_undefined(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function parse_message(update: TelegramUpdate): TelegramMessage | undefined {
  return update.message ?? update.edited_message;
}

function stringify_chat_id(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) return String(Math.trunc(value));
  if (typeof value === "string" && value.trim().length > 0) return value.trim();
  throw new XError("E_TG_BAD_UPDATE", "Invalid chat.id");
}

function stringify_user_id(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) return String(Math.trunc(value));
  if (typeof value === "string" && value.trim().length > 0) return value.trim();
  throw new XError("E_TG_BAD_UPDATE", "Invalid from.id");
}

export function normalize_telegram_update(update_input: unknown): NormalizedTelegramInbound | undefined {
  const update_obj = ensure_object(update_input, "update") as TelegramUpdate;
  const update_id = ensure_number((update_obj as any).update_id, "update.update_id");

  const message = parse_message(update_obj);
  if (!message) return undefined;

  const message_obj = ensure_object(message, "update.message") as TelegramMessage;
  const chat_obj = ensure_object((message_obj as any).chat, "update.message.chat");
  const from_obj = ensure_object((message_obj as any).from, "update.message.from");

  const text = ensure_string_or_undefined((message_obj as any).text) ?? ensure_string_or_undefined((message_obj as any).caption);
  if (!text) return undefined;

  const chat_id = stringify_chat_id((chat_obj as any).id);
  const from_id = stringify_user_id((from_obj as any).id);
  const message_id = String(ensure_number((message_obj as any).message_id, "update.message.message_id"));

  return {
    update_id,
    chat_id,
    from_id,
    message_id,
    text,
    profile: {
      ...(ensure_string_or_undefined((from_obj as any).username)
        ? { username: ensure_string_or_undefined((from_obj as any).username) }
        : {}),
      ...(ensure_string_or_undefined((from_obj as any).first_name)
        ? { first_name: ensure_string_or_undefined((from_obj as any).first_name) }
        : {}),
      ...(ensure_string_or_undefined((from_obj as any).last_name)
        ? { last_name: ensure_string_or_undefined((from_obj as any).last_name) }
        : {})
    },
    raw: {
      update_id,
      ...(ensure_string_or_undefined((chat_obj as any).type) ? { chat_type: ensure_string_or_undefined((chat_obj as any).type) } : {}),
      ...(typeof (message_obj as any).date === "number" ? { date: (message_obj as any).date } : {})
    }
  };
}

export function normalize_admin_chat_ids(value: unknown): string[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) {
    throw new XError("E_TG_BAD_PARAMS", "admins.chat_ids must be an array");
  }

  const out: string[] = [];
  const seen = new Set<string>();

  for (const item of value) {
    let normalized: string | undefined;
    if (typeof item === "number" && Number.isFinite(item)) normalized = String(Math.trunc(item));
    else if (typeof item === "string" && item.trim().length > 0) normalized = item.trim();

    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
  }

  return out;
}
