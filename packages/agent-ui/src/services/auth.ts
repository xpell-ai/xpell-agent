import type { ACPAuthUser } from "./api.js";

export type ACPAuthSession = {
  token: string;
  user: ACPAuthUser;
};

const TOKEN_STORAGE_KEY = "acp.token";
const USER_STORAGE_KEY = "acp.user";

function read_storage(): Storage | null {
  if (typeof window === "undefined") return null;
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

export function load_session(): ACPAuthSession | null {
  const token = load_token();
  if (!token) return null;

  const storage = read_storage();
  if (!storage) return null;

  const raw = storage.getItem(USER_STORAGE_KEY);
  if (!raw) return null;

  try {
    const parsed = JSON.parse(raw) as ACPAuthUser;
    if (!parsed || typeof parsed !== "object") return null;
    return { token, user: parsed };
  } catch {
    return null;
  }
}

export function save_session(session: ACPAuthSession): void {
  save_token(session.token);

  const storage = read_storage();
  if (!storage) return;
  storage.setItem(USER_STORAGE_KEY, JSON.stringify(session.user));
}

export function clear_session(): void {
  clear_token();

  const storage = read_storage();
  if (!storage) return;
  storage.removeItem(USER_STORAGE_KEY);
}

export function load_token(): string | null {
  const storage = read_storage();
  if (!storage) return null;

  const raw = storage.getItem(TOKEN_STORAGE_KEY);
  if (!raw) return null;

  const token = raw.trim();
  if (!token) return null;
  return token;
}

export function save_token(token: string): void {
  const storage = read_storage();
  if (!storage) return;
  storage.setItem(TOKEN_STORAGE_KEY, token.trim());
}

export function clear_token(): void {
  const storage = read_storage();
  if (!storage) return;
  storage.removeItem(TOKEN_STORAGE_KEY);
}
