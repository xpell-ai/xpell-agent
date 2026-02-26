import type { ACPSkillMode } from "../state/xd_keys.js";

export type ValidationResult =
  | { ok: true }
  | { ok: false; error: string };

function ok(): ValidationResult {
  return { ok: true };
}

function fail(error: string): ValidationResult {
  return { ok: false, error };
}

export function validate_login_form(identifier: string, password: string): ValidationResult {
  if (!identifier.trim()) return fail("Username/email is required.");
  if (!password) return fail("Password is required.");
  return ok();
}

export function validate_admin_create_input(name: string, username: string, password: string): ValidationResult {
  if (!name.trim()) return fail("Admin name is required.");
  if (!username.trim()) return fail("Admin username/email is required.");
  if (!password) return fail("Admin password is required.");
  return ok();
}

export function validate_admin_update_input(
  id: string,
  name: string,
  username: string,
  password: string
): ValidationResult {
  if (!id.trim()) return fail("Admin id is required for update.");
  if (!name.trim() && !username.trim() && !password) {
    return fail("Provide at least one field to update.");
  }
  return ok();
}

export function validate_admin_delete_input(id: string): ValidationResult {
  if (!id.trim()) return fail("Admin id is required for delete.");
  return ok();
}

export function validate_skill_id(skill_id: string): ValidationResult {
  if (!skill_id.trim()) return fail("Skill id is required.");
  return ok();
}

export function normalize_skill_mode(value: string): ACPSkillMode {
  return value.trim().toLowerCase() === "webhook" ? "webhook" : "polling";
}

export function parse_admin_chat_ids(value: string): string[] {
  if (!value.trim()) return [];
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

export function validate_telegram_settings(mode: ACPSkillMode): ValidationResult {
  if (mode !== "polling" && mode !== "webhook") {
    return fail("Telegram mode must be 'polling' or 'webhook'.");
  }
  return ok();
}

export function validate_agent_language_policy(value: string): ValidationResult {
  const normalized = value.trim().toLowerCase();
  if (normalized === "auto" || normalized === "spanish" || normalized === "english") {
    return ok();
  }
  return fail("Language policy must be Auto, Spanish, or English.");
}
