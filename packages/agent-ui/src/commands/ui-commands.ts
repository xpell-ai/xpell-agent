import { _xd, _xem, XUI, XVM } from "@xpell/ui";

import {
  clear_session,
  load_session,
  save_session,
  type ACPAuthSession
} from "../services/auth.js";
import {
  AgentApiError,
  type ACPAbout,
  type ACPAdminUser,
  type ACPAuthUser,
  type ACPSkillRuntimeStatus,
  type ACPSkill,
  type AgentApi
} from "../services/api.js";
import {
  normalize_skill_mode,
  parse_admin_chat_ids,
  validate_admin_create_input,
  validate_admin_delete_input,
  validate_admin_update_input,
  validate_login_form,
  validate_skill_id,
  validate_telegram_settings
} from "../services/validators.js";
import {
  ACTION_ADMIN_CREATE,
  ACTION_ADMIN_DELETE,
  ACTION_ADMIN_UPDATE,
  ACTION_DRAWER_CLOSE,
  ACTION_DRAWER_TOGGLE,
  ACTION_LOGIN,
  ACTION_LOGOUT,
  ACTION_NAVIGATE,
  ACTION_REFRESH_ABOUT,
  ACTION_SETTINGS_SERVER_URL_SAVE,
  ACTION_SETTINGS_THEME_TOGGLE,
  ACTION_SKILLS_DISABLE,
  ACTION_SKILLS_ENABLE,
  ACTION_SKILLS_OPEN_SETTINGS,
  ACTION_SKILL_MODE_POLLING,
  ACTION_SKILL_MODE_WEBHOOK,
  ACTION_SKILL_IMPORT_ENV,
  ACTION_SKILL_POLLING_START,
  ACTION_SKILL_POLLING_STOP,
  ACTION_SKILL_SERVICE_START,
  ACTION_SKILL_SERVICE_STOP,
  ACTION_SKILL_SETTINGS_SAVE,
  ACTION_SKILL_STATUS_REFRESH,
  ACTION_SKILL_VERIFY_AZURE,
  ACTION_SKILL_VERIFY_TOKEN,
  ID_DRAWER,
  ID_DRAWER_BACKDROP,
  ID_SHELL,
  ID_TOAST,
  ID_TOPBAR,
  KEY_ABOUT,
  KEY_ADMIN_USERS,
  KEY_AUTH_IS_AUTHENTICATED,
  KEY_AUTH_USER,
  KEY_SKILLS,
  KEY_SKILL_SETTINGS,
  KEY_UI_ABOUT_AGENT_VERSION_TEXT,
  KEY_UI_ABOUT_CONNECTION_TEXT,
  KEY_UI_ABOUT_SERVER_URL_TEXT,
  KEY_UI_ABOUT_XPELL_VERSION_TEXT,
  KEY_UI_ADMIN_USERS_LIST_TEXT,
  KEY_UI_DRAWER_OPEN,
  KEY_UI_LOGIN_MESSAGE,
  KEY_UI_ROUTE,
  KEY_UI_SETTINGS_SERVER_URL_TEXT,
  KEY_UI_SETTINGS_NOTICE,
  KEY_UI_SETTINGS_THEME_TEXT,
  KEY_UI_SKILLS_LIST_TEXT,
  KEY_UI_SKILL_ADMIN_CHAT_IDS,
  KEY_UI_SKILL_BOT_TOKEN,
  KEY_UI_SKILL_MODE,
  KEY_UI_SKILL_RUNTIME_STATUS_TEXT,
  KEY_UI_SKILL_SELECTED_ID,
  KEY_UI_SKILL_SELECTED_TEXT,
  KEY_UI_SKILL_SETTINGS_SUMMARY_TEXT,
  KEY_UI_STATUS_MESSAGE,
  KEY_UI_THEME,
  REGION_MAIN,
  ROUTE_ABOUT,
  ROUTE_LOGIN,
  ROUTE_SETTINGS,
  ROUTE_SKILLS,
  ROUTE_SKILL_SETTINGS,
  SOURCE_API,
  SOURCE_AUTH,
  SOURCE_BOOT,
  SOURCE_DRAWER,
  SOURCE_NAV,
  SOURCE_SETTINGS,
  SOURCE_UI,
  UI_ACTION_COMMAND,
  UI_COMMAND_OBJECT_IDS
} from "../state/xd_keys.js";

type ActionParams = Record<string, unknown>;

type UICommandsOptions = {
  api: AgentApi;
};

type UICommandsRuntime = {
  register: () => void;
  bootstrap: () => Promise<void>;
};

const THEME_STORAGE_KEY = "acp.theme";
const MASK_SENTINEL = "••••••••";
const AZURE_SKILL_ID = "@xpell/agent-skill-azure";
type ToastVariant = "default" | "success" | "error" | "warn" | "info";

function as_text(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function as_error_text(value: unknown): string {
  if (value instanceof Error) return value.message;
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function deep_clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function read_storage(): Storage | null {
  if (typeof window === "undefined") return null;
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

function read_stored_theme(): "dark" | "light" {
  const storage = read_storage();
  if (!storage) return "dark";
  const raw = storage.getItem(THEME_STORAGE_KEY);
  return raw === "light" ? "light" : "dark";
}

function save_theme(theme: "dark" | "light"): void {
  const storage = read_storage();
  if (!storage) return;
  storage.setItem(THEME_STORAGE_KEY, theme);
}

function set_xdata(key: string, value: unknown, source: string): void {
  _xd.set(key, value, { source });
}

function read_input_value(object_id: string, trim = true): string {
  const object_ref = XUI.getObject(object_id) as { dom?: { value?: unknown } } | undefined;
  const raw_value = object_ref?.dom?.value;
  if (typeof raw_value !== "string") return "";
  return trim ? raw_value.trim() : raw_value;
}

function write_input_value(object_id: string, value: string): void {
  const object_ref = XUI.getObject(object_id) as { dom?: { value?: unknown } } | undefined;
  if (!object_ref?.dom) return;
  object_ref.dom.value = value;
}

function set_login_message(message: string, source: string): void {
  set_xdata(KEY_UI_LOGIN_MESSAGE, message, source);
}

function show_toast(message: string, variant: ToastVariant): void {
  const toast = XUI.getObject(ID_TOAST) as
    | {
        _text?: string;
        _variant?: ToastVariant;
        _auto_close_ms?: number;
        _open?: boolean;
        open?: () => void;
        close?: () => void;
        setOpen?: (open: boolean) => void;
      }
    | undefined;

  if (!toast) return;

  if (!message.trim()) {
    if (typeof toast.close === "function") {
      toast.close();
      return;
    }
    if (typeof toast.setOpen === "function") {
      toast.setOpen(false);
      return;
    }
    toast._open = false;
    return;
  }

  toast._text = message;
  toast._variant = variant;
  toast._auto_close_ms = 3200;

  if (typeof toast.open === "function") {
    toast.open();
    return;
  }
  if (typeof toast.setOpen === "function") {
    toast.setOpen(true);
    return;
  }
  toast._open = true;
}

function set_status_message(message: string, source: string, variant: ToastVariant = "info"): void {
  set_xdata(KEY_UI_STATUS_MESSAGE, message, source);
  show_toast(message, variant);
}

function render_about(about: ACPAbout, source: string): void {
  set_xdata(KEY_ABOUT, deep_clone(about), source);
  set_xdata(KEY_UI_ABOUT_AGENT_VERSION_TEXT, `Agent version: ${about.agent_version}`, source);
  set_xdata(KEY_UI_ABOUT_XPELL_VERSION_TEXT, `Xpell version: ${about.xpell_version}`, source);
  set_xdata(
    KEY_UI_ABOUT_CONNECTION_TEXT,
    `Running status: ${about.connected ? "connected" : "disconnected"}`,
    source
  );

  const server_url = typeof about.server_url === "string" && about.server_url.trim() ? about.server_url : "n/a";
  set_xdata(KEY_UI_ABOUT_SERVER_URL_TEXT, `Server URL: ${server_url}`, source);
}

function render_admin_users(users: ACPAdminUser[], source: string): void {
  set_xdata(KEY_ADMIN_USERS, deep_clone(users), source);

  const text =
    users.length > 0
      ? users.map((user) => `- ${user.id} | ${user.name} | ${user.username} | ${user.role}`).join("\n")
      : "(no admin users)";

  set_xdata(KEY_UI_ADMIN_USERS_LIST_TEXT, text, source);
}

function render_skills(skills: ACPSkill[], source: string): void {
  set_xdata(KEY_SKILLS, deep_clone(skills), source);

  const text =
    skills.length > 0
      ? skills
          .map(
            (skill) =>
              `- ${skill.id} | v${skill.version} | enabled:${skill.enabled ? "yes" : "no"} | status:${skill.status}`
          )
          .join("\n")
      : "(no skills)";

  set_xdata(KEY_UI_SKILLS_LIST_TEXT, text, source);
}

function apply_dom_theme(theme: string): void {
  if (typeof document === "undefined") return;
  const normalized = theme === "light" ? "light" : "dark";
  document.documentElement.setAttribute("data-theme", normalized);
  document.body.setAttribute("data-theme", normalized);
}

function render_theme(theme: string, source: string): void {
  const normalized = theme === "light" ? "light" : "dark";
  set_xdata(KEY_UI_THEME, normalized, source);
  set_xdata(KEY_UI_SETTINGS_THEME_TEXT, `Theme: ${normalized}`, source);
  apply_dom_theme(normalized);
  save_theme(normalized);
}

function render_settings_server_url(server_url: string, source: string): void {
  const normalized = server_url.trim() || "n/a";
  set_xdata(KEY_UI_SETTINGS_SERVER_URL_TEXT, `ACP Server URL: ${normalized}`, source);
}

function render_skill_settings(
  skill_id: string,
  settings: Record<string, unknown>,
  source: string
): void {
  const current = (_xd.get(KEY_SKILL_SETTINGS) as Record<string, Record<string, unknown>> | undefined) ?? {};
  const next = {
    ...current,
    [skill_id]: deep_clone(settings)
  };

  set_xdata(KEY_SKILL_SETTINGS, next, source);
  set_xdata(KEY_UI_SKILL_SELECTED_ID, skill_id, source);
  set_xdata(KEY_UI_SKILL_SELECTED_TEXT, `Selected skill: ${skill_id}`, source);

  const bot_token = typeof settings.bot_token === "string" ? settings.bot_token : "";
  const chat_ids_raw = settings.admin_chat_ids;
  const chat_ids = Array.isArray(chat_ids_raw)
    ? chat_ids_raw.map((value) => String(value)).join(",")
    : typeof chat_ids_raw === "string"
      ? chat_ids_raw
      : "";

  const mode = normalize_skill_mode(String(settings.mode ?? "polling"));

  set_xdata(KEY_UI_SKILL_BOT_TOKEN, bot_token, source);
  set_xdata(KEY_UI_SKILL_ADMIN_CHAT_IDS, chat_ids, source);
  set_xdata(KEY_UI_SKILL_MODE, mode, source);

  const summary = [
    `skill_id: ${skill_id}`,
    `bot_token: ${bot_token ? "configured" : "empty"}`,
    `admin_chat_ids: ${chat_ids || "(none)"}`,
    `mode: ${mode}`
  ].join("\n");

  set_xdata(KEY_UI_SKILL_SETTINGS_SUMMARY_TEXT, summary, source);
}

function render_skill_runtime_status(status: ACPSkillRuntimeStatus, source: string): void {
  if (!status.available) {
    const reason = status.error ? ` (${status.error})` : "";
    set_xdata(KEY_UI_SKILL_RUNTIME_STATUS_TEXT, `Polling: unavailable${reason}`, source);
    return;
  }
  const mode = as_text(status.mode) || "polling";
  set_xdata(KEY_UI_SKILL_RUNTIME_STATUS_TEXT, `Polling: ${status.running ? "running" : "stopped"} (mode: ${mode})`, source);
}

function set_auth_state(is_authenticated: boolean, user: ACPAuthUser | null, source: string): void {
  set_xdata(KEY_AUTH_IS_AUTHENTICATED, is_authenticated, source);
  set_xdata(KEY_AUTH_USER, user ? deep_clone(user) : null, source);
}

function apply_drawer_state(open: boolean): void {
  const drawer = XUI.getObject(ID_DRAWER) as { addClass: (c: string) => void; removeClass: (c: string) => void } | undefined;
  const backdrop = XUI.getObject(ID_DRAWER_BACKDROP) as
    | { addClass: (c: string) => void; removeClass: (c: string) => void }
    | undefined;

  if (open) {
    drawer?.addClass("is-open");
    backdrop?.addClass("is-open");
  } else {
    drawer?.removeClass("is-open");
    backdrop?.removeClass("is-open");
  }
}

function set_drawer_open(open: boolean, source: string): void {
  set_xdata(KEY_UI_DRAWER_OPEN, open, source);
  apply_drawer_state(open);
}

function apply_shell_state(): void {
  const shell = XUI.getObject(ID_SHELL) as { addClass: (c: string) => void; removeClass: (c: string) => void } | undefined;
  const topbar = XUI.getObject(ID_TOPBAR) as { show: () => void; hide: () => void } | undefined;

  const is_authenticated = _xd.get(KEY_AUTH_IS_AUTHENTICATED) === true;

  if (is_authenticated) {
    shell?.removeClass("acp-shell-no-auth");
    topbar?.show();
  } else {
    shell?.addClass("acp-shell-no-auth");
    topbar?.hide();
    set_drawer_open(false, SOURCE_UI);
  }
}

export function create_ui_commands(opts: UICommandsOptions): UICommandsRuntime {
  const { api } = opts;
  let events_bound = false;

  const refresh_about = async (): Promise<void> => {
    const about = await api.system.getAbout();
    render_about(about, SOURCE_API);
    render_settings_server_url(api.system.getServerUrl(), SOURCE_API);
  };

  const refresh_admin_users = async (): Promise<void> => {
    const users = await api.users.listAdmins();
    render_admin_users(users, SOURCE_API);
  };

  const refresh_skills = async (): Promise<void> => {
    const skills = await api.skills.list();
    render_skills(skills, SOURCE_API);

    if (skills.length === 0) {
      set_xdata(KEY_UI_SKILL_SELECTED_ID, "", SOURCE_API);
      set_xdata(KEY_UI_SKILL_SELECTED_TEXT, "Selected skill: (none)", SOURCE_API);
      return;
    }

    const selected = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const fallback = selected || skills[0].id;
    const settings = await api.skills.getSettings(fallback);
    render_skill_settings(fallback, settings, SOURCE_API);
  };

  const refresh_skill_runtime_status = async (skill_id: string, source: string): Promise<void> => {
    const status = await api.skills.getRuntimeStatus(skill_id);
    render_skill_runtime_status(status, source);
  };

  const navigate_to = async (route: string): Promise<void> => {
    const requested = route.trim() || ROUTE_LOGIN;
    const is_authenticated = _xd.get(KEY_AUTH_IS_AUTHENTICATED) === true;

    let target = requested;

    if (!is_authenticated && target !== ROUTE_LOGIN) {
      target = ROUTE_LOGIN;
    }

    if (is_authenticated && target === ROUTE_LOGIN) {
      target = ROUTE_ABOUT;
    }

    if (target === ROUTE_SKILL_SETTINGS) {
      const selected_skill = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
      if (!selected_skill) {
        set_status_message("Select a skill first from Skills page.", SOURCE_NAV);
        target = ROUTE_SKILLS;
      }
    }

    set_xdata(KEY_UI_ROUTE, target, SOURCE_NAV);
    await XVM.navigate(target, { region: REGION_MAIN });
    set_drawer_open(false, SOURCE_DRAWER);
    apply_shell_state();
    bind_action_commands();

    if (target === ROUTE_SETTINGS) {
      write_input_value("settings-server-url-input", api.system.getServerUrl());
    }
  };

  const resolve_skill_id_from_ui = (): string => {
    const from_input = read_input_value("skills-skill-id-input");
    if (from_input) return from_input;

    const from_selected = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    if (from_selected) return from_selected;

    const skills = (_xd.get(KEY_SKILLS) as ACPSkill[] | undefined) ?? [];
    if (skills.length > 0) return skills[0].id;
    return "";
  };

  const login = async (): Promise<void> => {
    const identifier = read_input_value("acp-login-username");
    const password = read_input_value("acp-login-password", false);

    const validation = validate_login_form(identifier, password);
    if (!validation.ok) {
      set_login_message(validation.error, SOURCE_AUTH);
      return;
    }

    const session = await api.auth.login(identifier, password);
    save_session(session as ACPAuthSession);
    set_auth_state(true, session.user, SOURCE_AUTH);

    set_login_message("", SOURCE_AUTH);
    set_status_message(`Logged in as ${session.user.name}.`, SOURCE_AUTH);

    await refresh_about();
    await refresh_admin_users();
    await refresh_skills();
    await navigate_to(ROUTE_ABOUT);
  };

  const logout = async (): Promise<void> => {
    const session = load_session();
    await api.auth.logout(session?.token);
    clear_session();

    set_auth_state(false, null, SOURCE_AUTH);
    set_login_message("Session ended.", SOURCE_AUTH);
    set_status_message("Logged out.", SOURCE_AUTH);

    await navigate_to(ROUTE_LOGIN);
  };

  const admin_create = async (): Promise<void> => {
    const name = read_input_value("admin-create-name-input");
    const username = read_input_value("admin-create-username-input");
    const password = read_input_value("admin-create-password-input", false);

    const validation = validate_admin_create_input(name, username, password);
    if (!validation.ok) {
      set_status_message(validation.error, SOURCE_UI, "error");
      return;
    }

    await api.users.createAdmin({ name, username, password });
    await refresh_admin_users();

    write_input_value("admin-create-name-input", "");
    write_input_value("admin-create-username-input", "");
    write_input_value("admin-create-password-input", "");

    set_status_message("Admin created.", SOURCE_UI);
  };

  const admin_update = async (): Promise<void> => {
    const id = read_input_value("admin-edit-id-input");
    const name = read_input_value("admin-edit-name-input");
    const username = read_input_value("admin-edit-username-input");
    const password = read_input_value("admin-edit-password-input", false);

    const validation = validate_admin_update_input(id, name, username, password);
    if (!validation.ok) {
      set_status_message(validation.error, SOURCE_UI, "error");
      return;
    }

    await api.users.updateAdmin({
      id,
      ...(name ? { name } : {}),
      ...(username ? { username } : {}),
      ...(password ? { password } : {})
    });

    await refresh_admin_users();
    set_status_message(`Admin '${id}' updated.`, SOURCE_UI);
  };

  const admin_delete = async (): Promise<void> => {
    const id = read_input_value("admin-edit-id-input");
    const validation = validate_admin_delete_input(id);
    if (!validation.ok) {
      set_status_message(validation.error, SOURCE_UI, "error");
      return;
    }

    await api.users.deleteAdmin(id);
    await refresh_admin_users();

    write_input_value("admin-edit-id-input", "");
    set_status_message(`Admin '${id}' deleted.`, SOURCE_UI);
  };

  const skills_enable = async (): Promise<void> => {
    const skill_id = resolve_skill_id_from_ui();
    const validation = validate_skill_id(skill_id);
    if (!validation.ok) {
      set_status_message(validation.error, SOURCE_UI, "error");
      return;
    }

    await api.skills.enable(skill_id);
    await refresh_skills();
    set_status_message(`Skill '${skill_id}' enabled.`, SOURCE_UI);
  };

  const skills_disable = async (): Promise<void> => {
    const skill_id = resolve_skill_id_from_ui();
    const validation = validate_skill_id(skill_id);
    if (!validation.ok) {
      set_status_message(validation.error, SOURCE_UI, "error");
      return;
    }

    await api.skills.disable(skill_id);
    await refresh_skills();
    set_status_message(`Skill '${skill_id}' disabled.`, SOURCE_UI);
  };

  const skills_open_settings = async (): Promise<void> => {
    const skill_id = resolve_skill_id_from_ui();
    const validation = validate_skill_id(skill_id);
    if (!validation.ok) {
      set_status_message(validation.error, SOURCE_UI, "error");
      return;
    }

    const settings = await api.skills.getSettings(skill_id);
    render_skill_settings(skill_id, settings, SOURCE_UI);
    await refresh_skill_runtime_status(skill_id, SOURCE_UI);
    set_status_message("", SOURCE_UI);

    await navigate_to(ROUTE_SKILL_SETTINGS);
  };

  const skill_mode_polling = (): void => {
    set_xdata(KEY_UI_SKILL_MODE, "polling", SOURCE_SETTINGS);
    write_input_value("skill-settings-mode-input", "polling");
    set_status_message("Telegram mode set to polling.", SOURCE_SETTINGS);
  };

  const skill_mode_webhook = (): void => {
    set_xdata(KEY_UI_SKILL_MODE, "webhook", SOURCE_SETTINGS);
    write_input_value("skill-settings-mode-input", "webhook");
    set_status_message("Webhook mode is marked coming in v1.", SOURCE_SETTINGS);
  };

  const skill_settings_save = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    const bot_token = read_input_value("skill-settings-bot-token-input", false).trim();
    const admin_chat_ids_input = read_input_value("skill-settings-admin-chat-ids-input", false);
    const mode_input = read_input_value("skill-settings-mode-input", false);

    const mode = normalize_skill_mode(mode_input || as_text(_xd.get(KEY_UI_SKILL_MODE)));
    const validation_mode = validate_telegram_settings(mode);
    if (!validation_mode.ok) {
      set_status_message(validation_mode.error, SOURCE_SETTINGS, "error");
      return;
    }

    const admin_chat_ids = parse_admin_chat_ids(admin_chat_ids_input);

    const next_settings: Record<string, unknown> = {
      bot_token,
      admin_chat_ids,
      mode
    };

    const stored_settings = await api.skills.updateSettings(skill_id, next_settings);
    render_skill_settings(skill_id, stored_settings, SOURCE_SETTINGS);
    await refresh_skill_runtime_status(skill_id, SOURCE_SETTINGS);

    if (mode === "webhook") {
      set_status_message("Saved. Webhook mode is stored but activation is coming in v1.", SOURCE_SETTINGS);
    } else {
      set_status_message("Skill settings saved.", SOURCE_SETTINGS);
    }
  };

  const skill_status_refresh = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }
    await refresh_skill_runtime_status(skill_id, SOURCE_SETTINGS);
    set_status_message("Polling status refreshed.", SOURCE_SETTINGS);
  };

  const skill_service_start = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    await api.skills.enable(skill_id);
    await refresh_skills();
    await refresh_skill_runtime_status(skill_id, SOURCE_SETTINGS);
    set_status_message(`Skill service '${skill_id}' started (enabled).`, SOURCE_SETTINGS);
  };

  const skill_service_stop = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    await api.skills.disable(skill_id);
    await refresh_skills();
    await refresh_skill_runtime_status(skill_id, SOURCE_SETTINGS);
    set_status_message(`Skill service '${skill_id}' stopped (disabled).`, SOURCE_SETTINGS);
  };

  const skill_polling_start = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    const status = await api.skills.startPolling(skill_id);
    render_skill_runtime_status(status, SOURCE_SETTINGS);
    set_status_message("Polling start requested.", SOURCE_SETTINGS);
  };

  const skill_polling_stop = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    const status = await api.skills.stopPolling(skill_id);
    render_skill_runtime_status(status, SOURCE_SETTINGS);
    set_status_message("Polling stop requested.", SOURCE_SETTINGS);
  };

  const skill_verify_token = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    const input_token = read_input_value("skill-settings-bot-token-input", false).trim();
    const token_to_verify =
      input_token.length > 0 && input_token !== MASK_SENTINEL ? input_token : undefined;
    const result = await api.skills.verifyToken(skill_id, token_to_verify);

    if (result.valid) {
      const username = as_text(result.bot?.username);
      const bot_id = as_text(result.bot?.id);
      const bot_text =
        username.length > 0
          ? `@${username}${bot_id ? ` (id:${bot_id})` : ""}`
          : bot_id
            ? `id:${bot_id}`
            : "bot";
      const source = result.source === "input" ? "input token" : "configured token";
      set_status_message(`Token valid (${source}): ${bot_text}.`, SOURCE_SETTINGS);
      return;
    }

    const source = result.source === "input" ? "input token" : "configured token";
    const reason = as_text(result.error) || "invalid token";
    set_status_message(`Token invalid (${source}): ${reason}.`, SOURCE_SETTINGS, "error");
  };

  const skill_import_env = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    if (skill_id !== AZURE_SKILL_ID) {
      set_status_message("Env import is available only for Azure skill.", SOURCE_SETTINGS, "warn");
      return;
    }

    const result = await api.skills.importEnv(skill_id);
    const refreshed_settings = await api.skills.getSettings(skill_id);
    render_skill_settings(skill_id, refreshed_settings, SOURCE_SETTINGS);
    await refresh_skill_runtime_status(skill_id, SOURCE_SETTINGS);

    const imported_count =
      typeof result.imported_count === "number" && Number.isFinite(result.imported_count)
        ? Math.max(0, Math.floor(result.imported_count))
        : 0;

    if (imported_count > 0) {
      const keys_text = result.imported_keys.length > 0 ? ` (${result.imported_keys.join(", ")})` : "";
      set_status_message(`Imported ${imported_count} env key(s)${keys_text}.`, SOURCE_SETTINGS, "success");
      return;
    }

    const detail = as_text(result.detail);
    set_status_message(
      detail ? `No env vars imported: ${detail}.` : "No env vars imported.",
      SOURCE_SETTINGS,
      "warn"
    );
  };

  const skill_verify_azure = async (): Promise<void> => {
    const skill_id = as_text(_xd.get(KEY_UI_SKILL_SELECTED_ID));
    const validation_skill = validate_skill_id(skill_id);
    if (!validation_skill.ok) {
      set_status_message(validation_skill.error, SOURCE_SETTINGS, "error");
      return;
    }

    if (skill_id !== AZURE_SKILL_ID) {
      set_status_message("Azure key verification is available only for Azure skill.", SOURCE_SETTINGS, "warn");
      return;
    }

    const result = await api.skills.testAzureConnection(skill_id);
    const openai_text = `openai=${result.openai.ok ? "ok" : "fail"}${result.openai.detail ? ` (${result.openai.detail})` : ""}`;
    const speech_text = `speech=${result.speech.ok ? "ok" : "fail"}${result.speech.detail ? ` (${result.speech.detail})` : ""}`;
    const variant: ToastVariant = result.openai.ok || result.speech.ok ? "success" : "warn";
    set_status_message(`Azure verify: ${openai_text}; ${speech_text}`, SOURCE_SETTINGS, variant);
  };

  const settings_theme_toggle = (): void => {
    const current = as_text(_xd.get(KEY_UI_THEME)) === "light" ? "light" : "dark";
    const next = current === "dark" ? "light" : "dark";
    render_theme(next, SOURCE_SETTINGS);
    set_xdata(KEY_UI_SETTINGS_NOTICE, `Theme changed to ${next}.`, SOURCE_SETTINGS);
  };

  const settings_server_url_save = async (): Promise<void> => {
    const raw_input = read_input_value("settings-server-url-input", false);
    const next_url = api.system.setServerUrl(raw_input);
    render_settings_server_url(next_url, SOURCE_SETTINGS);
    write_input_value("settings-server-url-input", next_url);
    set_xdata(KEY_UI_SETTINGS_NOTICE, "Server URL saved.", SOURCE_SETTINGS);
    await refresh_about();
  };

  const dispatch_action = async (params: ActionParams): Promise<void> => {
    const action = as_text(params._action);

    switch (action) {
      case ACTION_NAVIGATE:
        await navigate_to(as_text(params._route));
        return;

      case ACTION_LOGIN:
        await login();
        return;

      case ACTION_LOGOUT:
        await logout();
        return;

      case ACTION_DRAWER_TOGGLE: {
        const is_open = _xd.get(KEY_UI_DRAWER_OPEN) === true;
        set_drawer_open(!is_open, SOURCE_DRAWER);
        return;
      }

      case ACTION_DRAWER_CLOSE:
        set_drawer_open(false, SOURCE_DRAWER);
        return;

      case ACTION_REFRESH_ABOUT:
        await refresh_about();
        set_status_message("About refreshed.", SOURCE_API);
        return;

      case ACTION_ADMIN_CREATE:
        await admin_create();
        return;

      case ACTION_ADMIN_UPDATE:
        await admin_update();
        return;

      case ACTION_ADMIN_DELETE:
        await admin_delete();
        return;

      case ACTION_SKILLS_ENABLE:
        await skills_enable();
        return;

      case ACTION_SKILLS_DISABLE:
        await skills_disable();
        return;

      case ACTION_SKILLS_OPEN_SETTINGS:
        await skills_open_settings();
        return;

      case ACTION_SKILL_MODE_POLLING:
        skill_mode_polling();
        return;

      case ACTION_SKILL_MODE_WEBHOOK:
        skill_mode_webhook();
        return;

      case ACTION_SKILL_SETTINGS_SAVE:
        await skill_settings_save();
        return;

      case ACTION_SKILL_STATUS_REFRESH:
        await skill_status_refresh();
        return;

      case ACTION_SKILL_SERVICE_START:
        await skill_service_start();
        return;

      case ACTION_SKILL_SERVICE_STOP:
        await skill_service_stop();
        return;

      case ACTION_SKILL_POLLING_START:
        await skill_polling_start();
        return;

      case ACTION_SKILL_POLLING_STOP:
        await skill_polling_stop();
        return;

      case ACTION_SKILL_VERIFY_TOKEN:
        await skill_verify_token();
        return;

      case ACTION_SKILL_IMPORT_ENV:
        await skill_import_env();
        return;

      case ACTION_SKILL_VERIFY_AZURE:
        await skill_verify_azure();
        return;

      case ACTION_SETTINGS_THEME_TOGGLE:
        settings_theme_toggle();
        return;

      case ACTION_SETTINGS_SERVER_URL_SAVE:
        await settings_server_url_save();
        return;

      default:
        set_status_message(`Unknown action '${action}'.`, SOURCE_UI, "warn");
    }
  };

  const force_login = async (reason: string): Promise<void> => {
    clear_session();
    set_auth_state(false, null, SOURCE_AUTH);
    set_drawer_open(false, SOURCE_DRAWER);
    set_login_message(reason, SOURCE_AUTH);
    set_status_message(reason, SOURCE_AUTH, "warn");
    await navigate_to(ROUTE_LOGIN);
  };

  const bind_action_commands = (): void => {
    for (const object_id of UI_COMMAND_OBJECT_IDS) {
      const object_ref = XUI.getObject(object_id) as
        | {
            addNanoCommand: (name: string, handler: (cmd: unknown) => Promise<void>) => void;
            __acp_action_bound?: boolean;
          }
        | undefined;

      if (!object_ref) continue;
      if (object_ref.__acp_action_bound === true) continue;

      object_ref.addNanoCommand(UI_ACTION_COMMAND, async (xcmd: unknown) => {
        const params =
          xcmd && typeof xcmd === "object" && (xcmd as { _params?: unknown })._params
            ? ((xcmd as { _params: ActionParams })._params as ActionParams)
            : {};
        const action = as_text(params._action);

        try {
          await dispatch_action(params);
        } catch (error) {
          if (error instanceof AgentApiError && error.code === "E_USERS_AUTH_FAILED" && action === ACTION_LOGIN) {
            const msg = error.message || "Invalid username or password.";
            set_login_message(msg, SOURCE_AUTH);
            set_status_message(msg, SOURCE_AUTH, "error");
            return;
          }

          if (error instanceof AgentApiError && error.auth_required && action !== ACTION_LOGIN) {
            await force_login("Session expired. Please login again.");
            return;
          }

          const text = as_error_text(error);
          set_status_message(text, SOURCE_UI, "error");
          set_login_message(text, SOURCE_UI);
        }
      });

      object_ref.__acp_action_bound = true;
    }
  };

  const bind_events_once = (): void => {
    if (events_bound) return;
    events_bound = true;

    _xem.on("xvm:view-rendered", () => {
      bind_action_commands();
      apply_shell_state();
    });
  };

  const seed_state = (): void => {
    set_auth_state(false, null, SOURCE_BOOT);
    set_xdata(KEY_UI_DRAWER_OPEN, false, SOURCE_BOOT);
    set_xdata(KEY_UI_ROUTE, ROUTE_LOGIN, SOURCE_BOOT);

    render_theme(read_stored_theme(), SOURCE_BOOT);
    render_settings_server_url(api.system.getServerUrl(), SOURCE_BOOT);
    set_xdata(KEY_UI_SETTINGS_NOTICE, "", SOURCE_BOOT);

    render_about(
      {
        agent_version: "unknown",
        xpell_version: "unknown",
        connected: false,
        server_url: "n/a"
      },
      SOURCE_BOOT
    );

    render_admin_users([], SOURCE_BOOT);
    render_skills([], SOURCE_BOOT);

    set_xdata(KEY_SKILL_SETTINGS, {}, SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_SELECTED_ID, "", SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_SELECTED_TEXT, "Selected skill: (none)", SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_BOT_TOKEN, "", SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_ADMIN_CHAT_IDS, "", SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_MODE, "polling", SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_SETTINGS_SUMMARY_TEXT, "(no skill selected)", SOURCE_BOOT);
    set_xdata(KEY_UI_SKILL_RUNTIME_STATUS_TEXT, "Polling: unknown", SOURCE_BOOT);

    set_login_message("", SOURCE_BOOT);
    set_status_message("", SOURCE_BOOT);
  };

  const restore_session = async (): Promise<boolean> => {
    const session = load_session();
    if (!session || !session.token) return false;

    try {
      const current = await api.auth.session(session.token);
      if (!current.is_authenticated || !current.user) {
        clear_session();
        set_auth_state(false, null, SOURCE_AUTH);
        return false;
      }

      const next_session: ACPAuthSession = { token: session.token, user: current.user };
      save_session(next_session);
      set_auth_state(true, current.user, SOURCE_AUTH);
      return true;
    } catch {
      clear_session();
      set_auth_state(false, null, SOURCE_AUTH);
      return false;
    }
  };

  const bootstrap = async (): Promise<void> => {
    seed_state();
    await restore_session();

    bind_action_commands();
    bind_events_once();

    await refresh_about();

    if (_xd.get(KEY_AUTH_IS_AUTHENTICATED) === true) {
      try {
        await refresh_admin_users();
        await refresh_skills();
        await navigate_to(ROUTE_ABOUT);
      } catch (error) {
        if (error instanceof AgentApiError && error.auth_required) {
          await force_login("Session expired. Please login again.");
        } else {
          throw error;
        }
      }
    } else {
      await navigate_to(ROUTE_LOGIN);
      set_login_message("Use agent admin credentials to login.", SOURCE_BOOT);
    }

    apply_shell_state();
  };

  return {
    register: bind_action_commands,
    bootstrap
  };
}

export default create_ui_commands;
