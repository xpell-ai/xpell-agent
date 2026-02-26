export type ACPTheme = "dark" | "light";
export type ACPSkillMode = "polling" | "webhook";

export const REGION_MAIN = "main";
export const REGION_OVERLAY = "overlay";

export const ROUTE_LOGIN = "/login";
export const ROUTE_ABOUT = "/about";
export const ROUTE_ADMIN_USERS = "/admin-users";
export const ROUTE_SKILLS = "/skills";
export const ROUTE_SKILL_SETTINGS = "/skill-settings/:skillId";
export const ROUTE_SETTINGS = "/settings";

export const VIEW_LOGIN = "view-login";
export const VIEW_ABOUT = "view-about";
export const VIEW_ADMIN_USERS = "view-admin-users";
export const VIEW_SKILLS = "view-skills";
export const VIEW_SKILL_SETTINGS = "view-skill-settings";
export const VIEW_SETTINGS = "view-settings";

export const UI_ACTION_COMMAND = "acp-ui-action";

export const SOURCE_BOOT = "acp:boot";
export const SOURCE_AUTH = "acp:auth";
export const SOURCE_NAV = "acp:navigate";
export const SOURCE_DRAWER = "acp:drawer";
export const SOURCE_API = "acp:api";
export const SOURCE_UI = "acp:ui";
export const SOURCE_SETTINGS = "acp:settings";

// Authoritative XData keys used by ACP v0.1
export const KEY_AUTH_IS_AUTHENTICATED = "acp.auth.is_authenticated";
export const KEY_AUTH_USER = "acp.auth.user";

export const KEY_UI_DRAWER_OPEN = "acp.ui.drawer_open";
export const KEY_UI_ROUTE = "acp.ui.route";
export const KEY_UI_THEME = "acp.ui.theme";

export const KEY_ABOUT = "acp.about";
export const KEY_ADMIN_USERS = "acp.admin_users";
export const KEY_SKILLS = "acp.skills";
export const KEY_SKILL_SETTINGS = "acp.skill_settings";

export const KEY_UI_LOGIN_MESSAGE = "acp.ui.login_message";
export const KEY_UI_STATUS_MESSAGE = "acp.ui.status_message";

export const KEY_UI_ABOUT_AGENT_VERSION_TEXT = "acp.ui.about.agent_version_text";
export const KEY_UI_ABOUT_XPELL_VERSION_TEXT = "acp.ui.about.xpell_version_text";
export const KEY_UI_ABOUT_CONNECTION_TEXT = "acp.ui.about.connection_text";
export const KEY_UI_ABOUT_SERVER_URL_TEXT = "acp.ui.about.server_url_text";

export const KEY_UI_ADMIN_USERS_LIST_TEXT = "acp.ui.admin_users.list_text";
export const KEY_UI_SKILLS_LIST_TEXT = "acp.ui.skills.list_text";

export const KEY_UI_SKILL_SELECTED_ID = "acp.ui.skill_settings.selected_skill_id";
export const KEY_UI_SKILL_SELECTED_TEXT = "acp.ui.skill_settings.selected_skill_text";
export const KEY_UI_SKILL_BOT_TOKEN = "acp.ui.skill_settings.bot_token";
export const KEY_UI_SKILL_ADMIN_CHAT_IDS = "acp.ui.skill_settings.admin_chat_ids";
export const KEY_UI_SKILL_MODE = "acp.ui.skill_settings.mode";
export const KEY_UI_SKILL_SETTINGS_SUMMARY_TEXT = "acp.ui.skill_settings.summary_text";
export const KEY_UI_SKILL_RUNTIME_STATUS_TEXT = "acp.ui.skill_settings.runtime_status_text";

export const KEY_UI_SETTINGS_THEME_TEXT = "acp.ui.settings.theme_text";
export const KEY_UI_SETTINGS_SERVER_URL_TEXT = "acp.ui.settings.server_url_text";
export const KEY_UI_SETTINGS_NOTICE = "acp.ui.settings.notice";

export const ACTION_NAVIGATE = "navigate";
export const ACTION_LOGIN = "login";
export const ACTION_LOGOUT = "logout";
export const ACTION_DRAWER_TOGGLE = "drawer_toggle";
export const ACTION_DRAWER_CLOSE = "drawer_close";

export const ACTION_REFRESH_ABOUT = "refresh_about";

export const ACTION_ADMIN_CREATE = "admin_create";
export const ACTION_ADMIN_UPDATE = "admin_update";
export const ACTION_ADMIN_DELETE = "admin_delete";

export const ACTION_SKILLS_ENABLE = "skills_enable";
export const ACTION_SKILLS_DISABLE = "skills_disable";
export const ACTION_SKILLS_OPEN_SETTINGS = "skills_open_settings";

export const ACTION_SKILL_MODE_POLLING = "skill_mode_polling";
export const ACTION_SKILL_MODE_WEBHOOK = "skill_mode_webhook";
export const ACTION_SKILL_SETTINGS_SAVE = "skill_settings_save";
export const ACTION_SKILL_STATUS_REFRESH = "skill_status_refresh";
export const ACTION_SKILL_SERVICE_START = "skill_service_start";
export const ACTION_SKILL_SERVICE_STOP = "skill_service_stop";
export const ACTION_SKILL_POLLING_START = "skill_polling_start";
export const ACTION_SKILL_POLLING_STOP = "skill_polling_stop";
export const ACTION_SKILL_VERIFY_TOKEN = "skill_verify_token";
export const ACTION_SKILL_IMPORT_ENV = "skill_import_env";
export const ACTION_SKILL_VERIFY_AZURE = "skill_verify_azure";

export const ACTION_SETTINGS_THEME_TOGGLE = "settings_theme_toggle";
export const ACTION_SETTINGS_SERVER_URL_SAVE = "settings_server_url_save";

export const ID_SHELL = "acp-shell";
export const ID_TOPBAR = "acp-shell-topbar";
export const ID_DRAWER = "acp-shell-drawer";
export const ID_DRAWER_BACKDROP = "acp-drawer-backdrop";
export const ID_TOAST = "acp-toast";

export const UI_COMMAND_OBJECT_IDS: string[] = [
  "acp-burger-btn",
  "acp-logout-btn",
  "acp-drawer-backdrop",
  "acp-menu-about-btn",
  "acp-menu-admin-users-btn",
  "acp-menu-skills-btn",
  "acp-menu-settings-btn",
  "acp-menu-logout-btn",
  "login-submit-btn",
  "about-refresh-btn",
  "admin-create-btn",
  "admin-update-btn",
  "admin-delete-btn",
  "skills-enable-btn",
  "skills-disable-btn",
  "skills-open-settings-btn",
  "skill-settings-mode-polling-btn",
  "skill-settings-mode-webhook-btn",
  "skill-settings-status-refresh-btn",
  "skill-settings-service-start-btn",
  "skill-settings-service-stop-btn",
  "skill-settings-polling-start-btn",
  "skill-settings-polling-stop-btn",
  "skill-settings-verify-token-btn",
  "skill-settings-import-env-btn",
  "skill-settings-verify-azure-btn",
  "skill-settings-save-btn",
  "skill-settings-back-btn",
  "settings-theme-btn",
  "settings-save-server-url-btn"
];
