import { _xlog, XUI, type XObjectData, type XVMApp } from "@xpell/ui";

import {
  REGION_MAIN,
  REGION_OVERLAY,
  ROUTE_ABOUT,
  ROUTE_AGENT,
  ROUTE_QAGENT,
  ROUTE_CONVERSATIONS,
  ROUTE_CONVERSATION_DETAILS,
  ROUTE_INTENTS,
  ROUTE_USERS,
  ROUTE_ADMIN_USERS,
  ROUTE_LOGIN,
  ROUTE_SETUP_ADMIN,
  ROUTE_SETTINGS,
  ROUTE_SKILLS,
  ROUTE_SKILL_SETTINGS,
  VIEW_ABOUT,
  VIEW_AGENT,
  VIEW_QAGENT,
  VIEW_CONVERSATIONS,
  VIEW_CONVERSATION_DETAILS,
  VIEW_INTENTS,
  VIEW_USERS,
  VIEW_ADMIN_USERS,
  VIEW_LOGIN,
  VIEW_SETUP_ADMIN,
  VIEW_SETTINGS,
  VIEW_SKILLS,
  VIEW_SKILL_SETTINGS
} from "../state/xd_keys.js";
import about_view_json from "../views/about.view.json";
import agent_view_json from "../views/agent.view.json";
import admin_users_view_json from "../views/admin-users.view.json";
import conversation_thread_view_json from "../views/conversation-thread.view.json";
import conversations_view_json from "../views/conversations.view.json";
import intents_view_json from "../views/intents.view.json";
import login_view_json from "../views/login.view.json";
import qagent_view_json from "../views/qagent.view.json";
import setup_admin_view_json from "../views/setup-admin.view.json";
import settings_view_json from "../views/settings.view.json";
import shell_view_json from "../views/shell.view.json";
import skill_settings_view_json from "../views/skill-settings.view.json";
import skills_view_json from "../views/skills.view.json";
import users_view_json from "../views/users.view.json";

function clone_view<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function attach_skills_action_render(view: XObjectData): void {
  const root = view as Record<string, unknown>;
  const children = Array.isArray(root._children) ? (root._children as Array<Record<string, unknown>>) : [];
  const table_host = children.find((child) => {
    const nested = Array.isArray(child?._children) ? (child._children as Array<Record<string, unknown>>) : [];
    return nested.some((entry) => entry?._id === "skills-table");
  });
  if (!table_host) return;

  const nested = Array.isArray(table_host._children) ? (table_host._children as Array<Record<string, unknown>>) : [];
  const table = nested.find((entry) => entry?._id === "skills-table");
  if (!table) return;

  const columns = Array.isArray(table._columns) ? (table._columns as Array<Record<string, unknown>>) : [];
  const action_column = columns.find((column) => column?.key === "action");
  if (!action_column) return;

  action_column.render = (_value: unknown, row: Record<string, unknown>) => {
    const skill_id = typeof row?.id === "string" ? row.id.trim() : "";
    const enabled = String(row?.enabled ?? "").trim().toLowerCase() === "yes";
    if (!skill_id) return "";
    const set_skill_input = (): void => {
      const input = XUI.getObject("skills-skill-id-input") as { dom?: { value?: unknown } } | undefined;
      if (input?.dom) {
        input.dom.value = skill_id;
      }
    };
    const click_static_button = (button_id: string, action: string): void => {
      _xlog.log(`[acp-ui] skills table action click skill_id=${skill_id} action=${action}`);
      set_skill_input();
      const button = XUI.getObject(button_id) as { dom?: { click?: () => void } } | undefined;
      if (typeof button?.dom?.click === "function") {
        button.dom.click();
        return;
      }
      _xlog.log(`[acp-ui] skills table action missing button button_id=${button_id}`);
    };

    return {
      _type: "xhtml",
      _html_tag: "div",
      class: "acp-link-actions",
      _children: [
        {
          _type: "xhtml",
          _html_tag: "a",
          href: "javascript:void(0)",
          class: "acp-link-action",
          _text: enabled ? "Disable" : "Enable",
          _on_click: () => click_static_button(enabled ? "skills-disable-btn" : "skills-enable-btn", enabled ? "disable" : "enable")
        },
        {
          _type: "xhtml",
          _html_tag: "a",
          href: "javascript:void(0)",
          class: "acp-link-action",
          _text: "Config",
          _on_click: () => click_static_button("skills-open-settings-btn", "config")
        }
      ]
    };
  };
}

function attach_users_action_render(view: XObjectData): void {
  const root = view as Record<string, unknown>;
  const children = Array.isArray(root._children) ? (root._children as Array<Record<string, unknown>>) : [];
  const table_host = children.find((child) => {
    const nested = Array.isArray(child?._children) ? (child._children as Array<Record<string, unknown>>) : [];
    return nested.some((entry) => entry?._id === "users-table");
  });
  if (!table_host) return;

  const nested = Array.isArray(table_host._children) ? (table_host._children as Array<Record<string, unknown>>) : [];
  const table = nested.find((entry) => entry?._id === "users-table");
  if (!table) return;

  const columns = Array.isArray(table._columns) ? (table._columns as Array<Record<string, unknown>>) : [];
  const action_column = columns.find((column) => column?.key === "action");
  if (!action_column) return;

  action_column.render = (_value: unknown, row: Record<string, unknown>) => {
    const user_id = typeof row?.raw_user_id === "string" ? row.raw_user_id.trim() : "";
    const role = typeof row?.role === "string" ? row.role.trim().toLowerCase() : "";
    if (!user_id || role === "owner") return "";

    const button_id = role === "customer" ? "users-promote-btn" : role === "admin" ? "users-demote-btn" : "";
    const label = role === "customer" ? "Make Admin" : role === "admin" ? "Make Customer" : "";
    if (!button_id || !label) return "";

    const click_static_button = (): void => {
      _xlog.log(`[acp-ui] users table action click user_id=${user_id} action=${label}`);
      const input = XUI.getObject("users-action-user-id-input") as { dom?: { value?: unknown } } | undefined;
      if (input?.dom) {
        input.dom.value = user_id;
      }
      const button = XUI.getObject(button_id) as { dom?: { click?: () => void } } | undefined;
      if (typeof button?.dom?.click === "function") {
        button.dom.click();
      }
    };

    return {
      _type: "xhtml",
      _html_tag: "a",
      href: "javascript:void(0)",
      class: "acp-link-action",
      _text: label,
      _on_click: click_static_button
    };
  };
}

function attach_intents_action_render(view: XObjectData): void {
  const root = view as Record<string, unknown>;
  const children = Array.isArray(root._children) ? (root._children as Array<Record<string, unknown>>) : [];
  const table_host = children.find((child) => {
    const nested = Array.isArray(child?._children) ? (child._children as Array<Record<string, unknown>>) : [];
    return nested.some((entry) => entry?._id === "intents-table");
  });
  if (!table_host) return;

  const nested = Array.isArray(table_host._children) ? (table_host._children as Array<Record<string, unknown>>) : [];
  const table = nested.find((entry) => entry?._id === "intents-table");
  if (!table) return;

  const columns = Array.isArray(table._columns) ? (table._columns as Array<Record<string, unknown>>) : [];
  const action_column = columns.find((column) => column?.key === "action");
  if (!action_column) return;

  action_column.render = (_value: unknown, row: Record<string, unknown>) => {
    const intent_id = typeof row?.raw_intent_id === "string" ? row.raw_intent_id.trim() : "";
    if (!intent_id) return "";

    const click_static_button = (): void => {
      const input = XUI.getObject("intents-action-intent-id-input") as { dom?: { value?: unknown } } | undefined;
      if (input?.dom) {
        input.dom.value = intent_id;
      }
      const button = XUI.getObject("intents-edit-btn") as { dom?: { click?: () => void } } | undefined;
      if (typeof button?.dom?.click === "function") {
        button.dom.click();
      }
    };

    return {
      _type: "xhtml",
      _html_tag: "a",
      href: "javascript:void(0)",
      class: "acp-link-action",
      _text: "Edit",
      _on_click: click_static_button
    };
  };
}

function attach_conversations_action_render(view: XObjectData): void {
  const root = view as Record<string, unknown>;
  const children = Array.isArray(root._children) ? (root._children as Array<Record<string, unknown>>) : [];
  const table_host = children.find((child) => {
    const nested = Array.isArray(child?._children) ? (child._children as Array<Record<string, unknown>>) : [];
    return nested.some((entry) => entry?._id === "conversations-table");
  });
  if (!table_host) return;

  const nested = Array.isArray(table_host._children) ? (table_host._children as Array<Record<string, unknown>>) : [];
  const table = nested.find((entry) => entry?._id === "conversations-table");
  if (!table) return;

  const columns = Array.isArray(table._columns) ? (table._columns as Array<Record<string, unknown>>) : [];
  const action_column = columns.find((column) => column?.key === "action");
  if (!action_column) return;

  action_column.render = (_value: unknown, row: Record<string, unknown>) => {
    const thread_id = typeof row?.raw_thread_id === "string" ? row.raw_thread_id.trim() : "";
    if (!thread_id) return "";

    const click_static_button = (): void => {
      const input = XUI.getObject("conversations-action-thread-id-input") as { dom?: { value?: unknown } } | undefined;
      if (input?.dom) {
        input.dom.value = thread_id;
      }
      const button = XUI.getObject("conversations-open-btn") as { dom?: { click?: () => void } } | undefined;
      if (typeof button?.dom?.click === "function") {
        button.dom.click();
      }
    };

    return {
      _type: "xhtml",
      _html_tag: "a",
      href: "javascript:void(0)",
      class: "acp-link-action",
      _text: "Open",
      _on_click: click_static_button
    };
  };
}

export async function create_acp_app(): Promise<XVMApp> {
  const shell_view = clone_view(shell_view_json) as XObjectData;

  const views: Record<string, XObjectData> = {
    [VIEW_LOGIN]: clone_view(login_view_json) as XObjectData,
    [VIEW_SETUP_ADMIN]: clone_view(setup_admin_view_json) as XObjectData,
    [VIEW_ABOUT]: clone_view(about_view_json) as XObjectData,
    [VIEW_AGENT]: clone_view(agent_view_json) as XObjectData,
    [VIEW_QAGENT]: clone_view(qagent_view_json) as XObjectData,
    [VIEW_CONVERSATIONS]: clone_view(conversations_view_json) as XObjectData,
    [VIEW_CONVERSATION_DETAILS]: clone_view(conversation_thread_view_json) as XObjectData,
    [VIEW_INTENTS]: clone_view(intents_view_json) as XObjectData,
    [VIEW_USERS]: clone_view(users_view_json) as XObjectData,
    [VIEW_ADMIN_USERS]: clone_view(admin_users_view_json) as XObjectData,
    [VIEW_SKILLS]: clone_view(skills_view_json) as XObjectData,
    [VIEW_SKILL_SETTINGS]: clone_view(skill_settings_view_json) as XObjectData,
    [VIEW_SETTINGS]: clone_view(settings_view_json) as XObjectData
  };

  attach_skills_action_render(views[VIEW_SKILLS]);
  attach_intents_action_render(views[VIEW_INTENTS]);
  attach_users_action_render(views[VIEW_USERS]);
  attach_conversations_action_render(views[VIEW_CONVERSATIONS]);

  return {
    _shell: shell_view,
    _containers: [
      {
        _id: "region-main"
      },
      {
        _id: "region-overlay"
      }
    ],
    _regions: [
      {
        _id: REGION_MAIN,
        _container_id: "region-main",
        _history: true,
        _hash_sync: true
      },
      {
        _id: REGION_OVERLAY,
        _container_id: "region-overlay",
        _history: false,
        _hash_sync: false
      }
    ],
    _views: views,
    _routes: [
      {
        _id: ROUTE_LOGIN,
        _view_id: VIEW_LOGIN,
        _region: REGION_MAIN
      },
      {
        _id: ROUTE_SETUP_ADMIN,
        _view_id: VIEW_SETUP_ADMIN,
        _region: REGION_MAIN
      },
      {
        _id: ROUTE_ABOUT,
        _view_id: VIEW_ABOUT,
        _region: REGION_MAIN
      },
      {
        _id: ROUTE_AGENT,
        _view_id: VIEW_AGENT,
        _region: REGION_MAIN
      },
      {
        _id: ROUTE_QAGENT,
        _view_id: VIEW_QAGENT,
        _region: REGION_MAIN
      },
      {
        _id: ROUTE_CONVERSATIONS,
        _view_id: VIEW_CONVERSATIONS,
        _region: REGION_MAIN
      },
      {
        _id: ROUTE_CONVERSATION_DETAILS,
        _view_id: VIEW_CONVERSATION_DETAILS,
        _region: REGION_MAIN
      },
      {
        _id: ROUTE_INTENTS,
        _view_id: VIEW_INTENTS,
        _region: REGION_MAIN
      },
      {
        _id: ROUTE_USERS,
        _view_id: VIEW_USERS,
        _region: REGION_MAIN
      },
      {
        _id: ROUTE_ADMIN_USERS,
        _view_id: VIEW_ADMIN_USERS,
        _region: REGION_MAIN
      },
      {
        _id: ROUTE_SKILLS,
        _view_id: VIEW_SKILLS,
        _region: REGION_MAIN
      },
      {
        _id: ROUTE_SKILL_SETTINGS,
        _view_id: VIEW_SKILL_SETTINGS,
        _region: REGION_MAIN
      },
      {
        _id: ROUTE_SETTINGS,
        _view_id: VIEW_SETTINGS,
        _region: REGION_MAIN
      }
    ],
    _router: {
      _region: REGION_MAIN,
      _fallback_view_id: VIEW_LOGIN
    },
    _start: {
      _route_id: ROUTE_LOGIN,
      _region: REGION_MAIN
    }
  };
}

export default create_acp_app;
