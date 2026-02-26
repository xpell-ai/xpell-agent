import type { XObjectData, XVMApp } from "@xpell/ui";

import {
  REGION_MAIN,
  REGION_OVERLAY,
  ROUTE_ABOUT,
  ROUTE_ADMIN_USERS,
  ROUTE_LOGIN,
  ROUTE_SETTINGS,
  ROUTE_SKILLS,
  ROUTE_SKILL_SETTINGS,
  VIEW_ABOUT,
  VIEW_ADMIN_USERS,
  VIEW_LOGIN,
  VIEW_SETTINGS,
  VIEW_SKILLS,
  VIEW_SKILL_SETTINGS
} from "../state/xd_keys.js";
import about_view_json from "../views/about.view.json";
import admin_users_view_json from "../views/admin-users.view.json";
import login_view_json from "../views/login.view.json";
import settings_view_json from "../views/settings.view.json";
import shell_view_json from "../views/shell.view.json";
import skill_settings_view_json from "../views/skill-settings.view.json";
import skills_view_json from "../views/skills.view.json";

function clone_view<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

export async function create_acp_app(): Promise<XVMApp> {
  const shell_view = clone_view(shell_view_json) as XObjectData;

  const views: Record<string, XObjectData> = {
    [VIEW_LOGIN]: clone_view(login_view_json) as XObjectData,
    [VIEW_ABOUT]: clone_view(about_view_json) as XObjectData,
    [VIEW_ADMIN_USERS]: clone_view(admin_users_view_json) as XObjectData,
    [VIEW_SKILLS]: clone_view(skills_view_json) as XObjectData,
    [VIEW_SKILL_SETTINGS]: clone_view(skill_settings_view_json) as XObjectData,
    [VIEW_SETTINGS]: clone_view(settings_view_json) as XObjectData
  };

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
        _id: ROUTE_ABOUT,
        _view_id: VIEW_ABOUT,
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
