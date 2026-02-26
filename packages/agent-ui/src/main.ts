import { _x, _xlog, XUI, XVM } from "@xpell/ui";
import { XDashPack } from "../../../../xpell-web/xdashboard/dist/index.js";
import "../../../../xpell-web/xdashboard/dist/xdashboard.css";

import "../assets/styles.css";
import { create_acp_app } from "./app/acp-app.js";
import { create_ui_commands } from "./commands/ui-commands.js";
import { create_agent_api } from "./services/api.js";

function should_use_mock_mode(): boolean {
  if (typeof window === "undefined") return false;
  try {
    const url = new URL(window.location.href);
    const raw = (url.searchParams.get("mock") ?? "").trim().toLowerCase();
    return raw === "1" || raw === "true" || raw === "yes";
  } catch {
    return false;
  }
}

async function main(): Promise<void> {
  _x.start();
  _x.loadModule(XUI);
  _x.loadModule(XVM);
  XUI.importObjectPack(XDashPack);

  XUI.createPlayer("xplayer", "acp-player", "xplayer-root", true);

  const api = create_agent_api({ mode: should_use_mock_mode() ? "mock" : "wormholes" });
  const app = await create_acp_app();

  await XVM.app(app);

  const ui_commands = create_ui_commands({ api });
  ui_commands.register();
  await ui_commands.bootstrap();
}

main().catch((error) => {
  _xlog.error("[acp-ui] bootstrap failed", error);
});
