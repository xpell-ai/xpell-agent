import { pathToFileURL } from "node:url";

import { _xlog } from "@xpell/node";

import { AgentRuntime } from "./runtime/AgentRuntime.js";
import { AgentModule } from "./modules/AgentModule.js";
import { ChannelsModule } from "./modules/ChannelsModule.js";
import { ConversationsModule } from "./modules/ConversationsModule.js";
import { KnowledgeModule } from "./modules/KnowledgeModule.js";
import { SettingsModule } from "./modules/SettingsModule.js";
import { SkillManagerModule } from "./modules/SkillManagerModule.js";
import { UsersModule } from "./modules/UsersModule.js";

async function main(): Promise<void> {
  const runtime = new AgentRuntime();
  await runtime.start();
}

const is_entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href === import.meta.url : false;

if (is_entrypoint) {
  main().catch((err) => {
    _xlog.error("[agent-core] boot failed", err);
    process.exitCode = 1;
  });
}

export { AgentRuntime, AgentModule, UsersModule, ConversationsModule, ChannelsModule, KnowledgeModule, SettingsModule, SkillManagerModule };
export * from "./types/users.js";
export * from "./types/conversations.js";
export * from "./types/skills.js";
export * from "./types/settings.js";
