import type { SkillSettingsMeta } from "./settings.js";

export type SkillLogLevel = "debug" | "info" | "warn" | "error";

export type XBotSkillCapability = {
  kernel_ops?: string[];
  channels?: string[];
  network?: boolean;
};

export type XBotSkillActionField = {
  key: string;
  label: string;
  type: "string" | "number" | "boolean" | "select";
  help?: string;
  options?: Array<{ label: string; value: unknown }>;
  placeholder?: string;
};

export type XBotSkillAction = {
  id: string;
  label: string;
  kind?: "primary" | "secondary" | "danger";
  op: {
    module: string;
    op: string;
  };
  params_schema?: {
    title?: string;
    fields: XBotSkillActionField[];
  };
  confirm?: {
    title: string;
    body: string;
  };
};

export type XBotIntentHandler = {
  module: string;
  op: string;
};

export type XBotIntentField = {
  key: string;
  label: string;
  type: "string" | "number" | "boolean" | "select" | "string_list" | "json";
  help?: string;
  secret?: boolean;
  options?: Array<{ label: string; value: unknown }>;
  placeholder?: string;
};

export type XBotIntent = {
  intent_id: string;
  title: string;
  description?: string;
  roles_allowed: Array<"owner" | "admin" | "customer">;
  channels_allowed?: string[];
  handler: XBotIntentHandler;
  params_schema?: {
    title?: string;
    fields: XBotIntentField[];
  };
  examples?: string[];
  synonyms?: string[];
};

export interface XBotSkillContext {
  execute(module: string, op: string, params?: any, meta?: any): Promise<any>;
  state_get(key: string): Promise<any>;
  state_set(key: string, value: any): Promise<void>;
  registerModule(moduleInstance: any): void;
  emit(eventName: string, payload: any): void;
  log(level: SkillLogLevel, msg: string, meta?: any): void;
  skill: { id: string; version: string };
}

export interface XBotSkill {
  id: string;
  version: string;
  name?: string;
  description?: string;
  settings?: SkillSettingsMeta;
  capabilities?: XBotSkillCapability;
  actions?: XBotSkillAction[];
  intents?: XBotIntent[];
  onEnable(ctx: XBotSkillContext): Promise<void> | void;
  onDisable?(ctx: XBotSkillContext): Promise<void> | void;
}

export type SkillRegisterFn = (ctx: XBotSkillContext) => void | Promise<void>;

export type SkillResolveConfig = {
  node_modules: boolean;
  local_paths: string[];
};

export type SkillConfig = {
  allow: string[];
  enabled: string[];
  resolve: SkillResolveConfig;
};

export type AgentConfig = {
  agent?: {
    agent_id?: string;
  };
  skills: SkillConfig;
};

export type SkillStatus = "loaded" | "error" | "disabled";

export type LoadedSkillRecord = {
  id: string;
  version?: string;
  name?: string;
  description?: string;
  enabled: boolean;
  status: SkillStatus;
  error?: string;
  settings_meta?: SkillSettingsMeta;
  capabilities?: XBotSkillCapability;
  actions?: XBotSkillAction[];
  intents?: XBotIntent[];
  source?: string;
  modules_registered?: string[];
};
