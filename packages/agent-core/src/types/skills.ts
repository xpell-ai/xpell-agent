import type { SkillSettingsMeta } from "./settings.js";

export type SkillLogLevel = "debug" | "info" | "warn" | "error";

export type XBotSkillCapability = {
  kernel_ops?: string[];
  channels?: string[];
  network?: boolean;
};

export interface XBotSkillContext {
  execute(module: string, op: string, params?: any, meta?: any): Promise<any>;
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
  skills: SkillConfig;
};

export type SkillStatus = "loaded" | "error" | "disabled";

export type LoadedSkillRecord = {
  id: string;
  version?: string;
  enabled: boolean;
  status: SkillStatus;
  error?: string;
  capabilities?: XBotSkillCapability;
  source?: string;
  modules_registered?: string[];
};
