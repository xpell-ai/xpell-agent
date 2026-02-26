export type XSettingsSchemaFieldType = "string" | "number" | "boolean" | "select" | "string_list";

export type XSettingsSchemaOption = {
  label: string;
  value: unknown;
};

export type XSettingsSchemaField = {
  key: string;
  label: string;
  type: XSettingsSchemaFieldType;
  help?: string;
  secret?: boolean;
  options?: XSettingsSchemaOption[];
  placeholder?: string;
};

export type XSettingsSchema = {
  title?: string;
  fields: XSettingsSchemaField[];
};

export type SkillSettingsMeta = {
  defaults?: Record<string, unknown>;
  schema?: XSettingsSchema;
  sensitive?: string[];
};

export type SettingsGetSkillResult = {
  skill_id: string;
  settings: Record<string, unknown>;
  masked: Record<string, boolean>;
  schema?: XSettingsSchema;
};
