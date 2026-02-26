export const USER_ROLE_OWNER = "owner" as const;
export const USER_ROLE_ADMIN = "admin" as const;
export const USER_ROLE_CUSTOMER = "customer" as const;

export type BotUserRole = typeof USER_ROLE_OWNER | typeof USER_ROLE_ADMIN | typeof USER_ROLE_CUSTOMER;

export type BotIdentity = {
  identity_id: string;
  channel: string;
  channel_user_id: string;
  display_name?: string;
  created_at: number;
  updated_at: number;
};

export type BotUser = {
  user_id: string;
  role: BotUserRole;
  display_name: string;
  created_at: number;
  updated_at: number;
  identities: BotIdentity[];
};

