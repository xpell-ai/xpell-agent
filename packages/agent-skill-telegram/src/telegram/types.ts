export type TelegramParseMode = "HTML" | "MarkdownV2";

export type TelegramUser = {
  id: number;
  is_bot?: boolean;
  username?: string;
  first_name?: string;
  last_name?: string;
};

export type TelegramChat = {
  id: number;
  type?: string;
  username?: string;
  first_name?: string;
  last_name?: string;
  title?: string;
};

export type TelegramMessage = {
  message_id: number;
  date: number;
  text?: string;
  caption?: string;
  chat: TelegramChat;
  from?: TelegramUser;
};

export type TelegramUpdate = {
  update_id: number;
  message?: TelegramMessage;
  edited_message?: TelegramMessage;
};

export type TelegramApiResponse<T> = {
  ok: boolean;
  result?: T;
  description?: string;
  error_code?: number;
};

export type TelegramBotInfo = {
  id: number;
  is_bot?: boolean;
  username?: string;
  first_name?: string;
  can_join_groups?: boolean;
  can_read_all_group_messages?: boolean;
  supports_inline_queries?: boolean;
};

export type NormalizedTelegramInbound = {
  update_id: number;
  chat_id: string;
  from_id: string;
  message_id: string;
  text: string;
  profile: {
    username?: string;
    first_name?: string;
    last_name?: string;
  };
  raw: {
    update_id: number;
    chat_type?: string;
    date?: number;
  };
};
