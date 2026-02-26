export const MESSAGE_DIRECTION_IN = "in" as const;
export const MESSAGE_DIRECTION_OUT = "out" as const;

export type ConversationDirection = typeof MESSAGE_DIRECTION_IN | typeof MESSAGE_DIRECTION_OUT;

export type ConversationSender = "customer" | "agent" | "admin" | "system";

export type ConversationThread = {
  thread_id: string;
  channel: string;
  channel_thread_id: string;
  user_id: string;
  status: string;
  created_at: number;
  updated_at: number;
  tags: string[];
};

export type ConversationMessage = {
  message_id: string;
  thread_id: string;
  direction: ConversationDirection;
  sender: ConversationSender;
  text: string;
  ts: number;
  channel_message_id?: string;
  meta?: Record<string, unknown>;
};

