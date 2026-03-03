import { XError } from "@xpell/node";

type Dict = Record<string, unknown>;

type IntentClassifierIntent = {
  intent_id: string;
  title: string;
  description?: string;
  examples?: string[];
  synonyms?: string[];
};

type IntentClassifierInput = {
  message: string;
  recent_messages: string[];
  enabled_intents: IntentClassifierIntent[];
  agent_name: string;
  agent_role: string;
  channel: string;
  chat: (messages: Array<{ role: "system" | "user"; content: string }>) => Promise<string>;
};

type IntentClassifierResult = {
  intent_id: string;
  params: Dict;
  confidence?: number;
};

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function has_function(value: unknown): boolean {
  if (typeof value === "function") return true;
  if (Array.isArray(value)) return value.some(has_function);
  if (is_plain_object(value)) return Object.values(value).some(has_function);
  return false;
}

function strip_code_fences(text: string): string {
  const trimmed = text.trim();
  if (!trimmed.startsWith("```")) return trimmed;
  const first_break = trimmed.indexOf("\n");
  if (first_break < 0) return trimmed.replace(/^```[a-zA-Z0-9_-]*\s*/, "").replace(/```$/, "").trim();
  const body = trimmed.slice(first_break + 1);
  const closing = body.lastIndexOf("```");
  return (closing >= 0 ? body.slice(0, closing) : body).trim();
}

function parse_json_payload(text: string): Dict {
  const cleaned = strip_code_fences(text);
  try {
    const parsed = JSON.parse(cleaned);
    if (!is_plain_object(parsed)) {
      throw new XError("E_INTENT_CLASSIFY_INVALID", "Classifier must return a JSON object");
    }
    if (has_function(parsed)) {
      throw new XError("E_INTENT_CLASSIFY_INVALID", "Classifier returned non-JSON-safe payload");
    }
    return parsed;
  } catch (err) {
    if (err instanceof XError) throw err;
    throw new XError("E_INTENT_CLASSIFY_INVALID", "Classifier returned invalid JSON");
  }
}

function ensure_plain_params(value: unknown): Dict {
  if (value === undefined || value === null) return {};
  if (!is_plain_object(value) || has_function(value)) {
    throw new XError("E_INTENT_CLASSIFY_INVALID", "Classifier params must be a JSON object");
  }
  return { ...value };
}

export async function classify_intent(input: IntentClassifierInput): Promise<IntentClassifierResult> {
  const allow_ids = new Set<string>(["none", ...input.enabled_intents.map((intent) => intent.intent_id)]);
  const intents_summary = input.enabled_intents.map((intent) => ({
    intent_id: intent.intent_id,
    title: intent.title,
    ...(intent.description ? { description: intent.description } : {}),
    ...(intent.examples && intent.examples.length > 0 ? { examples: intent.examples.slice(0, 4) } : {}),
    ...(intent.synonyms && intent.synonyms.length > 0 ? { synonyms: intent.synonyms.slice(0, 6) } : {})
  }));

  const messages = [
    {
      role: "system" as const,
      content:
        "You classify inbound messages into one allowed intent. " +
        "Return strict JSON only with keys intent_id, params, confidence. " +
        "intent_id must be one of the provided intent ids or 'none'. " +
        "Do not include prose, markdown, or explanations."
    },
    {
      role: "user" as const,
      content: JSON.stringify(
        {
          agent_name: input.agent_name,
          actor_role: input.agent_role,
          channel: input.channel,
          message: input.message,
          recent_messages: input.recent_messages.slice(-8),
          enabled_intents: intents_summary,
          output_contract: {
            intent_id: "one of enabled intent ids or 'none'",
            params: {},
            confidence: 0
          }
        },
        null,
        2
      )
    }
  ];

  const raw = await input.chat(messages);
  const parsed = parse_json_payload(raw);
  const intent_id = typeof parsed.intent_id === "string" ? parsed.intent_id.trim() : "";
  if (!intent_id || !allow_ids.has(intent_id)) {
    throw new XError("E_INTENT_CLASSIFY_INVALID", "Classifier selected an unknown intent_id");
  }

  const confidence_raw = parsed.confidence;
  const confidence = typeof confidence_raw === "number" && Number.isFinite(confidence_raw) ? confidence_raw : undefined;
  return {
    intent_id,
    params: ensure_plain_params(parsed.params),
    ...(typeof confidence === "number" ? { confidence } : {})
  };
}

export default classify_intent;
