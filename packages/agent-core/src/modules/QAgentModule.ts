import { randomUUID } from "node:crypto";

import { XError, XModule, _x, type XCommandData } from "@xpell/node";

import {
  init_qagent_xdb,
  get_qagent_run_xdb,
  list_qagent_cases_xdb,
  list_qagent_runs_xdb,
  save_qagent_case_xdb,
  save_qagent_run_xdb,
  type AgentQAgentXdbScope
} from "../xdb/qagent-xdb.js";
import { readCommandCtx, requireKernelCap, type AgentActorRole, type AgentCommandCtx } from "../runtime/guards.js";
import type { QAgentAudience, QAgentCase, QAgentCaseAudience, QAgentRun, QAgentTotals } from "../types/qagent.js";

const MODULE_NAME = "qagent";
const AGENT_MODULE_NAME = "agent";
const AZURE_MODULE_NAME = "azure";
const KB_MODULE_NAME = "kb";
const DEFAULT_CHANNEL_ID = "telegram";
const DEFAULT_MAX_CASES = 20;
const MAX_CASES = 50;
const DEFAULT_KB_FILE = "ruta1_kb.md";

type Dict = Record<string, unknown>;

type QAgentModuleOptions = {
  _app_id?: string;
  _env?: string;
};

type OpenAIChatMessage = {
  role: "system" | "user" | "assistant";
  content: string;
};

type PlanCase = {
  audience: QAgentCaseAudience;
  intent_id: string;
  question: string;
  expected_facts: string[];
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

function ensure_non_empty_string(value: unknown, field_name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new XError("E_QAGENT_BAD_PARAMS", `Invalid ${field_name}: expected non-empty string`);
  }
  return value.trim();
}

function ensure_optional_string(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalize_string_array(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const entry of value) {
    if (typeof entry !== "string") continue;
    const trimmed = entry.trim();
    if (!trimmed || seen.has(trimmed)) continue;
    seen.add(trimmed);
    out.push(trimmed);
  }
  return out;
}

function safe_json_stringify(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return "{}";
  }
}

function clamp_score(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.min(1, Math.max(0, value));
}

function truncate_text(value: string, max_chars: number): string {
  const trimmed = value.trim();
  if (trimmed.length <= max_chars) return trimmed;
  return `${trimmed.slice(0, Math.max(0, max_chars - 3))}...`;
}

function parse_json_object(text: string): Dict {
  const trimmed = text.trim();
  const cleaned = trimmed.startsWith("```")
    ? trimmed.replace(/^```[a-zA-Z0-9_-]*\s*/, "").replace(/```$/, "").trim()
    : trimmed;
  try {
    const parsed = JSON.parse(cleaned);
    if (!is_plain_object(parsed) || has_function(parsed)) {
      throw new Error("invalid");
    }
    return parsed;
  } catch {
    throw new XError("E_QAGENT_BAD_UPSTREAM", "Expected valid JSON object");
  }
}

export class QAgentModule extends XModule {
  static _name = MODULE_NAME;

  private _scope: AgentQAgentXdbScope;

  constructor(opts: QAgentModuleOptions = {}) {
    super({ _name: MODULE_NAME });
    this._scope = {
      _app_id: typeof opts._app_id === "string" && opts._app_id.trim() ? opts._app_id.trim() : "xbot",
      _env: typeof opts._env === "string" && opts._env.trim() ? opts._env.trim() : "default"
    };
  }

  async _init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }
  async _op_init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }

  async _create_run(xcmd: XCommandData) {
    return this.create_run_impl(xcmd);
  }
  async _op_create_run(xcmd: XCommandData) {
    return this.create_run_impl(xcmd);
  }

  async _run_quick(xcmd: XCommandData) {
    return this.run_quick_impl(xcmd);
  }
  async _op_run_quick(xcmd: XCommandData) {
    return this.run_quick_impl(xcmd);
  }

  async _generate_plan(xcmd: XCommandData) {
    return this.generate_plan_impl(xcmd);
  }
  async _op_generate_plan(xcmd: XCommandData) {
    return this.generate_plan_impl(xcmd);
  }

  async _run(xcmd: XCommandData) {
    return this.run_impl(xcmd);
  }
  async _op_run(xcmd: XCommandData) {
    return this.run_impl(xcmd);
  }

  async _get_run(xcmd: XCommandData) {
    return this.get_run_impl(xcmd);
  }
  async _op_get_run(xcmd: XCommandData) {
    return this.get_run_impl(xcmd);
  }

  async _get_last_run(xcmd: XCommandData) {
    return this.get_last_run_impl(xcmd);
  }
  async _op_get_last_run(xcmd: XCommandData) {
    return this.get_last_run_impl(xcmd);
  }

  async _list_runs(xcmd: XCommandData) {
    return this.list_runs_impl(xcmd);
  }
  async _op_list_runs(xcmd: XCommandData) {
    return this.list_runs_impl(xcmd);
  }

  async _list_cases(xcmd: XCommandData) {
    return this.list_cases_impl(xcmd);
  }
  async _op_list_cases(xcmd: XCommandData) {
    return this.list_cases_impl(xcmd);
  }

  private async init_on_boot_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    await init_qagent_xdb(this._scope);
    return { ok: true };
  }

  private async create_run_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    this.require_admin_actor(ctx);
    await init_qagent_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const audience = this.normalize_audience(params._audience);
    const max_cases = this.normalize_max_cases(params._max_cases);
    const kb_files = await this.resolve_kb_files(ctx, params._kb_files);
    const profile = await this.read_agent_profile(ctx);
    const run: QAgentRun = {
      _id: randomUUID(),
      _app_id: this._scope._app_id,
      _env: this._scope._env,
      _status: "created",
      _created_at: Date.now(),
      _updated_at: Date.now(),
      _kb_files: kb_files,
      _agent_name: profile.name,
      _agent_role: profile.role,
      _meta_json: safe_json_stringify({
        audience,
        max_cases,
        ...(ensure_optional_string(params._notes) ? { notes: ensure_optional_string(params._notes) } : {})
      })
    };
    await save_qagent_run_xdb(this._scope, run);
    return { run_id: run._id };
  }

  private async generate_plan_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    this.require_admin_actor(ctx);
    await init_qagent_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const run_id = ensure_non_empty_string(params.run_id, "run_id");
    const run = await this.must_get_run(run_id);
    const existing_cases = await list_qagent_cases_xdb(this._scope, run_id);
    if (existing_cases.length > 0) {
      throw new XError("E_QAGENT_ALREADY_GENERATED", "Run already has generated cases");
    }

    const meta = this.parse_json_object_optional(run._meta_json);
    const audience = this.normalize_audience(meta?.audience);
    const max_cases = this.normalize_max_cases(meta?.max_cases);
    const kb_payload = await this.load_kb_payload(ctx, run._kb_files);
    const plan = await this.generate_plan_from_llm({
      kb_payload,
      audience,
      max_cases,
      agent_name: run._agent_name,
      agent_role: run._agent_role
    });

    let case_idx = 0;
    for (const plan_case of plan) {
      case_idx += 1;
      const record: QAgentCase = {
        _id: randomUUID(),
        _run_id: run._id,
        _app_id: this._scope._app_id,
        _env: this._scope._env,
        _case_idx: case_idx,
        _audience: plan_case.audience,
        _intent_id: plan_case.intent_id,
        _question: plan_case.question,
        _expected_facts: [...plan_case.expected_facts],
        _answer: "",
        _score: 0,
        _judge_notes: "",
        _created_at: Date.now(),
        _updated_at: Date.now()
      };
      await save_qagent_case_xdb(this._scope, record);
    }

    run._totals_json = safe_json_stringify({
      planned_cases: case_idx
    });
    await save_qagent_run_xdb(this._scope, run);
    return { run_id: run._id, cases_created: case_idx };
  }

  private async run_quick_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    this.require_admin_actor(ctx);
    await init_qagent_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const max_cases = this.normalize_max_cases(params._max_cases ?? 8);
    const next_ctx = is_plain_object(params._ctx) ? params._ctx : undefined;

    const create_out = await this.create_run_impl({
      ...xcmd,
      _params: {
        ...(next_ctx ? { _ctx: next_ctx } : {}),
        _max_cases: max_cases
      }
    });
    const run_id = ensure_non_empty_string((create_out as Dict).run_id, "run_id");

    await this.generate_plan_impl({
      ...xcmd,
      _params: {
        ...(next_ctx ? { _ctx: next_ctx } : {}),
        run_id
      }
    });

    const run_out = await this.run_impl({
      ...xcmd,
      _params: {
        ...(next_ctx ? { _ctx: next_ctx } : {}),
        run_id,
        _max_cases: max_cases
      }
    });

    const latest = await this.get_last_run_impl(xcmd);
    return {
      ...(is_plain_object(run_out) ? run_out : {}),
      ...(is_plain_object(latest) ? latest : {}),
      run_id
    };
  }

  private async run_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    this.require_admin_actor(ctx);
    await init_qagent_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const run_id = ensure_non_empty_string(params.run_id, "run_id");
    const max_cases =
      params._max_cases === undefined ? undefined : this.normalize_max_cases(params._max_cases);
    const run = await this.must_get_run(run_id);
    const all_cases = await list_qagent_cases_xdb(this._scope, run_id);
    const cases = max_cases === undefined ? all_cases : all_cases.slice(0, max_cases);
    if (cases.length === 0) {
      throw new XError("E_QAGENT_NO_CASES", "Run has no generated cases");
    }

    run._status = "running";
    await save_qagent_run_xdb(this._scope, run);

    try {
      let score_sum = 0;
      let passed = 0;

      for (const test_case of cases) {
        const answer = await this.exec_agent_answer({
          channel_id: DEFAULT_CHANNEL_ID,
          text: test_case._question,
          actor_role: test_case._audience,
          user_ref: {
            name: test_case._audience === "admin" ? "QAgent Admin" : "QAgent Customer"
          },
          _ctx: this.forward_ctx(ctx)
        });

        const baseline = this.compute_baseline_score(test_case._expected_facts, answer.reply_text);
        const judged = await this.judge_answer({
          question: test_case._question,
          expected_facts: test_case._expected_facts,
          answer: answer.reply_text
        });
        const final_score = clamp_score(Math.max(baseline, judged.score));

        test_case._answer = answer.reply_text;
        test_case._score = final_score;
        test_case._judge_notes = judged.notes;
        await save_qagent_case_xdb(this._scope, test_case);

        score_sum += final_score;
        if (final_score >= 0.7) passed += 1;
      }

      const totals: QAgentTotals = {
        cases_total: cases.length,
        cases_passed: passed,
        avg_score: clamp_score(score_sum / cases.length),
        pass_rate: cases.length > 0 ? Math.min(1, Math.max(0, passed / cases.length)) : 0
      };
      run._status = "done";
      run._summary = `QAgent completed: ${totals.cases_passed}/${totals.cases_total} passed, avg_score=${totals.avg_score.toFixed(2)}`;
      run._totals_json = safe_json_stringify(totals);
      await save_qagent_run_xdb(this._scope, run);
      return { run_id: run._id, totals };
    } catch (err) {
      run._status = "failed";
      run._summary = `QAgent failed: ${err instanceof Error ? err.message : String(err)}`;
      await save_qagent_run_xdb(this._scope, run);
      throw err;
    }
  }

  private async get_run_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    this.require_admin_actor(ctx);
    await init_qagent_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const run = await this.must_get_run(ensure_non_empty_string(params.run_id, "run_id"));
    const cases = await list_qagent_cases_xdb(this._scope, run._id);
    return {
      run,
      totals: this.parse_json_object_optional(run._totals_json) ?? {},
      cases
    };
  }

  private async get_last_run_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    this.require_admin_actor(ctx);
    await init_qagent_xdb(this._scope);
    const runs = await list_qagent_runs_xdb(this._scope, 0, 1);
    if (runs.length === 0) {
      return {
        run: null,
        totals: {},
        top_failures: []
      };
    }
    const run = runs[0];
    const cases = await list_qagent_cases_xdb(this._scope, run._id);
    const top_failures = cases
      .filter((entry) => entry._score < 0.7)
      .sort((left, right) => {
        if (left._score !== right._score) return left._score - right._score;
        return left._case_idx - right._case_idx;
      })
      .slice(0, 10);
    return {
      run,
      totals: this.parse_json_object_optional(run._totals_json) ?? {},
      top_failures
    };
  }

  private async list_runs_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    this.require_admin_actor(ctx);
    await init_qagent_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const _skip = this.normalize_non_negative_int(params._skip, 0);
    const _limit = this.normalize_non_negative_int(params._limit, 20);
    return {
      items: await list_qagent_runs_xdb(this._scope, _skip, Math.max(1, _limit))
    };
  }

  private async list_cases_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    this.require_admin_actor(ctx);
    await init_qagent_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const run_id = ensure_non_empty_string(params.run_id, "run_id");
    return {
      items: await list_qagent_cases_xdb(this._scope, run_id)
    };
  }

  private require_admin_actor(ctx: AgentCommandCtx): { _actor_user_id: string; _actor_role: "admin" | "owner" } {
    const role = typeof ctx.actor?.role === "string" ? ctx.actor.role : "";
    const user_id = typeof ctx.actor?.user_id === "string" ? ctx.actor.user_id.trim() : "";
    if ((role !== "admin" && role !== "owner") || !user_id) {
      throw new XError("E_AGENT_FORBIDDEN", "Admin actor required");
    }
    return {
      _actor_user_id: user_id,
      _actor_role: role
    };
  }

  private ensure_params(value: unknown): Dict {
    if (value === undefined || value === null) return {};
    if (!is_plain_object(value) || has_function(value)) {
      throw new XError("E_QAGENT_BAD_PARAMS", "params must be a JSON-safe object");
    }
    return value;
  }

  private normalize_audience(value: unknown): QAgentAudience {
    const normalized = ensure_optional_string(value)?.toLowerCase();
    if (normalized === "customer" || normalized === "admin" || normalized === "both") return normalized;
    return "both";
  }

  private normalize_case_audience(value: unknown): QAgentCaseAudience {
    return ensure_optional_string(value)?.toLowerCase() === "admin" ? "admin" : "customer";
  }

  private normalize_max_cases(value: unknown): number {
    const parsed = Number.parseInt(String(value ?? DEFAULT_MAX_CASES), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_MAX_CASES;
    return Math.min(MAX_CASES, Math.max(1, Math.floor(parsed)));
  }

  private normalize_non_negative_int(value: unknown, fallback: number): number {
    const parsed = Number.parseInt(String(value ?? fallback), 10);
    if (!Number.isFinite(parsed) || parsed < 0) return fallback;
    return Math.floor(parsed);
  }

  private parse_json_object_optional(value: unknown): Dict | undefined {
    if (typeof value !== "string" || !value.trim()) return undefined;
    try {
      const parsed = JSON.parse(value);
      return is_plain_object(parsed) && !has_function(parsed) ? parsed : undefined;
    } catch {
      return undefined;
    }
  }

  private async must_get_run(run_id: string): Promise<QAgentRun> {
    const run = await get_qagent_run_xdb(this._scope, run_id);
    if (!run) {
      throw new XError("E_QAGENT_NOT_FOUND", `Run not found: ${run_id}`);
    }
    return run;
  }

  private async resolve_kb_files(ctx: AgentCommandCtx, override: unknown): Promise<string[]> {
    const explicit = normalize_string_array(override);
    if (explicit.length > 0) return explicit;
    try {
      const out = await _x.execute({
        _module: KB_MODULE_NAME,
        _op: "list_files",
        _params: { _ctx: this.forward_ctx(ctx) }
      });
      const raw_files = is_plain_object(out) && Array.isArray(out._files) ? out._files : [];
      const files: string[] = [];
      for (const entry of raw_files) {
        if (!is_plain_object(entry)) continue;
        const name = ensure_optional_string(entry._name);
        if (name) files.push(name);
      }
      return files.length > 0 ? files : [DEFAULT_KB_FILE];
    } catch {
      return [DEFAULT_KB_FILE];
    }
  }

  private async read_agent_profile(ctx: AgentCommandCtx): Promise<{ name: string; role: string }> {
    const out = await _x.execute({
      _module: AGENT_MODULE_NAME,
      _op: "get_profile",
      _params: { _ctx: this.forward_ctx(ctx) }
    });
    const payload = is_plain_object(out) ? out : {};
    return {
      name: ensure_optional_string(payload.name) ?? "XBot",
      role: ensure_optional_string(payload.role) ?? "Assistant"
    };
  }

  private async load_kb_payload(ctx: AgentCommandCtx, kb_files: string[]): Promise<Array<{ file: string; content: string }>> {
    const files = kb_files.length > 0 ? kb_files : [DEFAULT_KB_FILE];
    const out: Array<{ file: string; content: string }> = [];
    for (const file of files) {
      const result = await _x.execute({
        _module: KB_MODULE_NAME,
        _op: "show",
        _params: {
          _kb_file: file,
          _ctx: this.forward_ctx(ctx)
        }
      });
      const payload = is_plain_object(result) ? result : {};
      out.push({
        file,
        content: typeof payload.content === "string" ? payload.content : ""
      });
    }
    return out;
  }

  private async generate_plan_from_llm(input: {
    kb_payload: Array<{ file: string; content: string }>;
    audience: QAgentAudience;
    max_cases: number;
    agent_name: string;
    agent_role: string;
  }): Promise<PlanCase[]> {
    const kb_text = input.kb_payload
      .map((entry) => `FILE: ${entry.file}\n${truncate_text(entry.content, 6000)}`)
      .join("\n\n---\n\n");
    const out = await this.exec_azure_openai_chat({
      messages: [
        {
          role: "system",
          content:
            "Generate a deterministic QA test plan for an agent. " +
            "Return JSON only: {\"cases\":[{\"audience\":\"customer|admin\",\"intent_id\":\"...\",\"question\":\"...\",\"expected_facts\":[\"...\"]}]}. " +
            "Only include cases answerable from the KB. Keep questions concise. Do not exceed the requested max."
        },
        {
          role: "user",
          content: JSON.stringify({
            agent_name: input.agent_name,
            agent_role: input.agent_role,
            audience: input.audience,
            max_cases: input.max_cases,
            kb: kb_text
          })
        }
      ],
      temperature: 0
    });
    const parsed = parse_json_object(out.text);
    const raw_cases = Array.isArray(parsed.cases) ? parsed.cases : [];
    const allowed_audiences: QAgentCaseAudience[] =
      input.audience === "both" ? ["customer", "admin"] : [input.audience];
    const cases: PlanCase[] = [];
    for (const raw_case of raw_cases) {
      if (!is_plain_object(raw_case)) continue;
      const audience = this.normalize_case_audience(raw_case.audience);
      if (!allowed_audiences.includes(audience)) continue;
      const question = ensure_optional_string(raw_case.question);
      const intent_id = ensure_optional_string(raw_case.intent_id) ?? "qa";
      const expected_facts = normalize_string_array(raw_case.expected_facts);
      if (!question) continue;
      cases.push({
        audience,
        intent_id,
        question,
        expected_facts
      });
      if (cases.length >= input.max_cases) break;
    }
    if (cases.length === 0) {
      throw new XError("E_QAGENT_BAD_UPSTREAM", "No valid cases generated");
    }
    return cases;
  }

  private compute_baseline_score(expected_facts: string[], answer: string): number {
    if (expected_facts.length === 0) return 0;
    const answer_lower = answer.toLowerCase();
    let hits = 0;
    for (const fact of expected_facts) {
      if (answer_lower.includes(fact.toLowerCase())) hits += 1;
    }
    return clamp_score(hits / expected_facts.length);
  }

  private async judge_answer(input: {
    question: string;
    expected_facts: string[];
    answer: string;
  }): Promise<{ score: number; notes: string }> {
    try {
      const out = await this.exec_azure_openai_chat({
        messages: [
          {
            role: "system",
            content:
              "Judge whether the answer covers the expected facts. " +
              "Return JSON only: {\"score\":0,\"notes\":\"...\"}. Score must be between 0 and 1."
          },
          {
            role: "user",
            content: JSON.stringify(input)
          }
        ],
        temperature: 0
      });
      const parsed = parse_json_object(out.text);
      const raw_score = typeof parsed.score === "number" && Number.isFinite(parsed.score) ? parsed.score : 0;
      const notes = ensure_optional_string(parsed.notes) ?? "";
      return {
        score: clamp_score(raw_score),
        notes
      };
    } catch (err) {
      return {
        score: 0,
        notes: `LLM judge unavailable: ${err instanceof Error ? err.message : String(err)}`
      };
    }
  }

  private async exec_azure_openai_chat(params: {
    messages: OpenAIChatMessage[];
    temperature?: number;
  }): Promise<{ text: string }> {
    const out = await _x.execute({
      _module: AZURE_MODULE_NAME,
      _op: "openai_chat",
      _params: {
        messages: params.messages,
        ...(typeof params.temperature === "number" ? { temperature: params.temperature } : {})
      }
    });
    if (!is_plain_object(out)) {
      throw new XError("E_QAGENT_UPSTREAM", "azure.openai_chat returned invalid payload");
    }
    const text = ensure_optional_string(out.text);
    if (!text) {
      throw new XError("E_QAGENT_UPSTREAM", "azure.openai_chat.text is required");
    }
    return { text };
  }

  private async exec_agent_answer(params: {
    channel_id: string;
    text: string;
    actor_role: AgentActorRole;
    user_ref?: Record<string, unknown>;
    _ctx: Dict;
  }): Promise<{ reply_text: string }> {
    const out = await _x.execute({
      _module: AGENT_MODULE_NAME,
      _op: "answer",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_QAGENT_UPSTREAM", "agent.answer returned invalid payload");
    }
    const reply_text = ensure_non_empty_string(out.reply_text, "agent.answer.reply_text");
    return { reply_text };
  }

  private forward_ctx(ctx: AgentCommandCtx): Dict {
    const out: Dict = {};
    if (typeof ctx.kernel_cap === "string" && ctx.kernel_cap.trim().length > 0) {
      out.kernel_cap = ctx.kernel_cap;
    }
    if (ctx.actor && is_plain_object(ctx.actor)) {
      out.actor = { ...ctx.actor };
    }
    if (typeof ctx._wid === "string" && ctx._wid.trim().length > 0) {
      out._wid = ctx._wid;
    }
    if (typeof ctx._sid === "string" && ctx._sid.trim().length > 0) {
      out._sid = ctx._sid;
    }
    return out;
  }
}

export default QAgentModule;
