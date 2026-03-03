export type QAgentRunStatus = "created" | "running" | "done" | "failed";

export type QAgentAudience = "customer" | "admin" | "both";

export type QAgentCaseAudience = "customer" | "admin";

export type QAgentTotals = {
  cases_total: number;
  cases_passed: number;
  avg_score: number;
  pass_rate: number;
};

export type QAgentRun = {
  _id: string;
  _app_id: string;
  _env: string;
  _status: QAgentRunStatus;
  _created_at: number;
  _updated_at: number;
  _kb_files: string[];
  _agent_name: string;
  _agent_role: string;
  _summary?: string;
  _meta_json?: string;
  _totals_json?: string;
};

export type QAgentCase = {
  _id: string;
  _run_id: string;
  _app_id: string;
  _env: string;
  _case_idx: number;
  _audience: QAgentCaseAudience;
  _intent_id: string;
  _question: string;
  _expected_facts: string[];
  _answer: string;
  _score: number;
  _judge_notes: string;
  _created_at: number;
  _updated_at: number;
};
