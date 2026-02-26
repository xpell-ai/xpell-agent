import process from "node:process";

type CliOptions = {
  _url: string;
  _sid?: string;
};

type ReqEnvelope = {
  _v: 2;
  _id: string;
  _kind: "REQ";
  _sid?: string;
  _payload: {
    _module: string;
    _op: string;
    _params: Record<string, unknown>;
  };
};

const DEFAULT_URL = "http://127.0.0.1:3090/wh/v2/call";
const DEFAULT_MODULE = "agent";

function print_usage(): void {
  const usage = [
    "Usage:",
    "  pnpm agent:call <module> <op> [params_json] [--url <call_url>] [--sid <sid>]",
    "  pnpm agent:call <op> [params_json] [--url <call_url>] [--sid <sid>]  # shorthand for module='agent'",
    "",
    "Examples:",
    "  pnpm agent:call users list",
    "  pnpm agent:call channels list",
    "  pnpm agent:call conv list_threads '{\"limit\":10}'",
    "  pnpm agent:call status --url http://127.0.0.1:3090/wh/v2/call"
  ];
  console.log(usage.join("\n"));
}

function parse_options(argv: string[]): {
  _module?: string;
  _op?: string;
  _params: Record<string, unknown>;
  _opts: CliOptions;
} {
  const opts: CliOptions = { _url: DEFAULT_URL };
  const args: string[] = [];

  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === "--url") {
      const value = argv[i + 1];
      if (!value) throw new Error("Missing value for --url");
      opts._url = value;
      i += 1;
      continue;
    }
    if (token === "--sid") {
      const value = argv[i + 1];
      if (!value) throw new Error("Missing value for --sid");
      opts._sid = value;
      i += 1;
      continue;
    }
    if (token === "--help" || token === "-h") {
      print_usage();
      process.exit(0);
    }
    args.push(token);
  }

  if (args.length === 0) return { _params: {}, _opts: opts };

  let module_name = DEFAULT_MODULE;
  let op = args[0];
  let raw_params: string | undefined = args[1];

  const is_json_arg = typeof args[1] === "string" && args[1].trim().startsWith("{");

  if (args.length >= 2 && !is_json_arg) {
    module_name = args[0];
    op = args[1];
    raw_params = args[2];
  }

  if (!op) return { _params: {}, _opts: opts };
  if (!raw_params) return { _module: module_name, _op: op, _params: {}, _opts: opts };

  try {
    const parsed = JSON.parse(raw_params);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("params_json must be a JSON object");
    }
    return { _module: module_name, _op: op, _params: parsed as Record<string, unknown>, _opts: opts };
  } catch (err) {
    const message = err instanceof Error ? err.message : "Invalid params_json";
    throw new Error(`Invalid params_json: ${message}`);
  }
}

function build_req(module_name: string, op: string, params: Record<string, unknown>, opts: CliOptions): ReqEnvelope {
  const now = Date.now();
  const suffix = Math.random().toString(36).slice(2, 8);
  return {
    _v: 2,
    _id: `req_${now}_${suffix}`,
    _kind: "REQ",
    ...(opts._sid ? { _sid: opts._sid } : {}),
    _payload: {
      _module: module_name,
      _op: op,
      _params: params
    }
  };
}

async function main(): Promise<void> {
  const { _module: module_name, _op: op, _params: params, _opts: opts } = parse_options(process.argv.slice(2));
  if (!module_name || !op) {
    print_usage();
    process.exitCode = 1;
    return;
  }

  const req_env = build_req(module_name, op, params, opts);
  const headers: Record<string, string> = {
    "content-type": "application/json"
  };
  if (opts._sid && opts._sid.trim().length > 0) {
    headers["x-wormholes-sid"] = opts._sid.trim();
  }

  const response = await fetch(opts._url, {
    method: "POST",
    headers,
    body: JSON.stringify(req_env)
  });

  const text = await response.text();
  let payload: unknown = text;
  try {
    payload = JSON.parse(text);
  } catch {
    // keep raw body when not JSON
  }

  console.log(JSON.stringify(payload, null, 2));

  if (!response.ok) {
    process.exitCode = 1;
    return;
  }

  if (
    payload &&
    typeof payload === "object" &&
    (payload as any)._payload &&
    typeof (payload as any)._payload === "object" &&
    (payload as any)._payload._ok === false
  ) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  const message = err instanceof Error ? err.message : String(err);
  console.error(`[agent:call] ${message}`);
  process.exitCode = 1;
});
