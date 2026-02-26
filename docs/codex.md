Use skills: 
- xpell-contract 
- xpell-core 
- xpell-node 
- xpell-ui
- server-xvm 
- xdashboard 
- xpell-app
- xbot-skill 
  
  
This repository implements **Xpell Agent (XBot)** --- a deterministic
agent runtime (agent-core) + operational cockpit (agent-ui) built on
Xpell 2 Alpha.

This document is a STRICT CONTRACT for all code generation in this repo.

  -----------------------
  ARCHITECTURE OVERVIEW
  -----------------------

Packages:

-   packages/agent-core → Agent runtime (Node.js) Built on @xpell/node +
    @xpell/core

-   packages/agent-ui → Agent Control Panel (ACP) Built on @xpell/ui +
    @xpell/core + @xpell/xdashboard

Separation is mandatory: - No server logic inside UI - No UI logic
inside server - No shared hidden state

  --------------------------------------------
  GLOBAL RUNTIME DISCIPLINE (xpell-contract)
  --------------------------------------------

Single source of truth: xpell-contract.

Rules:

-   XModule is the ONLY server behavior extension point.
-   XObject must remain UI-free.
-   UI behavior lives ONLY in @xpell/ui (XUIObject).
-   XData is shared runtime memory --- no mirrored hidden state.
-   XEM is a decoupling bus --- never encode state/control flow in
    events.
-   Prefer Nano-Commands v2 JSON handlers + sequences for UI actions
    (data-first views).
-   No implicit background loops or polling.

  ------------------------------
  SERVER (agent-core) CONTRACT
  ------------------------------

All capabilities are implemented as XModule ops.

Transport: - Client/server communication MUST use Wormholes v2
envelopes. - No ad-hoc JSON APIs. - No custom protocol layers. - Gateway
must validate envelope shape. - Server must inject controlled \_ctx
before calling \_x.execute().

Transport Policy (Non-Negotiable): - Prefer @xpell/node bootstrap +
Wormholes gateways. - If hosting inside a custom HTTP server, it MUST
mount Wormholes REST + WS bridge. - Do NOT re-implement Wormholes
manually. - Do NOT create parallel REST endpoints unless explicitly
requested.

Execution: - All external behavior MUST route through \_x.execute(). -
Never bypass module boundary. - Never expose internal helpers directly.

Persistence: - Do not claim persistence unless implemented. - When
persistence is required, follow server-xvm repo + codec patterns. - No
implicit migrations. - No hidden file writes.

Settings: - All configurable state MUST go through SettingsModule. -
Skill settings MUST be stored under: skills.`<skill_id>`{=html}.\* - UI
modifies settings only via `_x.execute("settings", ...)`.

Security & Capabilities: - Admin operations require authenticated
session + capability checks. - Skills receive restricted kernel
capability surface only. - Skills MUST NOT import `_x` directly from
@xpell/core. - Skills operate only through provided kernel API.

Forbidden: - setInterval polling - unbounded retries - background worker
loops unless explicitly requested

  ------------------------
  UI (agent-ui) CONTRACT
  ------------------------

UI is an OPERATIONAL COCKPIT --- not marketing.

It must display: - Agent status - Skills list + enable/disable -
Settings editor - Admin users - Logs / operational state

Architecture: - Use XVMApp + XVM for navigation. - Views must be
data-first (Nano-Commands v2). - No inline JS handlers inside persisted
views. - No external frameworks (React/Vue/etc). - Use XDashboard
components for layout.

Handlers policy: - Generated dashboards MUST use Nano-Commands v2
(text/JSON). - JS function handlers allowed ONLY for local prototypes. -
Persisted views MUST NOT contain JS functions.

Transport: - UI must communicate via Wormholes client. - No direct
fetch() calls unless explicitly requested.

Styling: - Reuse existing CSS. - Avoid introducing custom global styles
unless necessary.

  -----------------------
  STATE + DOCUMENTATION
  -----------------------

Any new: - XData keys - Module ops - Skill contracts - Settings keys

MUST be documented.

No hidden conventions.

  ------
  GOAL
  ------

Keep it:

-   Minimal
-   Deterministic
-   Runnable
-   Real-time
-   Aligned with Xpell 2 architecture

This repository demonstrates:

A practical, extensible agent runtime + A real-time operational UI +
Skill-based capability model + Deterministic full-stack architecture.
