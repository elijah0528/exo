# Codex on Exoharness

Goal: run `codex` (TUI, `codex exec`, app-server) unchanged while every
effectful primitive underneath it — process execution, filesystem, network,
state, secrets — is backed by exoharness instead of codex's own machinery.
Codex keeps the semantics (prompts, model calls, compaction, approvals UX,
the turn loop); exoharness owns the substrate. Once the primitives route
through exoharness, everything exoharness provides — snapshots, forks,
remote sandbox providers, durable filesystems, secrets, the canonical event
log — becomes available to a stock codex session.

This is the "virtualize their exoharness-like components" goal stated in
`exoharness/docs/spec.md`, applied to Codex.

## Where Codex's primitives live

The pinned `codex/` gitlink resolves to upstream `openai/codex` at commit
`b348fc26` (codex-rs workspace, ~116 crates). Reading that source, codex's
primitives fall into three layers:

### 1. The environment seam (already virtualized)

Codex does not exec or touch the filesystem directly. Every effectful
operation goes through an `Environment` (`codex-rs/exec-server`), which is a
bundle of three traits over an RPC protocol (`exec-server-protocol`):

- **`ExecBackend` / `ExecProcess`** — `start(ExecParams)` returns a process
  handle with `read(after_seq, max_bytes, wait_ms)` (cursor-paged output),
  `write(stdin)`, `signal`, `terminate`, and `subscribe_events`. Backs
  `unified_exec` / `shell` / user-shell tools, including PTY sessions.
- **`ExecutorFileSystem`** (`codex-rs/file-system`) — `canonicalize`,
  `read_file`, `read_file_stream`, `write_file`, `create_directory`,
  `get_metadata`, `read_directory`, `walk`, `remove`, `copy`. Backs
  `apply_patch` (`core/src/tools/runtimes/apply_patch.rs` calls
  `environment.get_filesystem()`), file reads, image loads, directory scans.
- **`HttpClient`** — `http_request` / `http_request_stream`, the
  environment-scoped network capability with its own policy decider hook.

Environment selection is already a config primitive: `$CODEX_HOME/
environments.toml` declares `[[environments]]` with `id` + `url`
(websocket) **or `program`/`args` (stdio transport)** — e.g. upstream's own
example `program = "codex", args = ["exec-server", "--listen", "stdio"]`.
`CODEX_EXEC_SERVER_URL` selects one globally, and `thread/start` accepts a
per-thread `environments` override. `environment.is_remote()` already gates
codex behavior (remote shell snapshots, network policy deciders, approval
prompts for external sandboxes).

This is the seam that makes "replace all of codex's primitives" tractable:
nearly every codex tool (shell, unified_exec, apply_patch, file ops, network
access) resolves to methods on these three traits.

### 2. State primitives (local files, not virtualized)

- `codex-rs/rollout` — JSONL rollout files under `CODEX_HOME/sessions`, a
  sqlite session index, writer locks. This is codex's "canonical history".
- `codex-rs/state` — sqlite store for threads, logs, memories, queue, goals,
  projects, thread attachments.
- `codex-rs/history` — prompt history.
- `codex-rs/config` + `codex-home` + `config-schema` — `config.toml`,
  `auth.json`, CODEX_HOME resolution.
- `codex-rs/login`, `keyring-store`, `secrets` — credential material.

These are the exoharness-like components to virtualize onto conversations,
events, artifacts, bindings, and secrets.

### 3. Conversation primitives (the parallel data model)

Codex's app-server exposes a full conversation surface that lines up with
the exoharness data model almost one-to-one:

| codex (app-server v2) | exoharness |
|---|---|
| `thread/start` | `new_conversation` (per `ThreadStartParams.environments` → sandbox scope) |
| `thread/resume` | `get_conversation` + materialize prompt state from events (replaces rollout-file hydration) |
| `thread/fork` | `conversation.fork` — and unlike codex, a paired `fork_sandbox` rewinds the *filesystem* too |
| `thread/rollback` / `thread/revert` | `conversation.fork(up_to_inclusive)` — codex's own docs note rollback "does not revert local file changes"; exo time travel rewinds history **and** sandbox state |
| `thread/archive`/`unarchive`/`delete`, `name/set`, `metadata/update` | conversation record fields / `delete_conversation` (may need an `update_conversation` request — gap) |
| `thread/list` / `thread/read` / `thread/search*` | `list_conversations`, `get_events` + materialize; search is executor-side over events |
| live session (opening a thread) | `start_session` / `end_session` |
| `turn/start` | `begin_turn` |
| `turn/completed` | `turn.finish` |
| `turn/steer` (mid-turn input) | `turn.add_events` (injects into the active turn's log) — needs codex-side plumbing to surface it as a user item |
| `turn/interrupt` | gap — no turn-cancel request yet; today approximated by `cancel_sandbox_process` on in-flight calls |
| `thread/inject_items` | `add_events` / materialized-history → prompt items (what `codex-harness.ts` already does on cold start) |
| `thread/items|turns|timeline/list` | `get_events` with type filters |
| `thread/compact/start` | stays executor-side; the summary/derived view is stored as a custom event pointing at an artifact, per `spec.md` |
| `thread/queue/*`, `goal/*`, `section*`, `memoryMode`, `backgroundTerminals/*` | custom events + artifacts on the conversation (queue/goals are just durable state) |
| `project/*` (thread grouping by workspace) | agent + naming convention on conversations, or a lightweight project object if we add one |
| rollout `ResponseItem`s + sqlite `state/` rows | `Event`s — `messages`, `tool_requested`, `tool_result`, `codex_*` custom types |

The deep version of this — codex's rollout/state stores *replaced* by the
exoharness event log rather than mirrored into it — is the fork-level work
in Phase 4. The cheap version ships earlier: `CODEX_HOME` on a durable
filesystem keeps rollouts persistent, while the harness/bridge mirrors
items into events so exo remains the queryable canonical record.

### 4. Semantic primitives (stay in codex — executor domain)

Per the spec, anything semantic belongs to the executor: prompt assembly,
compaction policy, approvals policy and UX, `plan`/`update_plan`,
`tool_search`, MCP client connections (`codex-mcp`/`rmcp-client`),
`multi_agents` subagent spawning, plugins, skills. Codex keeps these; we
only want their *outputs* to land in exoharness (e.g. approval decisions
and tool activity recorded as events).

## Target architecture

### Phase 0/1 — exec-server bridge (no codex fork)

Ship an exo-side bridge that presents exoharness as a codex "remote
environment" over the stdio transport codex already supports:

```toml
# $CODEX_HOME/environments.toml
default = "exo"
include_local = true

[[environments]]
id = "exo"
program = "exo"
args = ["codex-env", "--sandbox", "<sandbox-id-or-binding>"]
# plus env config, e.g. EXOHARNESS_URL=http://127.0.0.1:4766
```

`exo codex-env` (new CLI subcommand backed by a small crate) speaks the
`exec-server-protocol` framing on stdio and translates each request onto an
exoharness `SandboxHandle`/`HttpExoHarness`:

| exec-server request | exoharness call |
|---|---|
| `exec` (ExecParams: argv, env, cwd, pty?) | `start_sandbox_process` (`mode: exec|pty`, `stdin: open|none`, `output: stream`, `lifecycle: attached`) |
| `read` (after_seq, max_bytes, wait) | `get_sandbox_process_events { after: cursor, follow: true }` |
| `write` (stdin chunk) | `write_sandbox_process_input` |
| `signal` / `terminate` | `cancel_sandbox_process { signal }` |
| `fs.*` (read/write/mkdir/stat/walk/rm/cp/canonicalize) | phase 1: shell-out via a persistent `sh`/`helper` process inside the sandbox (cat/base64/find/stat/dd — same trick `crates/excode` uses for `read_file`/`list_files`); phase 2: first-class `SandboxFs*` requests in `protocol::Request` |
| `http` (env-scoped network) | gap — see below |
| `initialize` / capability discovery | static `EnvironmentInfo` synthesized from the sandbox record; capability roots ≈ sandbox mounts |
| `shell_snapshot` | read the sandbox shell's env via a probe process, or expose via the fs/exec shim |

With `sandboxPolicy = externalSandbox` (already upstream), codex treats the
environment as the isolation boundary and skips its own landlock/seatbelt
layer — exactly right when the environment is an exoharness sandbox
(docker, smolvm, firecracker, daytona, vercel, e2b, sprites, aws_agentcore).

The workspace dir maps to a sandbox `FileSystemMount`; `CODEX_HOME` inside
the sandbox maps to a `DurableFileSystem` so auth/rollout/history survive
sandbox restarts.

Result: `codex` runs as normal; every command, patch, file read/write, and
network call executes inside an exoharness-managed sandbox and is recorded
in the canonical event log (`SandboxProcessStarted`, process events, fs
events once they exist).

### Phase 2 — state virtualization

Point codex's local state at durable exoharness primitives:

- `CODEX_HOME` on a durable FS mount (auth.json, sessions/) — cheapest step,
  no code changes.
- Rollout → canonical events: mirror `ResponseItem`s and turn lifecycle into
  conversation events (`messages`, `tool_requested`, `tool_result`, custom
  `codex_*` types — the TS harness already projects these). Codex `resume`
  can later hydrate from the event log instead of rollout files (needs the
  fork or an import tool).
- sqlite `state/` → conversations + artifacts: threads → conversations,
  memories/queue/goals → artifacts or custom events. This is the part most
  likely to require a codex fork (see below).

### Phase 3 — config/secrets/auth

- `config.toml` values → exoharness bindings: `Binding::Env` for env vars,
  `Binding::Llm` for model provider/base-url, `Binding::Mcp` for MCP servers,
  `Binding::Sandbox` for the environment itself.
- `auth.json` / API keys → `Secret` (key or oauth) material injected into
  sandbox process env via `StartSandboxProcessRequest.env` — never visible
  to the model, matching the secrets spec.
- `exo codex-env` can generate the `environments.toml` + minimal
  `config.toml` from an agent's bindings, so "run codex as normal" becomes
  `exo codex --sandbox <id>` (a thin wrapper that sets `CODEX_HOME` and
  execs stock codex).

### Phase 4 — fork codex for what's left (decision point)

Only things the environment seam can't reach justify a fork:

- Approval routing: `item/*requestApproval` decisions recorded as exoharness
  events; exo policy (e.g. auto-approve inside a trusted sandbox scope)
  answered harness-side — the TS harness already intercepts these.
- Rollout writer → event-log writer, for native resume-from-events.
- MCP bindings → codex's MCP client config.
- `multi_agents` spawn → exoharness conversations/forks.

Keep the fork a thin delta on top of upstream so rebases stay cheap. The
existing `codex/` gitlink pins upstream — swapping it to a fork is a
one-line change when we need it.

## Exoharness gaps to close

1. **Filesystem primitives.** `protocol::Request` has process ops but no
   `Fs*`. Phase 1 uses exec shims (fine for patches; slow for big walks).
   Phase 2 should add `SandboxReadFile`/`SandboxWriteFile`/`SandboxListDir`/
   `SandboxWalk`-style requests implemented per provider (docker exec,
   firecracker guest agent, local fs) — needed anyway for a first-class
   `ExecutorFileSystem`.
2. **`follow` in `SandboxProcessEventQuery` is defined but unimplemented.**
   The bridge needs long-poll/streaming reads for `ExecProcess.read` and
   `subscribe_events`; implement `follow` server-side (or add a streaming
   endpoint per `http.md`'s "future streaming endpoints").
3. **Environment HTTP.** Codex's `HttpClient` does env-scoped requests with
   a policy decider. Exoharness today has a boolean `enable_networking`.
   Options: exec `curl` inside the sandbox (phase-1 shim), or add an
   egress-policy surface (host allowlist per sandbox) so
   `NetworkPolicyDecider` can be answered by exo policy.
4. **Signals.** `cancel_sandbox_process` takes a free-form signal string —
   verify providers deliver SIGINT/SIGTERM/SIGKILL distinctly; PTY mode
   should also support ctrl-c via stdin byte.
5. **Capability roots / shell snapshot.** Codex discovers tools/skills roots
   per environment; map to sandbox mounts + a canned snapshot to avoid a
   probe exec per turn.

## What this unlocks

- `codex` in a Firecracker microVM / Daytona / Vercel / E2B / Sprites /
  AWS AgentCore sandbox by changing one binding — cloud execution with zero
  codex changes.
- Snapshot the sandbox mid-turn, fork the conversation + sandbox, resume —
  time-travel and branching for codex sessions.
- Durable CODEX_HOME and workspace across sandbox restarts.
- Every exec/fs/network effect recorded as canonical events → replay,
  audit, and a foundation for RSI experiments on a mainstream agent.
- Secrets reachable by codex tools without model exposure.

## Milestones

1. `exo codex-env` bridge: initialize + exec/read/write/terminate + exec-fs
   shim; `environments.toml` generator; E2E `codex exec` in a docker
   sandbox. (~1 session)
2. Follow-mode process events in exoharness; PTY + signals; fs shim
   hardening (quoting, binary-safe transfer, big walks). (~1 session)
3. Durable CODEX_HOME + workspace mounts; secrets→env injection; `exo codex`
   wrapper. (part of 1–2)
4. First-class FS requests in the protocol; env-scoped HTTP policy hook.
   (~1 session, provider-dependent)
5. Optional codex fork: rollout→events, approvals-as-events, MCP bindings,
   multi-agent mapping. (1–2 sessions)

## Test plan

- Bridge unit tests against `exec-server-protocol` fixtures; codex's own
  exec-server transport tests where reusable.
- Exoharness contract tests for any new protocol requests.
- E2E: `exo serve` + `CODEX_HOME` with generated `environments.toml` +
  `codex exec "create a file and read it back"` inside a docker sandbox —
  assert file lands in sandbox, events recorded, secrets injected.

## Open questions

- Do we want the bridge to target one sandbox per codex *process*, per
  *thread*, or per *turn*? (`SandboxScope` supports all three; per-thread
  with warm reuse matches the existing codex harness.)
- Keep `include_local` alongside the exo environment for fallback, or hard
  cutover?
- Fork timing: how much of Phase 4 do we want before maintaining a codex
  delta becomes worth it — and should the fork live at `codex/` (replace
  the upstream gitlink) or as a separate repo dependency?
- Codex `wait_for_environment` and environment-status UX assume codex-owned
  environments; what's the right story for exo sandbox provisioning time?
