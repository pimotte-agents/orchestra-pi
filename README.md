# orchestra-pi

Pi extensions for agent orchestration — modular, file-based, continuously running.

## Extensions

| Extension | Purpose |
|-----------|---------|
| `pi-queue` | Core queue manager — priority scheduling, daemon, event logging |
| `pi-github-tools` | GitHub API client + native Pi tools (`create_pr`, `comment`, etc.) |
| `pi-listeners` | GitHub listener pollers — issues, comments, PR reviews |
| `pi-autoformalize` | Autoformalization loop — formalize → review (TODOs) → implement → repeat |

## Install

```bash
git clone https://github.com/pimotte-agents/orchestra-pi.git
cd orchestra-pi
./install.sh
```

This copies each extension to `~/.agent/extensions/` and creates the required state directories.

## Directory Layout (Installed)

```
~/.agent/
├── extensions/
│   ├── pi-queue.ts
│   ├── pi-github-tools.ts
│   ├── pi-listeners.ts
│   └── pi-autoformalize.ts
├── orchestra-queue/          # Queue entries + events
│   └── events.jsonl
├── orchestra-listeners/      # Listener configs
├── orchestra-listener-state/ # Listener state
├── orchestra-logs/           # Daemon logs (viewable via `tail -f`)
│   ├── daemon.log
│   └── listeners.log
└── orchestra-github.json     # GitHub config
```

## TUI Commands

### `/queue`

| Command | Description |
|---------|-------------|
| `/queue list [status]` | List entries (execution order: running → priority → oldest) |
| `/queue show <id>` | Show full entry details |
| `/queue add <prompt> [--priority N] [--repo owner/repo] [--model name] [--mode fork\|pr] [--max-retries N]` | Add a task |
| `/queue pause <id>` | Pause a task |
| `/queue resume <id>` | Resume a paused task |
| `/queue cancel <id>` | Cancel a task (cascades to dependents) |
| `/queue retry <id>` | Retry a failed task |
| `/queue stats` | Show queue statistics |

### `/listeners`

| Command | Description |
|---------|-------------|
| `/listeners list` | List configured listeners |
| `/listeners enable <name>` | Enable a listener |
| `/listeners disable <name>` | Disable a listener |

### `/formalize`

| Command | Description |
|---------|-------------|
| `/formalize <prompt>` | Start autoformalization loop |
| `/formalize resume --add-retries N` | Reset max iterations |
| `/formalize status` | Show current loop state + TODO list |
| `/formalize cancel` | Abort |
| `/formalize skip` | Skip current TODOs, move to next iteration |

## Running Tests

```bash
pnpm test                        # All packages
pnpm test:queue                  # pi-queue only
pnpm test:github                 # pi-github-tools only
pnpm test:listeners              # pi-listeners only
pnpm test:autoformalize          # pi-autoformalize only
```

## Architecture

```
┌──────────────┐  writes entries   ┌──────────────┐
│ pi-listeners │ ───────────────► │              │
│ (polls GH)   │                  │   pi-queue   │
└──────────────┘                  │   (core)     │
┌────────────────────┐  writes   │              │
│ pi-autoformalize   │ ─────────► │              │
│ (loop controller)  │           │              │
└────────────────────┘           │              │
┌──────────────┐  native tools  │┌┤              │
│ pi-github-   │ ─────────────► ││ Task Runner    │
│ tools        │ (registerTool)││ (runs pi --print)│
└──────────────┘               └──────────────────┘
```

pi-queue is the hub. Everything writes entries to it or reads events from it. GitHub tools are native Pi tools (no MCP). Extensions are independent — enable/disable any combination.
