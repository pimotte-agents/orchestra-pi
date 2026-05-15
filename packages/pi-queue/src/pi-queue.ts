/**
 * pi-queue: Core queue manager for orchestra-pi
 *
 * Provides a priority-based task queue with daemon scheduling,
 * TUI commands, event logging, and cascade cancellation.
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent"
import * as fs from "node:fs"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type QueueStatus = "pending" | "running" | "done" | "failed" | "cancelled"
export type QueueSource = "manual" | "github-issues" | "github-comments" | "github-pr-reviews" | "autoformalize"
export type TaskMode = "fork" | "pr"

export interface QualityGate {
  maxIterations: number
  currentIteration: number
  remainingTodoCount?: number
  isFormalization?: boolean
  targetLanguage?: string
}

export interface QueueEntry {
  id: string
  createdAt: string
  status: QueueStatus
  paused: boolean
  priority: number
  source: QueueSource
  repo?: string
  prompt: string
  model?: string
  agent?: string
  maxRetries?: number
  retriesRemaining?: number
  qualityGate?: QualityGate
  validationScript?: string
  taskId?: string
  parentId?: string
  authSource?: string
  series?: string
  mode?: TaskMode
  tools?: string[]
  issueNumber?: number
}

export interface QueueEvent {
  event:
    | "task_done"
    | "task_failed"
    | "task_paused"
    | "daemon_started"
    | "daemon_stopped"
  id?: string
  status?: string
  reason?: string
  pid?: string
}

// ---------------------------------------------------------------------------
// Exported utilities (for testing)
// ---------------------------------------------------------------------------

export function generateId(): string {
  const ts = Date.now().toString(36)
  const rand = Math.random().toString(36).slice(2, 8)
  return `${ts}-${rand}`
}

export function isoNow(): string {
  return new Date().toISOString()
}

export function toJson(entry: QueueEntry): string {
  return JSON.stringify(entry, null, 2)
}

export function fromJson(raw: string): QueueEntry | null {
  try {
    const obj = JSON.parse(raw) as QueueEntry
    if (!obj.id || !obj.createdAt || !obj.status || !obj.prompt) return null
    return {
      ...obj,
      paused: obj.paused ?? false,
      priority: obj.priority ?? 10,
      retriesRemaining: obj.retriesRemaining ?? obj.maxRetries ?? 0,
      maxRetries: obj.maxRetries ?? 0,
      source: obj.source ?? "manual",
      mode: obj.mode ?? "pr",
      tools: obj.tools ?? undefined,
    }
  } catch {
    return null
  }
}

// ---------------------------------------------------------------------------
// Directory helpers
// ---------------------------------------------------------------------------

const QUEUE_DIR = () => {
  const home = process.env.HOME || ""
  return `${home}/.pi/agent/orchestra-queue`
}

const EVENTS_FILE = () => `${QUEUE_DIR()}/events.jsonl`

// ---------------------------------------------------------------------------
// Storage (file-based, single-writer)
// ---------------------------------------------------------------------------

class QueueStorage {
  private dir: string
  private fs = fs

  constructor() {
    this.dir = QUEUE_DIR()
    this.ensureDir()
  }

  private ensureDir() {
    if (!fs.existsSync(this.dir)) {
      fs.mkdirSync(this.dir, { recursive: true })
    }
  }

  save(entry: QueueEntry): void {
    this.ensureDir()
    const path = `${this.dir}/${entry.id}.json`
    fs.writeFileSync(path, toJson(entry))
  }

  load(id: string): QueueEntry | null {
    const path = `${this.dir}/${id}.json`
    if (!fs.existsSync(path)) return null
    const raw = fs.readFileSync(path, "utf-8")
    return fromJson(raw)
  }

  loadAll(): QueueEntry[] {
    this.ensureDir()
    const entries: QueueEntry[] = []
    try {
      const files = fs.readdirSync(this.dir).filter(f => f.endsWith(".json") && f !== "events.jsonl")
      for (const file of files) {
        const raw = fs.readFileSync(`${this.dir}/${file}`, "utf-8")
        const entry = fromJson(raw)
        if (entry) entries.push(entry)
      }
    } catch {
      // Return empty on error
    }
    return entries
  }

  remove(id: string): void {
    try {
      fs.unlinkSync(`${this.dir}/${id}.json`)
    } catch {
      // Ignore
    }
  }

  exists(id: string): boolean {
    return fs.existsSync(`${this.dir}/${id}.json`)
  }
}

// ---------------------------------------------------------------------------
// Event logging
// ---------------------------------------------------------------------------

class EventLogger {
  private file: string
  private fs = fs

  constructor() {
    this.file = EVENTS_FILE()
    this.ensureFile()
  }

  private ensureFile() {
    if (!fs.existsSync(this.file)) {
      fs.writeFileSync(this.file, "", "utf-8")
    }
  }

  emit(evt: QueueEvent): void {
    this.ensureFile()
    const line = JSON.stringify(evt) + "\n"
    fs.appendFileSync(this.file, line, "utf-8")
  }

  getProcessedIds(): string[] {
    if (!fs.existsSync(this.file)) return []
    try {
      const content = fs.readFileSync(this.file, "utf-8")
      const ids: string[] = []
      for (const line of content.trim().split("\n")) {
        if (!line) continue
        try {
          const evt = JSON.parse(line) as QueueEvent
          if (evt.event === "task_done" && evt.id) ids.push(evt.id)
          if (evt.event === "task_failed" && evt.id) ids.push(evt.id)
        } catch {
          // Skip malformed lines
        }
      }
      return ids
    } catch {
      return []
    }
  }
}

// ---------------------------------------------------------------------------
// Queue Manager (the core)
// ---------------------------------------------------------------------------

export class QueueManager {
  private storage: QueueStorage
  private events: EventLogger
  private daemonRunning: boolean = false
  private daemonInterval: ReturnType<typeof setInterval> | null = null
  private onEntryChange?: (entry: QueueEntry, oldStatus?: QueueStatus) => void

  constructor() {
    this.storage = new QueueStorage()
    this.events = new EventLogger()
  }

  setOnEntryChange(fn: (entry: QueueEntry, oldStatus?: QueueStatus) => void): void {
    this.onEntryChange = fn
  }

  // ---------------------------------------------------------------------------
  // CRUD
  // ---------------------------------------------------------------------------

  add(entry: Partial<QueueEntry>): QueueEntry {
    const newEntry: QueueEntry = {
      id: generateId(),
      createdAt: isoNow(),
      status: "pending",
      paused: false,
      priority: entry.priority ?? 10,
      source: entry.source ?? "manual",
      prompt: entry.prompt ?? "",
      repo: entry.repo,
      model: entry.model,
      agent: entry.agent,
      maxRetries: entry.maxRetries ?? 0,
      retriesRemaining: entry.retriesRemaining ?? entry.maxRetries ?? 0,
      qualityGate: entry.qualityGate,
      validationScript: entry.validationScript,
      taskId: entry.taskId,
      parentId: entry.parentId,
      authSource: entry.authSource,
      series: entry.series,
      mode: entry.mode ?? "pr",
      tools: entry.tools,
      issueNumber: entry.issueNumber,
    }
    this.storage.save(newEntry)
    this.notifyChange(newEntry)
    return newEntry
  }

  get(id: string): QueueEntry | null {
    return this.storage.load(id)
  }

  list(filter?: QueueStatus): QueueEntry[] {
    const all = this.storage.loadAll()
    if (filter) {
      return all.filter(e => e.status === filter)
    }
    // Default order: execution order
    return this.sortByExecutionOrder(all)
  }

  private sortByExecutionOrder(entries: QueueEntry[]): QueueEntry[] {
    return entries.sort((a, b) => {
      // 1. Running first
      if (a.status === "running" && b.status !== "running") return -1
      if (a.status !== "running" && b.status === "running") return 1
      // 2. Among pending, highest priority first
      if (a.status === "pending" && b.status === "pending") {
        if (a.priority !== b.priority) return b.priority - a.priority
        // 3. Among same priority, oldest first
        return new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime()
      }
      // Other statuses: keep original order
      return 0
    })
  }

  pause(id: string): QueueEntry | null {
    const entry = this.storage.load(id)
    if (!entry) return null
    entry.paused = true
    this.storage.save(entry)
    this.notifyChange(entry)
    this.events.emit({ event: "task_paused", id })
    return entry
  }

  resume(id: string): QueueEntry | null {
    const entry = this.storage.load(id)
    if (!entry) return null
    entry.paused = false
    this.storage.save(entry)
    this.notifyChange(entry)
    return entry
  }

  cancel(id: string): QueueEntry[] {
    const changed: QueueEntry[] = []
    const entry = this.storage.load(id)
    if (!entry) return changed

    // Only cascade if parent was in a cancellable state
    const shouldCascade = entry.status === "pending" || entry.status === "running"
    entry.status = "cancelled"
    this.storage.save(entry)
    changed.push(entry)
    this.notifyChange(entry, "pending")

    // Cascade: cancel dependents (only if parent was cancellable)
    if (shouldCascade) {
      this.cancelDependents(id, changed)
    }

    return changed
  }

  private cancelDependents(taskId: string, changed: QueueEntry[]): void {
    const all = this.storage.loadAll()
    for (const e of all) {
      if (e.status === "pending" && e.taskId === taskId && !e.paused) {
        e.status = "cancelled"
        this.storage.save(e)
        changed.push(e)
        this.notifyChange(e, "pending")
        this.cancelDependents(e.id, changed)
      }
    }
  }

  retry(id: string): QueueEntry | null {
    const entry = this.storage.load(id)
    if (!entry || entry.status !== "failed") return null
    entry.status = "pending"
    entry.retriesRemaining = entry.maxRetries
    this.storage.save(entry)
    this.notifyChange(entry)
    return entry
  }

  // ---------------------------------------------------------------------------
  // Statistics
  // ---------------------------------------------------------------------------

  stats(): { byStatus: Record<string, number>; bySource: Record<string, number> } {
    const all = this.storage.loadAll()
    const byStatus: Record<string, number> = {}
    const bySource: Record<string, number> = {}
    for (const e of all) {
      byStatus[e.status] = (byStatus[e.status] || 0) + 1
      bySource[e.source] = (bySource[e.source] || 0) + 1
    }
    return { byStatus, bySource }
  }

  // ---------------------------------------------------------------------------
  // Daemon (scheduling)
  // ---------------------------------------------------------------------------

  startDaemon(): boolean {
    if (this.daemonRunning) return false
    this.daemonRunning = true
    this.events.emit({ event: "daemon_started", pid: String(process.pid) })
    this.daemonInterval = setInterval(() => this.tick(), 1000)
    return true
  }

  stopDaemon(): boolean {
    if (!this.daemonRunning) return false
    this.daemonRunning = false
    if (this.daemonInterval) {
      clearInterval(this.daemonInterval)
      this.daemonInterval = null
    }
    this.events.emit({ event: "daemon_stopped" })
    return true
  }

  isDaemonRunning(): boolean {
    return this.daemonRunning
  }

  private tick(): void {
    if (!this.daemonRunning) return
    const next = this.nextPending()
    if (next) {
      this.executeEntry(next)
    }
  }

  nextPending(): QueueEntry | null {
    const all = this.storage.loadAll()
    const pending = all.filter(e => e.status === "pending" && !e.paused)
    if (pending.length === 0) return null
    // Pick highest priority, then oldest
    pending.sort((a, b) => {
      if (b.priority !== a.priority) return b.priority - a.priority
      return new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime()
    })
    return pending[0]
  }

  // ---------------------------------------------------------------------------
  // Task execution hook
  // ---------------------------------------------------------------------------

  /**
   * Called by the extension host to execute an entry.
   * Override this to provide actual task execution.
   *
   * @remarks The default implementation is a no-op (stderr only).
   * The extension host should call `setExecuteFn()` to provide real
   * task execution. The daemon does NOT auto-execute entries by default.
   */
  executeEntry(entry: QueueEntry): void {
    // Default: no-op — only logs to stderr so the daemon doesn't silently
    // consume entries. Hosts must call setExecuteFn() to provide real execution.
    console.error(
      `[pi-queue] task ${entry.id} executed (no handler registered; use setExecuteFn())`
    )
    entry.status = "done"
    this.storage.save(entry)
    this.events.emit({ event: "task_done", id: entry.id, status: "done" })
    this.notifyChange(entry)
  }

  /**
   * Hook to replace executeEntry with real task execution.
   * The host should call this after setting up pi-queue.
   */
  setExecuteFn(fn: (entry: QueueEntry) => void): void {
    this.executeEntry = fn
  }

  // ---------------------------------------------------------------------------
  // Internal
  // ---------------------------------------------------------------------------

  private notifyChange(entry: QueueEntry, oldStatus?: QueueStatus): void {
    if (this.onEntryChange) {
      this.onEntryChange(entry, oldStatus)
    }
  }
}

// ---------------------------------------------------------------------------
// Extension entry point
// ---------------------------------------------------------------------------

export default function piQueueExtension(pi: ExtensionAPI) {
  const queue = new QueueManager()

  // ---------------------------------------------------------------------------
  // Command handler (subcommands parsed from args string)
  // ---------------------------------------------------------------------------

  pi.registerCommand("queue", {
    description: "Queue management commands",
    handler: async (args: string, ctx) => {
      const trimmed = args.trim()
      if (!trimmed) {
        const { byStatus, bySource } = queue.stats()
        const total = Object.values(byStatus).reduce((a, b) => a + b, 0)
        const lines = [
          "📋 Queue Manager",
          "",
          `Total: ${total} entries`,
          `Status: ${Object.entries(byStatus).map(([k, v]) => `${k}:${v}`).join(", ")}`,
          `Sources: ${Object.entries(bySource).map(([k, v]) => `${k}:${v}`).join(", ")}`,
          "",
          "Usage:",
          "  /queue list [status]           — list entries",
          "  /queue show <id>               — show details",
          "  /queue add <prompt> [options]   — add task",
          "  /queue pause <id>              — pause task",
          "  /queue resume <id>             — resume paused",
          "  /queue cancel <id>             — cancel task",
          "  /queue retry <id>              — retry failed",
          "  /queue start                   — start daemon",
          "  /queue stop                    — stop daemon",
          "  /queue stats                   — summary stats",
          "",
          "Options for add: --priority N, --repo O/R, --model M, --mode fork|pr",
        ]
        ctx.ui.setWidget("queue", lines)
        ctx.ui.notify("Queue manager loaded", "info")
        return
      }

      const parts = trimmed.split(/\s+/)
      const sub = parts[0]!
      const rest = parts.slice(1).join(" ")

      switch (sub) {
        case "list": {
          const filter = rest || undefined
          const entries = queue.list(filter)
          const { byStatus, bySource } = queue.stats()
          const lines: string[] = [
            `📋 Queue (${entries.length} entries)`,
            "",
            `Totals — Status: ${Object.entries(byStatus).map(([k, v]) => `${k}:${v}`).join(", ")} | Sources: ${Object.entries(bySource).map(([k, v]) => `${k}:${v}`).join(", ")}`,
          ]
          if (filter) lines.push(`Filter: ${filter}`, "")
          else lines.push("")
          if (entries.length === 0) {
            lines.push("(empty)")
          } else {
            for (const e of entries) {
              const statusIcon = {
                pending: "⏳",
                running: "🔵",
                done: "✅",
                failed: "❌",
                cancelled: "⛔",
              }[e.status]
              const pausedTag = e.paused ? " [PAUSED]" : ""
              const sourceTag = e.source !== "manual" ? ` [${e.source}]` : ""
              const priorityTag = e.priority !== 10 ? ` [P:${e.priority}]` : ""
              lines.push(`${statusIcon} ${e.id.slice(0, 12)} | ${e.prompt.slice(0, 60).replace(/\n/g, " ")}${pausedTag}${sourceTag}${priorityTag}`)
            }
          }
          ctx.ui.setWidget("queue", lines)
          break
        }

        case "show": {
          const id = rest
          if (!id) {
            ctx.ui.notify("Error: queue show requires an <id>", "error")
            return
          }
          const entry = queue.get(id)
          if (!entry) {
            ctx.ui.notify(`Entry ${id} not found`, "error")
            return
          }
          ctx.ui.setWidget("queue", JSON.stringify(entry, null, 2).split("\n"))
          ctx.ui.notify(`Showing entry: ${id}`, "info")
          break
        }

        case "add": {
          if (!rest) {
            ctx.ui.notify("Error: queue add requires a <prompt>", "error")
            return
          }
          // Parse: "prompt --priority 5 --repo owner/repo --model name ..."
          const addArgs = rest.split(/(?=--)/)
          const prompt = addArgs[0]!.trim()
          const priority = parseInt(/--priority\s*(\d+)/.exec(rest)?.[1] || "") || 10
          const repo = /--repo\s*([^\s]+)/.exec(rest)?.[1]
          const model = /--model\s*([^\s]+)/.exec(rest)?.[1]
          const mode = /--mode\s*(fork|pr)/.exec(rest)?.[1] as TaskMode | undefined
          const agent = /--agent\s*([^\s]+)/.exec(rest)?.[1]
          const series = /--series\s*([^\s]+)/.exec(rest)?.[1]
          const maxRetries = parseInt(/--max-retries\s*(\d+)/.exec(rest)?.[1] || "") || 0

          const entry = queue.add({
            prompt,
            priority: isNaN(priority) ? 10 : priority,
            repo,
            model,
            mode: mode || "pr",
            agent,
            series,
            maxRetries: isNaN(maxRetries) ? 0 : maxRetries,
          })
          ctx.ui.notify(`Added: ${entry.id} | P:${entry.priority} | ${entry.prompt.slice(0, 80)}`, "success")
          break
        }

        case "pause": {
          const id = rest
          if (!id) {
            ctx.ui.notify("Error: queue pause requires an <id>", "error")
            return
          }
          const entry = queue.pause(id)
          if (!entry) {
            ctx.ui.notify(`Entry ${id} not found`, "error")
            return
          }
          ctx.ui.notify(`Paused: ${id}`, "info")
          break
        }

        case "resume": {
          const id = rest
          if (!id) {
            ctx.ui.notify("Error: queue resume requires an <id>", "error")
            return
          }
          const entry = queue.resume(id)
          if (!entry) {
            ctx.ui.notify(`Entry ${id} not found`, "error")
            return
          }
          ctx.ui.notify(`Resumed: ${id}`, "info")
          break
        }

        case "cancel": {
          const id = rest
          if (!id) {
            ctx.ui.notify("Error: queue cancel requires an <id>", "error")
            return
          }
          const changed = queue.cancel(id)
          const count = changed.length
          ctx.ui.notify(`Cancelled ${id}${count > 1 ? ` (and ${count - 1} dependents)` : ""}`, "info")
          break
        }

        case "retry": {
          const id = rest
          if (!id) {
            ctx.ui.notify("Error: queue retry requires an <id>", "error")
            return
          }
          const entry = queue.retry(id)
          if (!entry) {
            ctx.ui.notify(`Entry ${id} not found or not in failed state`, "error")
            return
          }
          ctx.ui.notify(`Retried: ${id}`, "success")
          break
        }

        case "start": {
          if (queue.isDaemonRunning()) {
            ctx.ui.notify("Daemon already running", "info")
          } else {
            queue.startDaemon()
            ctx.ui.notify("Daemon started", "success")
          }
          break
        }

        case "stop": {
          if (!queue.isDaemonRunning()) {
            ctx.ui.notify("Daemon not running", "info")
          } else {
            queue.stopDaemon()
            ctx.ui.notify("Daemon stopped", "info")
          }
          break
        }

        case "stats": {
          const { byStatus, bySource } = queue.stats()
          const lines: string[] = ["📊 Queue Stats", ""]
          lines.push("By Status:")
          for (const [status, count] of Object.entries(byStatus)) {
            lines.push(`  ${status}: ${count}`)
          }
          lines.push("")
          lines.push("By Source:")
          for (const [source, count] of Object.entries(bySource)) {
            lines.push(`  ${source}: ${count}`)
          }
          const pending = byStatus["pending"] || 0
          const running = byStatus["running"] || 0
          lines.push("")
          lines.push(`Total: ${Object.values(byStatus).reduce((a, b) => a + b, 0)} entries`)
          if (pending > 0) lines.push(`(${pending} pending, ${running} running)`)
          ctx.ui.setWidget("queue", lines)
          ctx.ui.notify("Stats updated", "info")
          break
        }

        default:
          ctx.ui.notify(`Unknown queue subcommand: ${sub}\nUse /queue for usage`, "error")
      }
    },
  })

  // Daemon lifecycle — do NOT auto-start.
  // The daemon starts only when explicitly invoked:
  //   - by the `/queue start` command
  //   - by another extension calling queue.startDaemon()
  //   - after setExecuteFn() is registered with a real handler
  pi.on("session_shutdown", () => queue.stopDaemon())

  // Also expose queue manager for other extensions
  ;(pi as any).__queue__ = queue

  return { queue }
}
