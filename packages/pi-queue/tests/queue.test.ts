import { describe, it, expect, beforeEach, afterEach } from "vitest"
import * as fs from "node:fs"
import * as path from "node:path"
import * as os from "node:os"
import { QueueManager, fromJson, QueueEntry } from "../src/pi-queue"

// Helper to create a temporary directory for queue storage
const TMP_BASE = path.join(process.env.HOME || "", ".pi", "agent", "tmp")
function createTempDir(): string {
  if (!fs.existsSync(TMP_BASE)) {
    fs.mkdirSync(TMP_BASE, { recursive: true })
  }
  return fs.mkdtempSync(path.join(TMP_BASE, "pi-queue-test-"))
}

// Patch the QUEUE_DIR constant by setting HOME to a temp dir
function setupTempQueue(tempDir: string): string {
  const oldHome = process.env.HOME
  const queueDir = path.join(tempDir, ".pi", "agent", "orchestra-queue")
  fs.mkdirSync(queueDir, { recursive: true })
  process.env.HOME = tempDir
  return queueDir
}

function cleanupTempQueue(tempDir: string): void {
  const oldHome = process.env.HOME
  if (oldHome && oldHome.includes("pi-queue-test-")) {
    fs.rmSync(oldHome, { recursive: true, force: true })
  }
  // Also clean the specific tempDir if HOME wasn't changed
  if (fs.existsSync(tempDir) && tempDir.startsWith(TMP_BASE)) {
    try {
      fs.rmSync(tempDir, { recursive: true, force: true })
    } catch {
      // Ignore
    }
  }
}

describe("fromJson", () => {
  it("parses a valid entry", () => {
    const raw = JSON.stringify({
      id: "test-1",
      createdAt: "2026-01-01T00:00:00Z",
      status: "pending",
      prompt: "test prompt",
      priority: 10,
      source: "manual",
    })
    const entry = fromJson(raw)
    expect(entry).not.toBeNull()
    expect(entry!.id).toBe("test-1")
    expect(entry!.prompt).toBe("test prompt")
  })

  it("applies defaults", () => {
    const raw = JSON.stringify({
      id: "test-2",
      createdAt: "2026-01-01T00:00:00Z",
      status: "pending",
      prompt: "test",
    })
    const entry = fromJson(raw)
    expect(entry).not.toBeNull()
    expect(entry!.priority).toBe(10)
    expect(entry!.paused).toBe(false)
    expect(entry!.source).toBe("manual")
    expect(entry!.maxRetries).toBe(0)
  })

  it("returns null for invalid JSON", () => {
    expect(fromJson("not json")).toBeNull()
  })

  it("returns null for entry without required fields", () => {
    expect(fromJson(JSON.stringify({ id: "test" }))).toBeNull()
  })
})

describe("QueueManager CRUD", () => {
  let queue: QueueManager
  let tempDir: string
  let oldHome: string | undefined

  beforeEach(() => {
    tempDir = createTempDir()
    oldHome = process.env.HOME
    setupTempQueue(tempDir)
    queue = new QueueManager()
  })

  afterEach(() => {
    cleanupTempQueue(tempDir)
    process.env.HOME = oldHome
  })

  it("adds an entry and returns it", () => {
    const entry = queue.add({ prompt: "do something" })
    expect(entry.id).toBeTruthy()
    expect(entry.status).toBe("pending")
    expect(entry.prompt).toBe("do something")
    expect(entry.priority).toBe(10)
    expect(entry.source).toBe("manual")
  })

  it("adds with custom priority", () => {
    const entry = queue.add({ prompt: "urgent", priority: 20 })
    expect(entry.priority).toBe(20)
  })

  it("adds with quality gate", () => {
    const entry = queue.add({
      prompt: "formalize X",
      qualityGate: { maxIterations: 5, currentIteration: 0 },
    })
    expect(entry.qualityGate).not.toBeNull()
    expect(entry.qualityGate!.maxIterations).toBe(5)
  })

  it("gets an existing entry", () => {
    const entry = queue.add({ prompt: "test" })
    const found = queue.get(entry.id)
    expect(found).not.toBeNull()
    expect(found!.id).toBe(entry.id)
  })

  it("returns null for non-existent entry", () => {
    expect(queue.get("nonexistent")).toBeNull()
  })

  it("pauses a task", () => {
    const entry = queue.add({ prompt: "test" })
    const paused = queue.pause(entry.id)
    expect(paused).not.toBeNull()
    expect(paused!.paused).toBe(true)
  })

  it("resumes a paused task", () => {
    const entry = queue.add({ prompt: "test" })
    queue.pause(entry.id)
    const resumed = queue.resume(entry.id)
    expect(resumed).not.toBeNull()
    expect(resumed!.paused).toBe(false)
  })

  it("cancels a task", () => {
    const entry = queue.add({ prompt: "test" })
    const changed = queue.cancel(entry.id)
    expect(changed.length).toBe(1)
    expect(changed[0].status).toBe("cancelled")
  })

  it("retries a failed task", () => {
    const entry = queue.add({ prompt: "test", maxRetries: 3 })
    // Manually set to failed for testing
    entry.status = "failed"
    queue["storage"].save(entry)
    const retried = queue.retry(entry.id)
    expect(retried).not.toBeNull()
    expect(retried!.status).toBe("pending")
  })

  it("does not retry a non-failed task", () => {
    const entry = queue.add({ prompt: "test" })
    expect(queue.retry(entry.id)).toBeNull()
  })

  it("persists entries to disk", () => {
    const entry = queue.add({ prompt: "persist test" })
    const reloaded = queue.get(entry.id)
    expect(reloaded).not.toBeNull()
    expect(reloaded!.prompt).toBe("persist test")
  })
})

describe("QueueManager list ordering", () => {
  let queue: QueueManager
  let tempDir: string
  let oldHome: string | undefined

  beforeEach(() => {
    tempDir = createTempDir()
    oldHome = process.env.HOME
    setupTempQueue(tempDir)
    queue = new QueueManager()
  })

  afterEach(() => {
    cleanupTempQueue(tempDir)
    process.env.HOME = oldHome
  })

  it("lists all entries when no filter", () => {
    queue.add({ prompt: "a", priority: 5 })
    queue.add({ prompt: "b", priority: 10 })
    queue.add({ prompt: "c", priority: 10 })
    const list = queue.list()
    expect(list.length).toBe(3)
    // Default order: pending sorted by priority desc, then oldest first
    expect(list[0].priority).toBe(10)
    expect(list[1].priority).toBe(10)
    expect(list[2].priority).toBe(5)
  })

  it("filters by status", () => {
    queue.add({ prompt: "pending" })
    queue.add({ prompt: "pending" })
    const doneEntry = queue.add({ prompt: "done" })
    doneEntry.status = "done"
    queue["storage"].save(doneEntry)
    const list = queue.list("done")
    expect(list.length).toBe(1)
    expect(list[0].status).toBe("done")
  })

  it("sorts execution order: running first", () => {
    const pending = queue.add({ prompt: "pending" })
    const running = queue.add({ prompt: "running" })
    running.status = "running"
    queue["storage"].save(running)
    const list = queue.list()
    expect(list[0].status).toBe("running")
    expect(list[1].status).toBe("pending")
  })

  it("sorts by priority among same status", () => {
    queue.add({ prompt: "low", priority: 1 })
    queue.add({ prompt: "high", priority: 20 })
    queue.add({ prompt: "mid", priority: 10 })
    const list = queue.list()
    expect(list[0].priority).toBe(20)
    expect(list[1].priority).toBe(10)
    expect(list[2].priority).toBe(1)
  })

  it("breaks priority ties by oldest first", () => {
    const first = queue.add({ prompt: "first", priority: 10 })
    // Small delay to ensure different createdAt timestamps
    const start = Date.now()
    while (Date.now() - start < 2) {}
    queue.add({ prompt: "second", priority: 10 })
    const list = queue.list()
    expect(list[0].id).toBe(first.id)
    expect(list[1].id).not.toBe(first.id)
  })

  it("skips paused tasks in nextPending", () => {
    const normal = queue.add({ prompt: "normal", priority: 10 })
    const paused = queue.add({ prompt: "paused", priority: 100 })
    queue.pause(paused.id)
    const next = queue.nextPending()
    expect(next!.id).toBe(normal.id)
    // Verify paused task is not returned
    expect(next!.id).not.toBe(paused.id)
  })
})
