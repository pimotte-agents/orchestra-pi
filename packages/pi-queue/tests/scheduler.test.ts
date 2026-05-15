import { describe, it, expect, beforeEach, afterEach } from "vitest"
import * as path from "node:path"
import * as fs from "node:fs"
import { QueueManager } from "../src/pi-queue"

const TMP_BASE = path.join(process.env.HOME || "", ".pi", "agent", "tmp")
function createTempDir(): string {
  if (!fs.existsSync(TMP_BASE)) {
    fs.mkdirSync(TMP_BASE, { recursive: true })
  }
  return fs.mkdtempSync(path.join(TMP_BASE, "pi-queue-test-"))
}

function setupTempQueue(tempDir: string): void {
  const queueDir = path.join(tempDir, ".pi", "agent", "orchestra-queue")
  fs.mkdirSync(queueDir, { recursive: true })
  process.env.HOME = tempDir
}

function cleanupTempQueue(tempDir: string): void {
  if (process.env.HOME?.includes("pi-queue-test-")) {
    fs.rmSync(process.env.HOME, { recursive: true, force: true })
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

describe("Scheduler: priority ordering", () => {
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

  it("picks highest priority first", () => {
    queue.add({ prompt: "low", priority: 1 })
    queue.add({ prompt: "high", priority: 100 })
    queue.add({ prompt: "mid", priority: 50 })
    const next = queue.nextPending()
    expect(next!.priority).toBe(100)
  })

  it("picks oldest among same priority", () => {
    const first = queue.add({ prompt: "first", priority: 10 })
    // Small delay to ensure different createdAt timestamps
    const start = Date.now()
    while (Date.now() - start < 2) {}
    queue.add({ prompt: "second", priority: 10 })
    const next = queue.nextPending()
    expect(next!.id).toBe(first.id)
  })

  it("returns null when no pending tasks", () => {
    expect(queue.nextPending()).toBeNull()
  })

  it("skips paused tasks", () => {
    const normal = queue.add({ prompt: "normal", priority: 10 })
    const paused = queue.add({ prompt: "paused", priority: 100 })
    queue.pause(paused.id)
    const next = queue.nextPending()
    expect(next!.id).toBe(normal.id)
  })

  it("skips non-pending tasks", () => {
    const done = queue.add({ prompt: "done" })
    done.status = "done"
    queue["storage"].save(done)
    queue.add({ prompt: "pending" })
    const next = queue.nextPending()
    expect(next!.status).toBe("pending")
    expect(next!.id).not.toBe(done.id)
  })
})

describe("Cascade cancellation", () => {
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

  it("cancels dependents when parent is cancelled", () => {
    const parent = queue.add({ prompt: "parent" })
    const child1 = queue.add({ prompt: "child1", taskId: parent.id })
    const child2 = queue.add({ prompt: "child2", taskId: child1.id })

    const changed = queue.cancel(parent.id)
    expect(changed.length).toBe(3) // parent + 2 children
    expect(changed.find(e => e.id === child2.id)!.status).toBe("cancelled")
  })

  it("does not cascade from non-pending entries", () => {
    const parent = queue.add({ prompt: "parent" })
    parent.status = "done"
    queue["storage"].save(parent)
    const child = queue.add({ prompt: "child", taskId: parent.id })

    const changed = queue.cancel(parent.id)
    // Child should not be cancelled because parent is already done
    expect(changed.length).toBe(1)
    expect(child.status).toBe("pending")
  })
})

describe("Statistics", () => {
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

  it("counts by status", () => {
    queue.add({ prompt: "a" })
    queue.add({ prompt: "b" })
    const c = queue.add({ prompt: "c" })
    c.status = "failed"
    queue["storage"].save(c)
    const { byStatus } = queue.stats()
    expect(byStatus["pending"]).toBe(2)
    expect(byStatus["failed"]).toBe(1)
  })

  it("counts by source", () => {
    queue.add({ prompt: "a", source: "manual" })
    queue.add({ prompt: "b", source: "github-issues" })
    queue.add({ prompt: "c", source: "github-issues" })
    const { bySource } = queue.stats()
    expect(bySource["manual"]).toBe(1)
    expect(bySource["github-issues"]).toBe(2)
  })
})
