import { describe, it, expect, beforeEach, afterEach } from "vitest"
import * as path from "node:path"
import * as fs from "node:fs"
import { QueueManager, QueueEntry } from "../src/pi-queue"

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

describe("Daemon lifecycle", () => {
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
    queue.stopDaemon()
    cleanupTempQueue(tempDir)
    process.env.HOME = oldHome
  })

  it("starts the daemon", () => {
    expect(queue.isDaemonRunning()).toBe(false)
    const result = queue.startDaemon()
    expect(result).toBe(true)
    expect(queue.isDaemonRunning()).toBe(true)
  })

  it("returns false when already running", () => {
    queue.startDaemon()
    expect(queue.startDaemon()).toBe(false)
  })

  it("stops the daemon", () => {
    queue.startDaemon()
    expect(queue.isDaemonRunning()).toBe(true)
    const result = queue.stopDaemon()
    expect(result).toBe(true)
    expect(queue.isDaemonRunning()).toBe(false)
  })

  it("returns false when already stopped", () => {
    expect(queue.stopDaemon()).toBe(false)
  })
})

describe("Daemon tick (synchronous)", () => {
  let queue: QueueManager
  let tempDir: string
  let oldHome: string | undefined
  let executed: QueueEntry[]

  beforeEach(() => {
    tempDir = createTempDir()
    oldHome = process.env.HOME
    setupTempQueue(tempDir)
    queue = new QueueManager()
    executed = []
    queue.setExecuteFn((entry: QueueEntry) => {
      executed.push(entry)
      entry.status = "done"
      queue["storage"].save(entry)
      queue["events"].emit({ event: "task_done", id: entry.id, status: "done" })
    })
  })

  afterEach(() => {
    queue.stopDaemon()
    cleanupTempQueue(tempDir)
    process.env.HOME = oldHome
  })

  it("executes the highest priority entry on tick", () => {
    queue.add({ prompt: "low", priority: 1 })
    queue.add({ prompt: "high", priority: 100 })
    queue.startDaemon()
    queue.tick()
    expect(executed.length).toBe(1)
    expect(executed[0].priority).toBe(100)
  })

  it("does nothing when no pending tasks", () => {
    queue.startDaemon()
    queue.tick()
    expect(executed.length).toBe(0)
  })

  it("skips paused tasks on tick", () => {
    const highPriority = queue.add({ prompt: "high", priority: 100 })
    queue.pause(highPriority.id)
    queue.startDaemon()
    queue.tick()
    expect(executed.length).toBe(0)
    // Verify the high priority task is still pending
    const entry = queue.get(highPriority.id)
    expect(entry!.status).toBe("pending")
    expect(entry!.paused).toBe(true)
  })

  it("executes multiple tasks in priority order", () => {
    queue.add({ prompt: "low", priority: 1 })
    queue.add({ prompt: "mid", priority: 10 })
    queue.add({ prompt: "high", priority: 100 })
    queue.startDaemon()
    queue.tick()
    queue.tick()
    queue.tick()
    expect(executed.length).toBe(3)
    expect(executed[0].priority).toBe(100)
    expect(executed[1].priority).toBe(10)
    expect(executed[2].priority).toBe(1)
  })

  it("emits events when executing tasks", () => {
    queue.add({ prompt: "test" })
    queue.startDaemon()
    queue.tick()
    const processedIds = queue["events"].getProcessedIds()
    expect(processedIds.length).toBe(1)
  })
})
