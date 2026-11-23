/**
 * Minimal test harness for the Streams WebSocket client (test-only, not production).
 *
 * The production contract (`docs/superpowers/specs/2026-11-23-streams-workspace-design.md` §6)
 * gives `openLive()` no dependency-injection parameters, so the harness replaces the three
 * ambient dependencies it must use: `WebSocket`, `setTimeout`/`setInterval`/`Date.now`, and
 * `location`/`document`. Everything else (client token + refresh) is driven through the real
 * `shared/api/client` module with a stubbed `fetch`, so token lifecycle is not mocked away.
 */

export interface SentFrame {
  type?: string
  request_id?: string
  [key: string]: unknown
}

type Listener = (event: unknown) => void

/** Real timer captured before FakeClock installs, for awaiting non-faked internals. */
export const realSetTimeout = globalThis.setTimeout

export class FakeWebSocket {
  static CONNECTING = 0
  static OPEN = 1
  static CLOSING = 2
  static CLOSED = 3
  static instances: FakeWebSocket[] = []
  static reset(): void {
    FakeWebSocket.instances = []
  }
  static get last(): FakeWebSocket | undefined {
    return FakeWebSocket.instances.at(-1)
  }

  readonly url: string
  readonly protocols: string[]
  readyState = 0
  binaryType = 'blob'
  bufferedAmount = 0
  closedByClient = false
  closeCode: number | undefined
  rawSent: string[] = []
  sent: SentFrame[] = []
  onopen: Listener | null = null
  onmessage: Listener | null = null
  onclose: Listener | null = null
  onerror: Listener | null = null
  private listeners = new Map<string, Set<Listener>>()

  constructor(url: string | URL, protocols?: string | string[]) {
    this.url = String(url)
    this.protocols = protocols === undefined ? [] : Array.isArray(protocols) ? protocols : [protocols]
    FakeWebSocket.instances.push(this)
  }

  addEventListener(type: string, listener: Listener): void {
    const set = this.listeners.get(type) ?? new Set<Listener>()
    set.add(listener)
    this.listeners.set(type, set)
  }

  removeEventListener(type: string, listener: Listener): void {
    this.listeners.get(type)?.delete(listener)
  }

  send(data: string): void {
    if (this.readyState !== 1) throw new Error('send() before open')
    this.rawSent.push(data)
    this.sent.push(JSON.parse(data) as SentFrame)
  }

  close(code?: number, reason?: string): void {
    if (this.readyState === 3) return
    this.closedByClient = true
    this.readyState = 3
    this.closeCode = code
    this.emit('close', { code: code ?? 1000, reason: reason ?? '', wasClean: true })
  }

  /** Server-side transitions, for driving the client. */
  serverOpen(): void {
    this.readyState = 1
    this.emit('open', {})
  }

  serverSend(frame: unknown): void {
    this.emit('message', { data: typeof frame === 'string' ? frame : JSON.stringify(frame) })
  }

  serverClose(code = 1006, reason = ''): void {
    if (this.readyState === 3) return
    this.readyState = 3
    this.closeCode = code
    this.emit('close', { code, reason, wasClean: code === 1000 })
  }

  framesOfType(type: string): SentFrame[] {
    return this.sent.filter(frame => frame.type === type)
  }

  private emit(type: string, event: unknown): void {
    const handler = (this as unknown as Record<string, Listener | null>)[`on${type}`]
    handler?.(event)
    for (const listener of this.listeners.get(type) ?? []) listener(event)
  }
}

interface Task {
  id: number
  at: number
  run: () => void
  interval?: number
}

/** Virtual clock: deterministic reauth/backoff assertions without real waiting. */
export class FakeClock {
  now = 0
  private sequence = 1
  private tasks = new Map<number, Task>()
  private restore: (() => void) | null = null

  install(): void {
    const globals = globalThis as unknown as Record<string, unknown>
    const original = {
      setTimeout: globals.setTimeout,
      clearTimeout: globals.clearTimeout,
      setInterval: globals.setInterval,
      clearInterval: globals.clearInterval,
      dateNow: Date.now,
    }
    globals.setTimeout = ((handler: TimerHandler, ms?: number, ...args: unknown[]) => {
      if (typeof handler !== 'function') throw new Error('FakeClock expects a function timer')
      const id = this.sequence++
      this.tasks.set(id, { id, at: this.now + Math.max(0, Number(ms) || 0), run: () => handler(...args) })
      return id
    }) as typeof setTimeout
    globals.clearTimeout = ((id: number) => { this.tasks.delete(Number(id)) }) as typeof clearTimeout
    globals.setInterval = ((handler: TimerHandler, ms?: number, ...args: unknown[]) => {
      if (typeof handler !== 'function') throw new Error('FakeClock expects a function timer')
      const id = this.sequence++
      const period = Math.max(1, Number(ms) || 1)
      this.tasks.set(id, { id, at: this.now + period, interval: period, run: () => handler(...args) })
      return id
    }) as typeof setInterval
    globals.clearInterval = ((id: number) => { this.tasks.delete(Number(id)) }) as typeof clearInterval
    Date.now = () => this.now
    this.restore = () => {
      globals.setTimeout = original.setTimeout
      globals.clearTimeout = original.clearTimeout
      globals.setInterval = original.setInterval
      globals.clearInterval = original.clearInterval
      Date.now = original.dateNow
    }
  }

  uninstall(): void {
    this.restore?.()
    this.restore = null
    this.tasks.clear()
  }

  /** Pending timers as [delayFromNow, interval?] pairs. */
  pending(): Array<{ delay: number; interval?: number }> {
    return [...this.tasks.values()].map(task => ({ delay: task.at - this.now, interval: task.interval }))
  }

  advance(ms: number): void {
    const target = this.now + ms
    for (;;) {
      const due = [...this.tasks.values()]
        .filter(task => task.at <= target)
        .sort((a, b) => a.at - b.at || a.id - b.id)[0]
      if (!due) break
      this.now = due.at
      if (due.interval) due.at = this.now + due.interval
      else this.tasks.delete(due.id)
      due.run()
    }
    this.now = target
  }
}

/**
 * The only globals this harness writes. Keep the list exhaustive and literal: the
 * cleanup restores exactly these own properties, so adding a guessed browser global
 * here (window, navigator, ...) would clobber ambient state the harness never owned.
 * `fetch` and the fake timers are installed/restored separately (`installFetchStub`,
 * `FakeClock`) because they are independent of the browser-global lifecycle.
 */
const browserGlobalNames = ['WebSocket', 'location', 'document'] as const

type BrowserGlobalName = (typeof browserGlobalNames)[number]

/**
 * Own-property descriptors captured before the first install of this module.
 *
 * The live-socket suites call `installBrowserGlobals()` in every `beforeEach`, so a
 * per-call capture would record the *previous fake* as the original and the cleanup
 * would restore the leak instead of removing it. The snapshot is therefore taken once
 * and reused until the cleanup runs; after that the next install captures afresh.
 */
let originalDescriptors: Map<BrowserGlobalName, PropertyDescriptor | undefined> | null = null

/**
 * Install WebSocket + browser-ish globals the client derives its URL/auth from.
 *
 * Returns an idempotent cleanup that restores every touched global's own property
 * descriptor exactly as it was before the first install. Globals that did not exist
 * before are deleted again, so a later test file that imports Vue (runtime-dom probes
 * `document` at module scope and calls `doc.createElement`) sees the real environment
 * rather than a partial fake.
 */
export function installBrowserGlobals(): () => void {
  const globals = globalThis as unknown as Record<string, unknown>
  if (!originalDescriptors) {
    originalDescriptors = new Map(
      browserGlobalNames.map(name => [name, Object.getOwnPropertyDescriptor(globalThis, name)] as const),
    )
  }
  const snapshot = originalDescriptors
  globals.WebSocket = FakeWebSocket
  globals.location = {
    protocol: 'https:',
    host: 'console.example',
    hostname: 'console.example',
    port: '',
    origin: 'https://console.example',
    href: 'https://console.example/traffic',
    pathname: '/traffic',
    search: '',
    hash: '',
  }
  globals.document = {
    cookie: 'hydra_csrf=test-csrf',
    hidden: false,
    visibilityState: 'visible',
    addEventListener() {},
    removeEventListener() {},
  }

  return () => {
    // Idempotent and stale-safe: only the install that still owns the snapshot restores,
    // so calling the returned cleanup twice (or after a later install) is a no-op.
    if (originalDescriptors !== snapshot) return
    originalDescriptors = null
    for (const [name, descriptor] of snapshot) {
      if (descriptor) Object.defineProperty(globalThis, name, descriptor)
      else Reflect.deleteProperty(globalThis, name)
    }
  }
}

export interface FetchCall {
  url: string
  method: string
}

/** Stubbed fetch that issues rotating access tokens for the real client module. */
export function installFetchStub(calls: FetchCall[]): void {
  let issued = 0
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    const url = String(input)
    calls.push({ url, method: init?.method ?? 'GET' })
    if (url.endsWith('/auth/login') || url.endsWith('/auth/refresh')) {
      return Response.json({ access_token: `token-${++issued}`, expires_in: 60 })
    }
    throw new Error(`Unexpected fetch in live-socket test: ${url}`)
  }) as typeof fetch
}
