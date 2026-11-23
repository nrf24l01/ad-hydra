/**
 * Frontend unit tests for the Streams WebSocket client.
 *
 * Contract under test (design §3, §6):
 *   openLive({query, onFrame, onState}) -> {subscribe, select, page, messages, search, resync,
 *   pause, resume, close}
 *   - one socket, endpoint /api/v1/traffic/events/socket, subprotocol hydra.traffic.v1
 *   - access token only in the first `auth` frame, never in the URL/query
 *   - `ready` (not `open`) is what makes the page live
 *   - request_id correlation; generation monotonicity (older frames discarded)
 *   - re-auth within 50 s using the existing refresh/accessToken lifecycle
 *   - loss -> resync; abnormal close -> bounded backoff reconnect with re-subscribe + re-select
 *   - pause/resume, unmount close + timer cleanup
 *
 * Assumptions (documented in scratch/hydra-ws-frontend-tests.md):
 *   - openLive reads `accessToken()` / `refresh()` from shared/api/client (real module here).
 *   - ambient WebSocket/location/document/timers are the only injected dependencies.
 */
import { afterAll, beforeEach, expect, test } from 'bun:test'
import { clear, login } from '../src/shared/api/client'
import { openLive } from '../src/features/traffic/liveSocket'
import {
  FakeClock,
  FakeWebSocket,
  installBrowserGlobals,
  installFetchStub,
  realSetTimeout,
  type FetchCall,
  type SentFrame,
} from './helpers/live-socket-harness'

const query = { service_id: null, favorite: null, include_blocked: false, newest_first: true, limit: 100 }
const ready = { type: 'ready', protocol: 1, connection_id: 'conn-1', actor: 'analyst', expires_in_ms: 41200, server_time_ms: 0 }
const flowEvent = { kind: 'flow_opened', flow_id: 'flow-1', service_id: 'svc-1' }

let clock: FakeClock
const fetchCalls: FetchCall[] = []
const originalFetch = globalThis.fetch
let liveHandle: { close(): void } | undefined

function socket(): FakeWebSocket {
  const current = FakeWebSocket.last
  if (!current) throw new Error('no socket was opened')
  return current
}

/** Drain microtasks so the client's async token refresh settles under the virtual clock. */
async function settle(): Promise<void> {
  for (let i = 0; i < 5; i++) await Promise.resolve()
  await new Promise<void>(resolve => realSetTimeout(resolve, 0))
  for (let i = 0; i < 5; i++) await Promise.resolve()
}

async function connect() {
  const frames: SentFrame[] = []
  const states: string[] = []
  const live = openLive({
    query,
    onFrame: (frame: unknown) => frames.push(frame as SentFrame),
    onState: (state: unknown) => states.push(String(state)),
  })
  liveHandle = live
  return { live, frames, states }
}

beforeEach(async () => {
  try { liveHandle?.close() } catch { /* socket may already be closed */ }
  clock?.uninstall()
  FakeWebSocket.reset()
  fetchCalls.length = 0
  clock = new FakeClock()
  clock.install()
  installBrowserGlobals()
  installFetchStub(fetchCalls)
  clear()
  await login('analyst', 'secret')
  liveHandle = undefined
})

afterAll(() => {
  clock.uninstall()
  globalThis.fetch = originalFetch
  clear()
})

test('opens one socket at the documented endpoint with the traffic subprotocol and no token in the URL', async () => {
  const { states } = await connect()
  expect(FakeWebSocket.instances).toHaveLength(1)
  const ws = socket()
  expect(ws.protocols).toContain('hydra.traffic.v1')
  const url = new URL(ws.url)
  expect(url.pathname).toBe('/api/v1/traffic/events/socket')
  expect(url.search).toBe('')
  expect(ws.url).not.toContain('token')
  expect(ws.url).not.toContain('access_token')
  expect(states).toEqual(['connecting'])
  expect(states).not.toContain('live')
})

test('sends auth as the first frame and only reports live after the ready frame', async () => {
  const { live, states } = await connect()
  const ws = socket()
  ws.serverOpen()
  expect(ws.sent).toHaveLength(1)
  expect(ws.sent[0]).toMatchObject({ type: 'auth', token: 'token-1' })
  expect(typeof ws.sent[0]?.request_id).toBe('string')
  expect(states).not.toContain('live')

  ws.serverSend(ready)
  expect(states).toContain('live')
  const subscribe = ws.framesOfType('subscribe').at(-1)
  expect(subscribe).toMatchObject({ query })
  expect(subscribe?.request_id).toBeString()

  live.close()
})

test('correlates responses by request_id and discards frames from an older generation', async () => {
  const { live, frames } = await connect()
  const ws = socket()
  ws.serverOpen()
  ws.serverSend(ready)

  live.page(12345)
  const page = ws.framesOfType('page').at(-1)
  expect(page).toMatchObject({ after: 12345 })
  expect(page?.request_id).toBeString()

  // A response carrying a different request_id belongs to another in-flight request: discard it.
  ws.serverSend({ type: 'flows_page', request_id: 'someone-else', generation: 1, items: [], next_cursor: null })
  expect(frames.filter(frame => frame.type === 'flows_page')).toHaveLength(0)
  ws.serverSend({ type: 'flows_page', request_id: page?.request_id, generation: 1, items: [], next_cursor: null })
  expect(frames.filter(frame => frame.type === 'flows_page')).toHaveLength(1)

  const subscribe = ws.framesOfType('subscribe').at(-1)
  ws.serverSend({ type: 'snapshot', request_id: subscribe?.request_id, generation: 2, items: [], next_cursor: null, query })
  ws.serverSend({ type: 'flow', generation: 1, event: flowEvent })
  expect(frames.filter(frame => frame.type === 'flow')).toHaveLength(0)
  ws.serverSend({ type: 'flow', generation: 2, event: flowEvent })
  expect(frames.filter(frame => frame.type === 'flow')).toHaveLength(1)

  live.close()
})

test('re-auths within 50 seconds and refreshes the token once it is no longer fresh', async () => {
  const { live } = await connect()
  const ws = socket()
  ws.serverOpen()
  ws.serverSend(ready)
  expect(ws.framesOfType('auth')).toHaveLength(1)

  const scheduled = clock.pending().filter(timer => !timer.interval).map(timer => timer.delay)
  expect(scheduled.length).toBeGreaterThan(0)
  expect(Math.min(...scheduled)).toBeLessThanOrEqual(50_000)

  // First re-auth reuses the still-fresh access token (60 s lifetime), no refresh round trip.
  clock.advance(45_000)
  await settle()
  const reauth = ws.framesOfType('auth')
  expect(reauth.length).toBeGreaterThanOrEqual(2)
  expect(reauth.at(-1)?.token).toBe('token-1')
  expect(reauth.at(-1)?.request_id).not.toBe(reauth[0]?.request_id)
  expect(fetchCalls.some(call => call.url.endsWith('/auth/refresh'))).toBe(false)

  // Next re-auth falls past the token lifetime, so the existing refresh() lifecycle must run.
  clock.advance(45_000)
  await settle()
  expect(fetchCalls.some(call => call.url.endsWith('/auth/refresh'))).toBe(true)
  expect(ws.framesOfType('auth').at(-1)?.token).toBe('token-2')

  live.close()
})

test('turns a loss frame into a resync and accepts the fresh snapshot generation', async () => {
  const { live, frames } = await connect()
  const ws = socket()
  ws.serverOpen()
  ws.serverSend(ready)
  ws.serverSend({ type: 'snapshot', generation: 1, items: [], next_cursor: null, query })

  ws.serverSend({ type: 'loss', generation: 1, dropped: 7, reason: 'broadcast_lag' })
  const resync = ws.framesOfType('resync').at(-1)
  expect(resync).toBeDefined()
  expect(resync?.request_id).toBeString()
  expect(typeof resync?.reason).toBe('string')

  ws.serverSend({ type: 'snapshot', request_id: resync?.request_id, generation: 2, items: [], next_cursor: null, query })
  expect(frames.at(-1)).toMatchObject({ type: 'snapshot', generation: 2 })

  live.close()
})

test('reconnects with bounded backoff and re-subscribes the current query and selection', async () => {
  const { live, states } = await connect()
  const first = socket()
  first.serverOpen()
  first.serverSend(ready)
  live.select('flow-1', true)
  expect(first.framesOfType('select').at(-1)).toMatchObject({ flow_id: 'flow-1' })
  live.subscribe({ ...query, limit: 50 })

  first.serverClose(1013, 'slow consumer')
  expect(states).toContain('retrying')
  const expected = [1000, 2000, 4000, 5000, 5000]
  for (const delay of expected) {
    const timers = clock.pending().filter(timer => !timer.interval).map(timer => timer.delay)
    expect(timers.length).toBeGreaterThan(0)
    expect(Math.min(...timers)).toBe(delay)
    expect(delay).toBeLessThanOrEqual(5000)
    clock.advance(delay)
    socket().serverClose(1013, 'slow consumer')
  }

  clock.advance(5000)
  const second = socket()
  expect(second).not.toBe(first)
  expect(second.protocols).toContain('hydra.traffic.v1')
  expect(second.url).not.toContain('token')
  second.serverOpen()
  expect(second.sent[0]?.type).toBe('auth')
  second.serverSend(ready)
  expect(second.framesOfType('subscribe').at(-1)?.query).toMatchObject({ limit: 50 })
  expect(second.framesOfType('select').at(-1)).toMatchObject({ flow_id: 'flow-1' })

  live.close()
})

test('pause suppresses automatic resync and resume asks for a fresh snapshot', async () => {
  const { live, frames } = await connect()
  const ws = socket()
  ws.serverOpen()
  ws.serverSend(ready)
  ws.serverSend({ type: 'snapshot', generation: 1, items: [], next_cursor: null, query })

  live.pause()
  expect(ws.framesOfType('pause')).toHaveLength(1)
  // Server pushes are stopped while paused; a queued loss must not force a resync behind the UI's back.
  ws.serverSend({ type: 'loss', generation: 1, dropped: 3, reason: 'broadcast_lag' })
  expect(ws.framesOfType('resync')).toHaveLength(0)

  live.resume()
  const resume = ws.framesOfType('resume').at(-1)
  expect(resume?.request_id).toBeString()
  ws.serverSend({ type: 'snapshot', request_id: resume?.request_id, generation: 2, items: [], next_cursor: null, query })
  expect(frames.at(-1)).toMatchObject({ type: 'snapshot', generation: 2 })

  live.close()
})

test('close() closes the socket, cancels timers and stops reconnecting', async () => {
  const { live, frames } = await connect()
  const ws = socket()
  ws.serverOpen()
  ws.serverSend(ready)

  live.close()
  expect(ws.closedByClient).toBe(true)
  expect(clock.pending()).toEqual([])

  const delivered = frames.length
  const authCount = ws.framesOfType('auth').length
  clock.advance(120_000)
  expect(FakeWebSocket.instances).toHaveLength(1)
  expect(frames.length).toBe(delivered)
  expect(ws.framesOfType('auth')).toHaveLength(authCount)
})
