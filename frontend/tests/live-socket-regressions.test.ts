/**
 * Focused regression coverage for the Streams WebSocket client — the gaps left by
 * `live-socket.test.ts` and `hydra-ws-frontend-run1.md`. Same harness, same real
 * `shared/api/client` token lifecycle, only the ambient browser globals are faked.
 *
 * Expected red before the matching production fixes:
 *   1. reconnect while paused -> resume must replay `subscribe` AND the selection
 *      AND send the real `resume` frame (replaying the two frames alone leaves
 *      Core's session paused, so no live events ever arrive).
 *   2. a failed health probe must retry, not leave the socket labelled `retrying` forever.
 *   3. every advanced query field must reach the `subscribe` frame unchanged.
 *   4. a `selected` frame for an out-of-page (closed UDP) flow must be forwarded with
 *      its payload, correlated to the outstanding `select` request and generation.
 *
 * Client frames are asserted from `FakeWebSocket.rawSent` (the exact JSON text
 * `openLive` wrote) rather than from harness bookkeeping, so a frame that never
 * reached the socket cannot satisfy a test.
 *
 * Not covered: "a generic `error` frame must clear the selection loading state"
 * (`TrafficPage.vue:111-115`). There is no component harness in `frontend/` (no
 * `@vue/test-utils`, no jsdom/happy-dom), so mounting `TrafficPage.vue` cannot be
 * proven here. This is an explicit coverage gap — it is NOT tested, not even as a
 * skip counted towards coverage.
 */
import { afterAll, beforeEach, expect, test } from 'bun:test'
import { clear, login } from '../src/shared/api/client'
import { openLive, type LiveQuery } from '../src/features/traffic/liveSocket'
import {
  FakeClock,
  FakeWebSocket,
  installBrowserGlobals,
  installFetchStub,
  realSetTimeout,
  type FetchCall,
  type SentFrame,
} from './helpers/live-socket-harness'

const query: LiveQuery = { service_id: null, favorite: null, include_blocked: false, newest_first: true, limit: 100 }
const ready = { type: 'ready', protocol: 1, connection_id: 'conn-1', actor: 'analyst', expires_in_ms: 41200, server_time_ms: 0 }

let clock: FakeClock
const fetchCalls: FetchCall[] = []
const originalFetch = globalThis.fetch
let liveHandle: { close(): void } | undefined
let restoreBrowserGlobals: (() => void) | undefined

function socket(): FakeWebSocket {
  const current = FakeWebSocket.last
  if (!current) throw new Error('no socket was opened')
  return current
}

/** Frames `openLive` actually serialized onto the socket, in wire order. */
function wire(fake: FakeWebSocket): SentFrame[] {
  return fake.rawSent.map(text => JSON.parse(text) as SentFrame)
}

async function settle(): Promise<void> {
  for (let i = 0; i < 5; i++) await Promise.resolve()
  await new Promise<void>(resolve => realSetTimeout(resolve, 0))
  for (let i = 0; i < 5; i++) await Promise.resolve()
}

async function connect(overrides: { query?: LiveQuery; health?: () => Promise<boolean> } = {}) {
  const frames: SentFrame[] = []
  const states: string[] = []
  const live = openLive({
    query: overrides.query ?? query,
    health: overrides.health,
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
  restoreBrowserGlobals = installBrowserGlobals()
  installFetchStub(fetchCalls)
  clear()
  await login('analyst', 'secret')
  liveHandle = undefined
})

afterAll(() => {
  // Restore the browser globals first and unconditionally: a leaked partial `document`
  // breaks every later test file that imports Vue, so the fake clock/fetch restores must
  // still run even if the browser-global cleanup were to throw.
  try {
    restoreBrowserGlobals?.()
  } finally {
    clock.uninstall()
    globalThis.fetch = originalFetch
    clear()
  }
})

test('resuming after a reconnect that happened while paused replays the query and selection, then sends the real resume frame', async () => {
  const { live, frames } = await connect()
  const first = socket()
  first.serverOpen()
  first.serverSend(ready)
  live.select('flow-1', true)
  live.pause()
  expect(wire(first).filter(frame => frame.type === 'pause')).toHaveLength(1)

  // Drop the socket while the page is paused (TrafficPage pauses on `document.hidden`
  // and resumes on visibilitychange); the client reconnects on its own.
  first.serverClose(1013, 'slow consumer')
  clock.advance(1000)
  const second = socket()
  expect(second).not.toBe(first)
  second.serverOpen()
  second.serverSend(ready)

  // A paused socket must not fetch a snapshot on `ready`, must not replay the
  // selection, and must not send `resume` behind the page's back: the fresh
  // connection is only re-paused. This is the "hidden, still paused" case — no
  // false positive that the page is live again.
  const pausedTypes = wire(second).map(frame => frame.type)
  expect(pausedTypes).toContain('pause')
  expect(pausedTypes).not.toContain('subscribe')
  expect(pausedTypes).not.toContain('select')
  expect(pausedTypes).not.toContain('resume')
  expect(frames.filter(frame => frame.type === 'snapshot')).toHaveLength(0)

  // ...but resuming on this fresh connection has to re-establish the query and
  // selection AND unpause the server session, otherwise Core has no subscription
  // (answers `error not_subscribed`) or keeps dropping live events as paused.
  live.resume()

  const resumed = wire(second)
  const resumeFrames = resumed.filter(frame => frame.type === 'resume')
  // Exactly one real resume frame: the replay must not emit one per replayed frame.
  expect(resumeFrames).toHaveLength(1)
  expect(resumeFrames[0]?.request_id).toBeString()

  // Order on the wire: subscribe, then select, then resume. Core's `resume`
  // handler needs `session.query` (set by `subscribe`) or it answers
  // `not_subscribed`; `subscribe` itself never clears `session.paused`.
  const subscribeAt = resumed.findIndex(frame => frame.type === 'subscribe')
  const selectAt = resumed.findIndex(frame => frame.type === 'select')
  const resumeAt = resumed.findIndex(frame => frame.type === 'resume')
  expect(subscribeAt).toBeGreaterThanOrEqual(0)
  expect(selectAt).toBeGreaterThan(subscribeAt)
  expect(resumeAt).toBeGreaterThan(selectAt)
  expect(resumed.filter(frame => frame.type === 'subscribe')).toHaveLength(1)

  const subscribe = resumed[subscribeAt]
  expect(subscribe?.query).toEqual(query)
  expect(subscribe?.request_id).toBeString()
  expect(resumed[selectAt]).toMatchObject({ flow_id: 'flow-1', active_bytes: true })

  // The replayed subscribe snapshot is obsolete: `resume()` repoints `latest.snapshot`
  // at the resume frame's request_id, so a snapshot answered for the subscribe id is
  // ignored rather than applied.
  const resumeId = resumeFrames[0]?.request_id
  expect(resumeId).toBeString()
  second.serverSend({ type: 'snapshot', request_id: subscribe?.request_id, generation: 2, items: [], next_cursor: null, query })
  expect(frames.filter(frame => frame.type === 'snapshot')).toHaveLength(0)

  // Only the resume request_id, at the resume generation, is applied — exactly once.
  second.serverSend({ type: 'snapshot', request_id: resumeId, generation: 3, items: [], next_cursor: null, query })
  const applied = frames.filter(frame => frame.type === 'snapshot')
  expect(applied).toHaveLength(1)
  expect(applied[0]?.request_id).toBe(resumeId)
  expect(wire(second).filter(frame => frame.type === 'subscribe')).toHaveLength(1)

  live.close()
})

test('a failed health probe retries and recovers instead of staying stuck on retrying', async () => {
  let probes = 0
  const health = async (): Promise<boolean> => {
    probes++
    return probes > 1
  }
  const { live, states } = await connect({ health })
  const ws = socket()
  ws.serverOpen()
  ws.serverSend(ready)
  await settle()

  expect(states.at(-1)).toBe('retrying')
  expect(probes).toBe(1)
  expect(ws.closedByClient).toBe(false)

  // Recovery must be probed again on the still-open socket; the UI may not be
  // left saying "Reconnecting live traffic…" while frames keep flowing.
  for (let elapsed = 0; elapsed < 10_000 && !states.includes('live'); elapsed += 500) {
    clock.advance(500)
    await settle()
  }
  expect(probes).toBeGreaterThanOrEqual(2)
  expect(states.at(-1)).toBe('live')
  expect(ws.closedByClient).toBe(false)

  live.close()
})

test('advanced query fields reach the subscribe frame exactly as the page built them', async () => {
  // Mirrors TrafficQuery.vue: every advanced filter the user can set must survive
  // the Streams `subscribe` frame (Core rejects unknown fields, so an absent field
  // is a silently dropped filter).
  const advanced = {
    service_id: 'svc-1',
    favorite: true,
    include_blocked: true,
    newest_first: false,
    limit: 50,
    client_ip: '10.0.0.7',
    transport: 2,
    direction: 1,
    since_ms: 1_700_000_000_000,
    until_ms: 1_700_000_001_000,
    field_path: 'http.status_code',
    field_value: '500',
    matched_rule: 'rule-1',
    has_flag: true,
  } as unknown as LiveQuery
  const { live } = await connect({ query: advanced })
  const ws = socket()
  ws.serverOpen()
  ws.serverSend(ready)

  const subscribe = ws.framesOfType('subscribe').at(-1)
  expect(subscribe).toBeDefined()
  expect(subscribe?.query).toEqual(advanced)

  live.close()
})

test('a selected frame for a closed out-of-page UDP flow is forwarded with request correlation and generation', async () => {
  const { live, frames } = await connect()
  const ws = socket()
  ws.serverOpen()
  ws.serverSend(ready)
  ws.serverSend({ type: 'snapshot', request_id: ws.framesOfType('subscribe').at(-1)?.request_id, generation: 1, items: [], next_cursor: null, query })

  live.select('flow-udp', false)
  const select = ws.framesOfType('select').at(-1)
  expect(select?.request_id).toBeString()

  const flow = {
    flow_id: 'flow-udp',
    service_id: 'svc-1',
    transport: 2,
    closed: true,
    client: { ip: '10.0.0.7', port: 40000 },
    upstream: { ip: '10.0.0.9', port: 53 },
  }
  ws.serverSend({ type: 'selected', request_id: select?.request_id, generation: 2, flow })
  const selected = frames.filter(frame => frame.type === 'selected')
  expect(selected).toHaveLength(1)
  expect(selected[0]).toMatchObject({ request_id: select?.request_id, generation: 2 })
  expect(selected[0]?.flow).toMatchObject({
    flow_id: 'flow-udp',
    transport: 2,
    closed: true,
    client: { ip: '10.0.0.7', port: 40000 },
    upstream: { ip: '10.0.0.9', port: 53 },
  })

  // A stale generation must not overwrite the newer metadata.
  ws.serverSend({ type: 'selected', request_id: select?.request_id, generation: 1, flow: { ...flow, transport: 1 } })
  expect(frames.filter(frame => frame.type === 'selected')).toHaveLength(1)

  // A frame answering a different in-flight request belongs elsewhere: discard it.
  ws.serverSend({ type: 'selected', request_id: 'someone-else', generation: 3, flow: { flow_id: 'flow-other' } })
  expect(frames.filter(frame => frame.type === 'selected')).toHaveLength(1)

  // The matching request id is still forwarded.
  ws.serverSend({ type: 'selected', request_id: select?.request_id, generation: 3, flow })
  expect(frames.filter(frame => frame.type === 'selected')).toHaveLength(2)

  live.close()
})

// COVERAGE GAP — deliberately not executed. `TrafficPage.vue:111-115` maps only
// history_pending/flow_not_found/active_bytes_unavailable/history_unavailable, so a
// generic `query_failed` error leaves `wsSelectionStatus === 'loading'`. Proving that
// needs a real DOM component harness; this repo has neither `@vue/test-utils` nor
// jsdom/happy-dom, and a hand-rolled mount would not be the real SFC lifecycle.
// Kept visible so no report claims this behaviour is covered.
test.skip('generic error frames clear the conversation loading state (needs a component harness)', () => {})
