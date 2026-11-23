import type { HistorySegment } from '@/generated/contracts'
import { accessToken, accessTokenFresh, refresh } from '@/shared/api/client'
import { apiBaseUrl } from '@/shared/config/env'
import type { FlagSpan } from './highlight'
import type { Flow, Message } from './traffic'

export type LiveState = 'connecting' | 'live' | 'retrying'

export interface LiveQuery {
  service_id?: string | null
  favorite?: boolean | null
  include_blocked?: boolean
  newest_first?: boolean
  limit?: number
  /** Present for parity with the Core query type; subscribe does not page. */
  after?: number | null
  client_ip?: string | null
  since_ms?: number | null
  until_ms?: number | null
  transport?: number | null
  source_ip?: string | null
  destination_ip?: string | null
  direction?: number | null
  field_path?: string | null
  field_value?: string | null
  matched_rule?: string | null
  has_flag?: boolean | null
}

export type SearchScope = 'messages' | 'flows' | 'both'

export interface SearchQuery {
  q: string
  scope?: SearchScope
  service_id?: string | null
  since_ms?: number | null
  until_ms?: number | null
  has_flag?: boolean | null
  after?: string | null
  limit?: number
}

export interface SearchItem {
  kind: string
  flow_id: string
  message_id?: string
  service_id: string
  occurred_ms: number
  direction?: number
  protocol_id?: string
  snippet: string
  snippet_truncated?: boolean
  spans?: FlagSpan[]
  flag_spans?: FlagSpan[]
  has_flag?: boolean
  display_text?: string
}

export interface SearchCoverage {
  scanned: number
  truncated: boolean
  continuation: boolean
  undecodable: number
  decoded_only: boolean
  raw_history_searched: boolean
  flag_pattern?: string
}

export interface SearchPage {
  items: SearchItem[]
  next_cursor: string | null
  coverage: SearchCoverage
  took_ms?: number
  request_id?: string
}

export interface ActiveHistorySnapshot {
  flow_id: string
  service_id?: string
  protocol_id?: string
  opened_at_ms?: number
  active: boolean
  pending?: boolean
  segments: HistorySegment[]
  truncated_client_to_server?: boolean
  truncated_server_to_client?: boolean
  retained_client_to_server_bytes?: number
  retained_server_to_client_bytes?: number
  reset?: boolean
  client_sequence?: number
  server_sequence?: number
  client_head_dropped?: number
  server_head_dropped?: number
  generated_at_ms?: number
}

export interface WorkspaceHistory {
  flow_id: string
  active: boolean
  pending: boolean
  segments: HistorySegment[]
  reset: boolean
  truncated_client_to_server: boolean
  truncated_server_to_client: boolean
  retained_client_to_server_bytes: number
  retained_server_to_client_bytes: number
  client_head_dropped: number
  server_head_dropped: number
  gap_bytes: number
  generated_at_ms: number
}

/** Data injected by TrafficPage into the shared conversation view. */
export interface WorkspaceStreamSource {
  flow?: Flow
  messages: Message[]
  history?: WorkspaceHistory
  status: 'idle' | 'loading' | 'ready' | 'active' | 'missing' | 'unavailable'
  /** Set only for a failed retained-capture read; the flow and its decoded messages stay valid. */
  historyNotice?: string
  messageCursor?: number | null
  onMore?: () => void
  onRefresh?: () => void
}

export interface ReadyFrame { type: 'ready'; protocol?: number; connection_id?: string; actor?: string; expires_in_ms?: number; server_time_ms?: number }
export interface SnapshotFrame { type: 'snapshot'; request_id?: string; generation?: number; items: Array<Record<string, unknown>>; next_cursor?: number | null; query?: LiveQuery }
export interface FlowsPageFrame { type: 'flows_page'; request_id?: string; generation?: number; items: Array<Record<string, unknown>>; next_cursor?: number | null }
export interface MessagesPageFrame { type: 'messages_page'; request_id?: string; generation?: number; flow_id?: string; items: Array<Record<string, unknown>>; next_cursor?: number | null }
export interface FlowFrame { type: 'flow'; request_id?: string; generation?: number; event: Record<string, unknown>; flow?: Record<string, unknown> }
export interface MessageFrame { type: 'message'; request_id?: string; generation?: number; flow_id?: string; event: Record<string, unknown>; display_text?: string; flag_spans?: FlagSpan[] }
export interface CountersFrame { type: 'counters'; generation?: number; flows_open?: number; flows_seen?: number; messages_seen?: number; bytes_c2s?: number; bytes_s2c?: number; sockets?: number; at_ms?: number }
export interface SearchPageFrame extends SearchPage { type: 'search_page'; generation?: number }
export interface ActiveBytesFrame { type: 'active_bytes'; request_id?: string; generation?: number; flow_id?: string; snapshot: ActiveHistorySnapshot }
export interface LossFrame { type: 'loss'; generation?: number; dropped?: number; reason?: string }
export interface ErrorFrame { type: 'error'; request_id?: string; code?: string; message?: string; retryable?: boolean }
export interface ReauthRequiredFrame { type: 'reauth_required'; reason?: string }
/** Core's selection acknowledgement: actual flow metadata for the requested flow. */
export interface SelectedFrame { type: 'selected'; request_id?: string; generation?: number; flow_id?: string; flow?: Record<string, unknown> }
export type ServerFrame = ReadyFrame | SnapshotFrame | FlowsPageFrame | MessagesPageFrame | FlowFrame | MessageFrame | CountersFrame | SearchPageFrame | ActiveBytesFrame | LossFrame | ErrorFrame | ReauthRequiredFrame | SelectedFrame

export interface OpenLiveOptions {
  query: LiveQuery
  onFrame: (frame: ServerFrame) => void
  onState: (state: LiveState) => void
  /** User-safe close reason for the existing live-status line. */
  onNotice?: (text: string) => void
  /** Optional Core reachability probe; live state is withheld until it succeeds. */
  health?: () => Promise<boolean>
}

export interface LiveSocket {
  subscribe(query?: LiveQuery): void
  page(after: number, limit?: number): void
  select(flowId: string | null, active?: boolean): void
  messages(flowId: string, after?: number, limit?: number): void
  search(query: SearchQuery): string
  resync(): void
  pause(): void
  resume(): void
  close(): void
}

const REAUTH_MS = 45_000
const AUTH_DEADLINE_MS = 5_000
const MIN_BACKOFF_MS = 1_000
const MAX_BACKOFF_MS = 5_000
const MAX_FRAME_CHARS = 2 * 1024 * 1024


/** http(s) API base -> ws(s) socket URL; the access token is never part of this URL. */
export function socketUrl(base = apiBaseUrl): string {
  const origin = typeof location === 'undefined' ? 'http://localhost' : location.origin
  const resolved = new URL(base || '/api', origin)
  const protocol = resolved.protocol === 'https:' ? 'wss:' : 'ws:'
  const path = `${resolved.pathname.replace(/\/+$/, '')}/v1/traffic/events/socket`
  return `${protocol}//${resolved.host}${path}`
}

export function backoffDelay(attempt: number): number {
  return Math.min(MAX_BACKOFF_MS, MIN_BACKOFF_MS * 2 ** Math.min(Math.max(attempt, 0), 3))
}

/** User-safe meaning for the protocol's close codes. */
export function closeCodeText(code: number): string {
  switch (code) {
    case 4400: return 'Invalid live frame.'
    case 4401: return 'Live authentication expired or was revoked.'
    case 4403: return 'Live connection origin was rejected.'
    case 4429: return 'Too many live connections for this account.'
    case 1013: return 'Live connection fell behind; reconnecting.'
    default: return 'Live connection closed; reconnecting.'
  }
}

export function newRequestId(): string {
  return typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function' ? crypto.randomUUID() : `r-${Date.now()}-${Math.random().toString(16).slice(2, 10)}`
}

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}
}

function number(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined
}

function boolean(value: unknown): boolean | undefined {
  return typeof value === 'boolean' ? value : undefined
}

/** Parse one server text frame into the typed union, or undefined for malformed input. */
export function parseServerFrame(data: unknown): ServerFrame | undefined {
  if (typeof data !== 'string' || !data || data.length > MAX_FRAME_CHARS) return undefined
  let parsed: unknown
  try { parsed = JSON.parse(data) } catch { return undefined }
  const frame = record(parsed)
  const type = frame.type
  if (typeof type !== 'string') return undefined
  switch (type) {
    case 'ready':
      return { type, protocol: number(frame.protocol), connection_id: typeof frame.connection_id === 'string' ? frame.connection_id : undefined, actor: typeof frame.actor === 'string' ? frame.actor : undefined, expires_in_ms: number(frame.expires_in_ms), server_time_ms: number(frame.server_time_ms) }
    case 'snapshot':
      return { type, request_id: typeof frame.request_id === 'string' ? frame.request_id : undefined, generation: number(frame.generation), items: Array.isArray(frame.items) ? frame.items.map(record) : [], next_cursor: typeof frame.next_cursor === 'number' ? frame.next_cursor : null, query: record(frame.query) as unknown as LiveQuery }
    case 'flows_page':
    case 'messages_page':
      return { type, request_id: typeof frame.request_id === 'string' ? frame.request_id : undefined, generation: number(frame.generation), flow_id: typeof frame.flow_id === 'string' ? frame.flow_id : undefined, items: Array.isArray(frame.items) ? frame.items.map(record) : [], next_cursor: typeof frame.next_cursor === 'number' ? frame.next_cursor : null }
    case 'flow':
      return { type, request_id: typeof frame.request_id === 'string' ? frame.request_id : undefined, generation: number(frame.generation), event: record(frame.event), flow: Object.keys(record(frame.flow)).length ? record(frame.flow) : undefined }
    case 'message':
      return { type, request_id: typeof frame.request_id === 'string' ? frame.request_id : undefined, generation: number(frame.generation), flow_id: typeof frame.flow_id === 'string' ? frame.flow_id : undefined, event: record(frame.event), display_text: typeof frame.display_text === 'string' ? frame.display_text : undefined, flag_spans: Array.isArray(frame.flag_spans) ? frame.flag_spans.filter((item): item is FlagSpan => Array.isArray(item) && item.length === 2 && typeof item[0] === 'number' && typeof item[1] === 'number') : undefined }
    case 'counters':
      return { type, generation: number(frame.generation), flows_open: number(frame.flows_open), flows_seen: number(frame.flows_seen), messages_seen: number(frame.messages_seen), bytes_c2s: number(frame.bytes_c2s), bytes_s2c: number(frame.bytes_s2c), sockets: number(frame.sockets), at_ms: number(frame.at_ms) }
    case 'search_page':
      return { type, request_id: typeof frame.request_id === 'string' ? frame.request_id : undefined, generation: number(frame.generation), items: Array.isArray(frame.items) ? frame.items.map(record) as unknown as SearchItem[] : [], next_cursor: typeof frame.next_cursor === 'string' ? frame.next_cursor : null, coverage: record(frame.coverage) as unknown as SearchCoverage, took_ms: number(frame.took_ms) }
    case 'active_bytes':
      return { type, request_id: typeof frame.request_id === 'string' ? frame.request_id : undefined, generation: number(frame.generation), flow_id: typeof frame.flow_id === 'string' ? frame.flow_id : undefined, snapshot: record(frame.snapshot) as unknown as ActiveHistorySnapshot }
    case 'loss':
      return { type, generation: number(frame.generation), dropped: number(frame.dropped), reason: typeof frame.reason === 'string' ? frame.reason : undefined }
    case 'error':
      return { type, request_id: typeof frame.request_id === 'string' ? frame.request_id : undefined, code: typeof frame.code === 'string' ? frame.code : undefined, message: typeof frame.message === 'string' ? frame.message : undefined, retryable: boolean(frame.retryable) }
    case 'reauth_required':
      return { type, reason: typeof frame.reason === 'string' ? frame.reason : undefined }
    case 'selected':
      return { type, request_id: typeof frame.request_id === 'string' ? frame.request_id : undefined, generation: number(frame.generation), flow_id: typeof frame.flow_id === 'string' ? frame.flow_id : undefined, flow: Object.keys(record(frame.flow)).length ? record(frame.flow) : undefined }
    default:
      return undefined
  }
}

/**
 * One page-scoped socket. Auth is in-band, re-auth runs every 45 s (< 50 s), and the
 * token never appears in the URL, headers or logs. Reconnect backoff is bounded 1–5 s
 * and re-subscribes the current query plus the selected flow.
 */
export function openLive(opts: OpenLiveOptions): LiveSocket {
  let socket: WebSocket | undefined
  let closed = false
  let ready = false
  let paused = false
  let generation = 0
  let selectionGeneration = -1
  let attempts = 0
  let connection = 0
  let query: LiveQuery = { ...opts.query }
  let selected: { id: string; active: boolean } | null = null
  let needsSubscribe = false
  let needsSelect = false
  let needsDeselect = false
  let subscribed = false
  let pendingMessages: { flowId: string; after?: number; limit: number; requestId: string } | null = null
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined
  let reauthTimer: ReturnType<typeof setTimeout> | undefined
  let authDeadline: ReturnType<typeof setTimeout> | undefined
  let healthTimer: ReturnType<typeof setTimeout> | undefined
  let healthAttempts = 0
  const latest = new Map<string, string>()

  function send(frame: Record<string, unknown>): void {
    if (!socket || socket.readyState !== WebSocket.OPEN) return
    socket.send(JSON.stringify(frame))
  }

  function sendAuth(): void {
    if (closed) return
    const token = accessToken()
    if (!token) {
      void refresh().then(() => sendAuth()).catch(() => undefined)
      return
    }
    send({ type: 'auth', request_id: newRequestId(), token })
  }

  function sendSubscribe(): void {
    const request_id = newRequestId()
    latest.set('snapshot', request_id)
    subscribed = true
    needsSubscribe = false
    send({ type: 'subscribe', request_id, query })
  }

  function sendSelectFrame(flowId: string | null, active: boolean): void {
    const request_id = newRequestId()
    latest.set('selected', request_id)
    selectionGeneration = -1
    send({ type: 'select', request_id, flow_id: flowId, active_bytes: active })
  }

  function flushPendingMessages(): void {
    if (!pendingMessages) return
    const pending = pendingMessages
    pendingMessages = null
    send({ type: 'messages', request_id: pending.requestId, flow_id: pending.flowId, after: pending.after ?? null, limit: pending.limit })
  }

  function scheduleReauth(): void {
    clearTimeout(reauthTimer)
    reauthTimer = setTimeout(() => {
      if (closed) return
      if (socket?.readyState === WebSocket.OPEN) {
        if (accessTokenFresh()) sendAuth()
        else void refresh().then(sendAuth).catch(() => undefined)
        scheduleReauth()
      }
    }, REAUTH_MS)
  }

  function clearTimers(): void {
    clearTimeout(reconnectTimer); clearTimeout(reauthTimer); clearTimeout(authDeadline); clearTimeout(healthTimer)
    reconnectTimer = reauthTimer = authDeadline = healthTimer = undefined
  }

  function scheduleReconnect(): void {
    if (closed || reconnectTimer) return
    const delay = backoffDelay(attempts++)
    opts.onState('retrying')
    reconnectTimer = setTimeout(() => { reconnectTimer = undefined; connect() }, delay)
  }

  /** Health is re-probed with bounded backoff instead of leaving "retrying" stuck forever. */
  function scheduleHealthRecheck(): void {
    if (closed || !ready || healthTimer) return
    opts.onState('retrying')
    const delay = backoffDelay(healthAttempts++)
    healthTimer = setTimeout(() => { healthTimer = undefined; checkHealth() }, delay)
  }

  function checkHealth(): void {
    if (closed || !ready) return
    if (!opts.health) { opts.onState('live'); return }
    void opts.health().then(ok => {
      if (closed || !ready) return
      if (ok) { healthAttempts = 0; opts.onState('live') }
      else scheduleHealthRecheck()
    }).catch(() => { if (!closed && ready) scheduleHealthRecheck() })
  }

  function onReady(): void {
    ready = true
    attempts = 0
    healthAttempts = 0
    clearTimeout(authDeadline); authDeadline = undefined
    checkHealth()
    if (paused) {
      // A snapshot must be requested again after resume; the fresh server session has no query.
      needsSubscribe = true
      send({ type: 'pause' })
    }
    else {
      sendSubscribe()
      if (selected) sendSelectFrame(selected.id, selected.active)
      flushPendingMessages()
    }
  }

  function handleFrame(frame: ServerFrame): void {
    if (frame.type === 'ready') { onReady(); opts.onFrame(frame); return }
    const frameGeneration = 'generation' in frame ? frame.generation : undefined
    if (frame.type === 'snapshot') {
      if (frameGeneration !== undefined) {
        if (generation > 0 && frameGeneration <= generation) return
        generation = frameGeneration
      }
    } else if (frameGeneration !== undefined && generation > 0 && frameGeneration < generation) {
      return
    }
    const key = frame.type === 'snapshot' ? 'snapshot' : frame.type === 'flows_page' ? 'flows_page' : frame.type === 'messages_page' ? 'messages_page' : frame.type === 'search_page' ? 'search_page' : frame.type === 'selected' ? 'selected' : undefined
    if (key && 'request_id' in frame && frame.request_id) {
      const expected = latest.get(key)
      if (expected && expected !== frame.request_id) return
    }
    if (frame.type === 'selected' && frameGeneration !== undefined) {
      // Bound to the matching select request id; a stale generation must not overwrite newer metadata.
      if (frameGeneration <= selectionGeneration) return
      selectionGeneration = frameGeneration
    }
    if (frame.type === 'reauth_required') {
      void refresh().then(sendAuth).catch(() => undefined)
    }
    if (frame.type === 'loss' && !paused) {
      const request_id = newRequestId()
      latest.set('snapshot', request_id)
      send({ type: 'resync', request_id, reason: frame.reason ?? 'loss' })
    }
    opts.onFrame(frame)
  }

  function connect(): void {
    if (closed) return
    opts.onState('connecting')
    const start = () => {
      if (closed) return
      generation = 0
      selectionGeneration = -1
      subscribed = false
      let ws: WebSocket
      try { ws = new WebSocket(socketUrl(), ['hydra.traffic.v1']) } catch { scheduleReconnect(); return }
      socket = ws
      const mine = ++connection
      ws.onopen = () => {
        if (closed || mine !== connection) { ws.close(1000); return }
        sendAuth()
        authDeadline = setTimeout(() => { if (!ready && mine === connection) ws.close(4401) }, AUTH_DEADLINE_MS)
        scheduleReauth()
      }
      ws.onmessage = event => {
        if (closed || mine !== connection) return
        const frame = parseServerFrame(event.data)
        if (!frame) return
        handleFrame(frame)
      }
      ws.onerror = () => undefined
      ws.onclose = (event: CloseEvent) => {
        if (mine !== connection) return
        ready = false
        subscribed = false
        socket = undefined
        clearTimers()
        if (!closed) {
          const code = event?.code ?? 1006
          if (code !== 1000 && code !== 1005) opts.onNotice?.(closeCodeText(code))
          scheduleReconnect()
        }
      }
    }
    if (accessTokenFresh()) start()
    else void refresh().then(start).catch(start)
  }

  connect()

  return {
    subscribe(next) { if (next) query = { ...next }; if (ready && !paused) sendSubscribe(); else needsSubscribe = true },
    page(after, limit = 100) { const request_id = newRequestId(); latest.set('flows_page', request_id); send({ type: 'page', request_id, after, limit }) },
    select(flowId, active = true) {
      selected = flowId ? { id: flowId, active } : null
      if (paused) {
        needsSelect = flowId !== null
        needsDeselect = flowId === null
        return
      }
      needsSelect = false
      needsDeselect = false
      sendSelectFrame(flowId, active)
    },
    messages(flowId, after, limit = 100) {
      const request_id = newRequestId(); latest.set('messages_page', request_id)
      if (paused) { pendingMessages = { flowId, after, limit, requestId: request_id }; return }
      send({ type: 'messages', request_id, flow_id: flowId, after: after ?? null, limit })
    },
    search(value) { const request_id = newRequestId(); latest.set('search_page', request_id); send({ type: 'search', request_id, ...value, after: value.after ?? null }); return request_id },
    resync() { const request_id = newRequestId(); latest.set('snapshot', request_id); send({ type: 'resync', request_id, reason: 'loss' }) },
    pause() { paused = true; send({ type: 'pause' }) },
    resume() {
      // A reconnect while paused sends `pause` to the fresh Core session, and
      // `subscribe` never clears that flag: only a `resume` frame does. Replay
      // the subscription first (Core's ResumeFrame carries no query and would
      // answer `not_subscribed`), then resume so Core unpauses and re-snapshots
      // once at a new generation. The subscribe snapshot is superseded by the
      // resume snapshot via `latest`, so exactly one snapshot is applied.
      // Repeating resume while already live is a no-op (no resync storm).
      const resuming = paused || needsSubscribe || !subscribed
      paused = false
      if (needsSubscribe || !subscribed) {
        sendSubscribe()
        if (selected) sendSelectFrame(selected.id, selected.active)
        else if (needsDeselect) sendSelectFrame(null, false)
        needsSelect = needsDeselect = false
        flushPendingMessages()
      }
      else {
        if (needsSelect) { needsSelect = false; if (selected) sendSelectFrame(selected.id, selected.active) }
        else if (needsDeselect) { needsDeselect = false; sendSelectFrame(null, false) }
        flushPendingMessages()
      }
      if (!resuming) return
      const request_id = newRequestId(); latest.set('snapshot', request_id); send({ type: 'resume', request_id })
    },
    close() { closed = true; clearTimers(); connection++; try { socket?.close(1000) } catch { /* already closed */ } socket = undefined; opts.onState('retrying') },
  }
}

function directionSegments(segments: HistorySegment[], direction: number): HistorySegment[] {
  return segments.filter(segment => segment.direction === direction)
}

function trimDirection(segments: HistorySegment[], direction: number): HistorySegment[] {
  const keep = new Set<HistorySegment>()
  let bytes = 0
  let chunks = 0
  for (let index = segments.length - 1; index >= 0; index--) {
    const segment = segments[index]!
    if (segment.direction !== direction) continue
    const size = Array.isArray(segment.data) ? segment.data.length : 0
    if (chunks >= 1024 || bytes + size > 64 * 1024) continue
    bytes += size
    chunks++
    keep.add(segment)
  }
  return segments.filter(segment => segment.direction !== direction || keep.has(segment))
}

/**
 * Apply one active_bytes snapshot with the spec's replace/append/gap rules.
 * A reset replaces both directions; otherwise only newer sequences append and an
 * identical shorter head chunk is superseded. Each direction keeps ≤64 KiB / 1024 chunks.
 */
export function applyActiveHistory(current: WorkspaceHistory | undefined, snapshot: ActiveHistorySnapshot): WorkspaceHistory {
  const incoming = Array.isArray(snapshot.segments) ? snapshot.segments : []
  const previous = current && current.flow_id === snapshot.flow_id ? current : undefined
  const reset = !previous || (snapshot.pending !== true && snapshot.reset === true)
  let segments: HistorySegment[]
  if (reset) {
    segments = incoming.map(segment => ({ ...segment, data: [...segment.data] }))
  } else {
    segments = previous.segments.map(segment => ({ ...segment, data: [...segment.data] }))
    const maxSequence = new Map<number, number>()
    for (const segment of segments) maxSequence.set(segment.direction, Math.max(maxSequence.get(segment.direction) ?? 0, Number(segment.sequence) || 0))
    for (const segment of incoming) {
      const direction = segment.direction
      const sequence = Number(segment.sequence) || 0
      const index = segments.findIndex(existing => existing.direction === direction && Number(existing.sequence) === sequence)
      if (index >= 0) {
        // Same sequence: a shorter head chunk means the buffer drained part of it; replace in place.
        if ((segment.data?.length ?? 0) !== (segments[index]!.data?.length ?? 0)) segments[index] = { ...segment, data: [...segment.data] }
        continue
      }
      if (sequence > (maxSequence.get(direction) ?? 0)) {
        segments.push({ ...segment, data: [...segment.data] })
        maxSequence.set(direction, sequence)
      }
    }
  }
  segments = trimDirection(trimDirection(segments, 1), 2)
  const previousClientDrop = current?.client_head_dropped ?? 0
  const previousServerDrop = current?.server_head_dropped ?? 0
  const clientDrop = Math.max(0, Number(snapshot.client_head_dropped) || 0)
  const serverDrop = Math.max(0, Number(snapshot.server_head_dropped) || 0)
  return {
    flow_id: snapshot.flow_id,
    active: snapshot.active === true,
    pending: snapshot.pending === true,
    segments,
    reset,
    truncated_client_to_server: snapshot.truncated_client_to_server === true,
    truncated_server_to_client: snapshot.truncated_server_to_client === true,
    retained_client_to_server_bytes: Number(snapshot.retained_client_to_server_bytes) || directionSegments(segments, 1).reduce((total, segment) => total + segment.data.length, 0),
    retained_server_to_client_bytes: Number(snapshot.retained_server_to_client_bytes) || directionSegments(segments, 2).reduce((total, segment) => total + segment.data.length, 0),
    client_head_dropped: clientDrop,
    server_head_dropped: serverDrop,
    gap_bytes: reset ? 0 : Math.max(0, clientDrop - previousClientDrop) + Math.max(0, serverDrop - previousServerDrop),
    generated_at_ms: Number(snapshot.generated_at_ms) || 0,
  }
}
