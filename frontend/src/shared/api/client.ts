import { apiBaseUrl } from '@/shared/config/env'
import { path as operationPath } from '@/generated/operations'
import { EventFrames, isEventFrame } from './sse'
let token = '', expires = 0
let refreshing: Promise<void> | undefined
const sessions = typeof window === 'undefined' ? undefined : new BroadcastChannel('hydra-session')
const changed = () => window.dispatchEvent(new Event('hydra-session-changed'))
if (sessions) sessions.onmessage = () => { clear(); changed() }
export class ApiError extends Error {
  constructor(message: string, public status: number, public requestId?: string) { super(message) }
}
export function clear(): void { token = ''; expires = 0 }
/** Access token for in-band WebSocket auth. Never put this value in a URL. */
export function accessToken(): string { return token }
export function accessTokenFresh(withinMs = 5000): boolean { return Boolean(token) && Date.now() <= expires - withinMs }
function remember(value: unknown): void {
  if (!value || typeof value !== 'object' || !('access_token' in value) || typeof value.access_token !== 'string' || !('expires_in' in value) || typeof value.expires_in !== 'number') throw new Error('Invalid sign-in response')
  token = value.access_token; expires = Date.now() + value.expires_in * 1000
}
const endpoint = (suffix: string) => `${apiBaseUrl}/v1${suffix}`
const operation = (id: Parameters<typeof operationPath>[0], values: Record<string, string> = {}) => apiBaseUrl + operationPath(id, values).slice(4)
const csrf = () => document.cookie.split(';').map(x => x.trim()).find(x => x.startsWith('hydra_csrf='))?.slice(11) || ''
async function error(response: Response): Promise<never> {
  const body: unknown = await response.json().catch(() => undefined)
  let message = `Request failed (${response.status})`, requestId = response.headers.get('x-request-id') || undefined
  if (body && typeof body === 'object') {
    if ('request_id' in body && typeof body.request_id === 'string') requestId = body.request_id
    if ('error' in body && typeof body.error === 'string') message = body.error
    if ('error' in body && body.error && typeof body.error === 'object' && 'message' in body.error && typeof body.error.message === 'string') message = body.error.message
  }
  throw new ApiError(message, response.status, requestId)
}
export function errorMessage(value: unknown): string {
  if (value instanceof ApiError) return value.message + (value.requestId ? ` · Reference: ${value.requestId}` : '')
  return value instanceof Error ? value.message : 'Request failed'
}
export async function login(username: string, password: string): Promise<void> {
  const response = await fetch(operation('login'), { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({username,password}) })
  if (!response.ok) await error(response)
  remember(await response.json()); sessions?.postMessage('login')
}
export async function refresh(): Promise<void> {
  if (!refreshing) {
    const run = async () => {
      const response = await fetch(operation('refresh'), { method: 'POST', credentials: 'same-origin', headers: { 'X-CSRF-Token': csrf() } })
      if (!response.ok) { clear(); changed(); await error(response) }
      remember(await response.json())
    }
    refreshing = (async () => {
      if (navigator.locks) await navigator.locks.request('hydra-refresh', run)
      else await run()
    })().finally(() => { refreshing = undefined })
  }
  return refreshing
}
export async function api<T = Record<string, unknown>>(path: string, method = 'GET', value?: unknown, retry = true, extraHeaders: Record<string,string> = {}, raw = false): Promise<T> {
  if (!token || Date.now() > expires - 5000) await refresh()
  const headers: Record<string,string> = {...extraHeaders, Authorization: `Bearer ${token}`}
  if (value !== undefined) headers['Content-Type'] = raw ? 'application/octet-stream' : 'application/json'
  if (path === '/auth/logout') headers['X-CSRF-Token'] = csrf()
  const response = await fetch(endpoint(path), {method, credentials:'same-origin', headers, body: value === undefined ? undefined : raw ? value as BodyInit : JSON.stringify(value)})
  if (response.status === 401 && retry) { await refresh(); return api<T>(path,method,value,false,extraHeaders,raw) }
  if (!response.ok) await error(response)
  return response.status === 204 ? undefined as T : response.json() as Promise<T>
}
export async function logout(): Promise<void> { await api('/auth/logout','POST'); clear(); changed(); sessions?.postMessage('logout') }
export type LiveState = 'connecting' | 'live' | 'retrying'
export async function events(onEvent: (frame: string) => void, signal: AbortSignal, onState?: (state:LiveState)=>void): Promise<void> {
  onState?.('connecting')
  while (!signal.aborted) {
    try {
      if (!token || Date.now() > expires - 5000) await refresh()
      const response = await fetch(endpoint('/traffic/events/stream'), {headers: {Authorization:`Bearer ${token}`}, signal, credentials:'same-origin'})
      if (response.status === 401) { await refresh(); continue }
      if (!response.ok) await error(response)
      if (!response.body) throw new Error('Live stream unavailable')
      const reader = response.body.getReader(), decoder = new TextDecoder(), frames = new EventFrames()
      onState?.('live')
      try {
        while (!signal.aborted) {
          const {done,value} = await reader.read(); if (done) break
          for (const frame of frames.push(decoder.decode(value,{stream:true}))) if (isEventFrame(frame)) onEvent(frame)
        }
      } finally { await reader.cancel().catch(() => {}); reader.releaseLock() }
    } catch { if (signal.aborted) return }
    if (!signal.aborted) onState?.('retrying')
    if (!signal.aborted) await new Promise<void>(resolve => {
      const finish = () => { clearTimeout(timer); signal.removeEventListener('abort',finish); resolve() }
      const timer = setTimeout(finish,1000); signal.addEventListener('abort',finish,{once:true})
    })
  }
}
export async function uploadProtocol(id: string, version: string, file: File): Promise<{error?:string; state:string}> {
  if (file.size > 4*1024*1024) throw new Error('WASM limit is 4 MiB')
  if (!token || Date.now() > expires - 5000) await refresh()
  const body = await file.arrayBuffer(), digest = await crypto.subtle.digest('SHA-256',body)
  const hash = [...new Uint8Array(digest)].map(x => x.toString(16).padStart(2,'0')).join('')
  const response = await fetch(operation('upload',{id,version}), {method:'PUT', headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/wasm','X-SHA256':hash},body})
  if (!response.ok) await error(response)
  return response.json()
}
export async function activateProtocol(id: string, version: string, service_id: string): Promise<{applied:boolean;error?:string}> {
  return api(`/protocols/${encodeURIComponent(id)}/${encodeURIComponent(version)}/activate`,'POST',{service_id},true,{'Idempotency-Key':crypto.randomUUID()})
}
