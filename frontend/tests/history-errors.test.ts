import { expect, test } from 'bun:test'
import { captureHistoryError } from '../src/features/traffic/historyErrors'

const terminalCodes = ['history_pending', 'active_bytes_unavailable', 'history_unavailable', 'flow_not_found'] as const
const requestId = 'req-terminal-capture-7'

function unavailableNotice(code: string, id?: string | null): string {
 const result = captureHistoryError(code, id)
 expect(result).not.toBeNull()
 if (!result) throw new Error(`expected ${code} to be a terminal capture error`)
 expect(result.status).toBe('unavailable')
 return result.notice
}

test('history_pending is terminal, tells the user to Refresh manually and does not keep retrying', () => {
 const notice = unavailableNotice('history_pending', requestId)
 expect(notice).toContain('Captured bytes')
 expect(notice).toContain('Refresh')
 expect(notice).toContain(requestId)
 expect(notice).not.toMatch(/retrying/i)
 expect(notice).not.toMatch(/partial loss/i)
 expect(notice).not.toMatch(/(?:bytes|packets)\s+(?:were\s+)?lost/i)
})

test('all terminal retained-capture codes map to unavailable Captured bytes notices with the request reference', () => {
 for (const code of terminalCodes) {
  const notice = unavailableNotice(code, requestId)
  expect(notice).toContain('Captured bytes')
  expect(notice).toContain(requestId)
  expect(notice).not.toMatch(/partial loss/i)
  expect(notice).not.toMatch(/(?:bytes|packets)\s+(?:were\s+)?lost/i)
 }
})

test('metadata, query, search, auth and unknown errors stay null for the normal error handler', () => {
 const passthrough: (string | undefined)[] = [
  'flow_unavailable',
  'invalid_query',
  'search_timeout',
  'unauthorized',
  'invalid_frame',
  'internal_error',
  'unknown',
  undefined,
 ]
 for (const code of passthrough) expect(captureHistoryError(code, requestId)).toBeNull()
})

test('a missing request id never renders undefined or null and never invents a reference', () => {
 for (const code of terminalCodes) {
  const omitted = unavailableNotice(code)
  const explicitNull = unavailableNotice(code, null)
  const empty = unavailableNotice(code, '')
  expect(omitted).toBe(explicitNull)
  expect(omitted).toBe(empty)
  expect(omitted).not.toMatch(/undefined|null/i)
  expect(omitted).not.toMatch(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i)
 }
})
