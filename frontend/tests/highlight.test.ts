/**
 * Frontend unit tests for server-annotated flag highlighting.
 *
 * Contract (design §5.3, §6):
 *   segments(text, spans: Array<[number, number]>) -> Array<{text: string; flag: boolean}>
 *   - spans are UTF-16 code-unit offsets into the returned `text`/`snippet`, sliced with String#slice
 *   - spans are validated: start < end <= length, sorted, non-overlapping, capped at 64; invalid dropped
 *   - rendering escapes by construction (text interpolation only): segments return literal text,
 *     never HTML entities and never evaluated markup
 *   - spans come from the server's configured highlight pattern; the browser never re-runs the regex
 *     over untrusted text for server spans
 *
 * No DOM is asserted here: this is the pure segmentation layer that Vue renders.
 */
import { expect, test } from 'bun:test'
import { segments } from '../src/features/traffic/highlight'
import { compileFlagPattern } from '../src/features/traffic/flagPattern'
import { flagChunks } from '../src/features/traffic/terminalColors'

/** Compute the spans the server would derive for `text`, so expectations are not hardcoded. */
function derivedSpans(text: string, pattern: string): Array<[number, number]> {
  const { regex } = compileFlagPattern(pattern)
  if (!regex) throw new Error(`pattern is not safe-compilable: ${pattern}`)
  const spans: Array<[number, number]> = []
  regex.lastIndex = 0
  for (let match = regex.exec(text); match; match = regex.exec(text)) {
    if (!match[0]) { regex.lastIndex++; continue }
    spans.push([match.index, match.index + match[0].length])
  }
  regex.lastIndex = 0
  return spans
}

function joined(parts: Array<{ text: string }>): string {
  return parts.map(part => part.text).join('')
}

test('slices spans as UTF-16 code units, not code points, around astral characters', () => {
  const text = '😀flag{x}'
  const spans = derivedSpans(text, 'flag\\{[a-z_]{1,64}\\}')
  // "😀" is one code point but two UTF-16 units, so the match starts at index 2 and ends at 9.
  expect(spans).toEqual([[2, 9]])
  const parts = segments(text, spans)
  expect(joined(parts)).toBe(text)
  expect(parts).toEqual([
    { text: '😀', flag: false },
    { text: 'flag{x}', flag: true },
  ])
})

test('keeps astral characters inside a flag span intact', () => {
  const text = 'a😀flag{😀}'
  const spans = derivedSpans(text, 'flag\\{.{0,8}\\}')
  expect(spans).toEqual([[3, 11]])
  const parts = segments(text, spans)
  expect(joined(parts)).toBe(text)
  expect(parts.filter(part => part.flag).map(part => part.text)).toEqual(['flag{😀}'])
})

test('returns literal text for <, > and & without HTML-encoding or evaluating markup', () => {
  const text = '<img src=x onerror=alert(1)>flag{abc}&"quote"'
  const spans = derivedSpans(text, 'flag\\{[a-z]{1,64}\\}')
  const parts = segments(text, spans)
  expect(joined(parts)).toBe(text)
  expect(parts.filter(part => part.flag).map(part => part.text)).toEqual(['flag{abc}'])
  for (const part of parts) {
    expect(part.text).not.toContain('&lt;')
    expect(part.text).not.toContain('&gt;')
    expect(part.text).not.toContain('&amp;')
    expect(part.text).not.toContain('&quot;')
  }
  expect(parts[0]?.text.startsWith('<img src=x onerror=alert(1)>')).toBe(true)
})

test('drops invalid, out-of-range and overlapping spans instead of guessing', () => {
  const text = 'abcdefghij'
  const parts = segments(text, [[-1, 3], [5, 2], [8, 12], [2, 4], [3, 6]])
  expect(joined(parts)).toBe(text)
  expect(parts.filter(part => part.flag).map(part => part.text)).toEqual(['cd'])
  for (const part of parts) expect(text.includes(part.text)).toBe(true)
})

test('handles empty text and no spans without inventing highlight', () => {
  expect(joined(segments('', []))).toBe('')
  expect(segments('', []).filter(part => part.flag)).toHaveLength(0)
  expect(segments('plain', [])).toEqual([{ text: 'plain', flag: false }])
})

test('caps server spans at 64 matches and preserves the full text', () => {
  const text = 'a'.repeat(120)
  const spans: Array<[number, number]> = Array.from({ length: 100 }, (_, index) => [index, index + 1] as [number, number])
  const parts = segments(text, spans)
  expect(joined(parts)).toBe(text)
  expect(parts.filter(part => part.flag).reduce((total, part) => total + part.text.length, 0)).toBe(64)
})

test('highlights exactly what the configured pattern derives, including a multi-match body', () => {
  const text = 'GET /?q=flag{one_two} HTTP/1.1\r\nX-Test: flag{two}\r\n'
  const spans = derivedSpans(text, 'flag\\{[a-z_]{1,64}\\}')
  expect(spans).toHaveLength(2)
  const parts = segments(text, spans)
  expect(joined(parts)).toBe(text)
  expect(parts.filter(part => part.flag).map(part => part.text)).toEqual(['flag{one_two}', 'flag{two}'])
  // Spans are exactly the regex match offsets; nothing is re-derived in the browser from prose.
  expect(parts.filter(part => part.flag)).toHaveLength(spans.length)
})

test('an unsupported or absent pattern yields no highlight and no raw RegExp evaluation', () => {
  const unsupported = compileFlagPattern('(a+)+$')
  expect(unsupported.regex).toBeNull()
  expect(compileFlagPattern('').regex).toBeNull()
  expect(segments('flag{abc}', []).filter(part => part.flag)).toHaveLength(0)
})

test('cross-chunk terminal flags keep every byte and highlight each chunk half (documented limitation)', () => {
  const pattern = compileFlagPattern('flag\\{[a-z]{1,64}\\}').regex
  expect(pattern).not.toBeNull()
  const background = '\x1b[48;2;61;46;18m'
  const split = [
    { id: '1', direction: 2, text: 'tail flag{' },
    { id: '2', direction: 2, text: 'abc} done' },
  ]
  const out = flagChunks(split, pattern)
  // No terminal bytes are dropped or reordered by the per-chunk highlighter.
  expect(out.map(chunk => chunk.text.replace(/\x1b\[[0-9;]*m/g, ''))).toEqual(['tail flag{', 'abc} done'])
  // Existing behaviour is per chunk, so a split flag is washed in two halves rather than one span.
  expect(out.every(chunk => chunk.text.includes(background))).toBe(true)

  const whole = flagChunks([{ id: '3', direction: 2, text: 'tail flag{abc} done' }], pattern)
  expect(whole[0]?.text).toBe(`tail ${background}flag{abc}\x1b[49m done`)
})
