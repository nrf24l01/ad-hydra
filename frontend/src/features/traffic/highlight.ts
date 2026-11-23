/** A validated server span in UTF-16 code units, relative to the displayed string. */
export type FlagSpan = [number, number]
export interface HighlightSegment { text: string; flag: boolean }

const MAX_SPANS = 64


/**
 * Server-provided highlight spans are display annotations only. They are validated,
 * sorted and de-overlapped here, then sliced with String#slice (UTF-16 code units).
 * Nothing is interpolated as HTML; callers render text and <mark> segments in Vue.
 */
export function segments(text: string, spans: readonly FlagSpan[] | null | undefined): HighlightSegment[] {
  if (typeof text !== 'string' || !text) return []
  const valid = validateSpans(text.length, spans)
  if (!valid.length) return [{ text, flag: false }]
  const result: HighlightSegment[] = []
  let cursor = 0
  for (const [start, end] of valid) {
    if (start > cursor) result.push({ text: text.slice(cursor, start), flag: false })
    result.push({ text: text.slice(start, end), flag: true })
    cursor = end
  }
  if (cursor < text.length) result.push({ text: text.slice(cursor), flag: false })
  return result
}

/** Drop malformed, out-of-range and overlapping spans; keep the first of an overlap. */
export function validateSpans(length: number, spans: readonly unknown[] | null | undefined): FlagSpan[] {
  if (!Array.isArray(spans) || length <= 0) return []
  const candidates: FlagSpan[] = []
  for (const value of spans) {
    if (!Array.isArray(value) || value.length !== 2) continue
    const start = value[0]
    const end = value[1]
    if (typeof start !== 'number' || typeof end !== 'number') continue
    if (!Number.isInteger(start) || !Number.isInteger(end)) continue
    if (start < 0 || end <= start || end > length) continue
    candidates.push([start, end])
  }
  candidates.sort((a, b) => a[0] - b[0] || a[1] - b[1])
  const accepted: FlagSpan[] = []
  for (const span of candidates) {
    const previous = accepted[accepted.length - 1]
    if (previous && span[0] < previous[1]) continue
    accepted.push(span)
    if (accepted.length >= MAX_SPANS) break
  }
  return accepted
}

/** Read a server `flag_spans` value without trusting its shape. */
export function readFlagSpans(value: unknown): FlagSpan[] {
  return Array.isArray(value) ? value.filter((item): item is FlagSpan => Array.isArray(item) && item.length === 2 && typeof item[0] === 'number' && typeof item[1] === 'number') : []
}
