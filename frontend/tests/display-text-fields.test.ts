/**
 * Regression coverage for the Streams message pipeline: Core's annotated
 * `display_text` is a bounded display summary and must never replace, erase or
 * stringify the decoded typed fields (Inspect table) or the original field bytes.
 *
 * Fixture traced from the real length-prefixed decoder and the browser smoke:
 * a 2-byte big-endian length header followed by the payload, exposed as a typed
 * `protocol.payload` bytes field (`{"type":"bytes","value":[...]}`). Core's
 * `text.rs::display_text` deliberately omits binary fields, so a bytes-only
 * frame is summarised as `protocol_id` + `message_type`; `fields` keeps the bytes.
 *
 * Pure mapper contract only: no DOM, no transport, no source-text matching.
 */
import { expect, test } from 'bun:test'
import { fields, messageDisplayText, messageRows, streamText, type FlagSpan, type Message } from '../src/features/traffic/traffic'

type TypedField = { type: string; value: unknown }
type Row = Record<string, unknown>

const typedString = (value: string): TypedField => ({ type: 'string', value })
const typedBytes = (value: number[]): TypedField => ({ type: 'bytes', value })
/** 2-byte big-endian length prefix + payload, exactly as the length-prefixed decoder frames it. */
const framed = (payload: number[]): number[] => [payload.length >> 8, payload.length & 0xff, ...payload]
const fieldOf = (message: Message, key: string): TypedField => message.fields[key] as TypedField

/** A real-shaped message row: typed protocol.payload bytes, no client-side summary. */
function messageRow(payload: number[], overrides: Row = {}): Row {
  return {
    message_id: 'msg-1',
    stream_sequence: 7,
    occurred_ms: 1_700_000_000_000,
    direction: 1,
    protocol_id: 'custom:length-prefixed/v1',
    message_type: 'frame',
    sample_id: 'sample-1',
    original_size: framed(payload).length,
    truncated: false,
    fields: { 'protocol.payload': typedBytes(payload) },
    ...overrides,
  }
}

test('annotated display_text wins without erasing the decoded binary field', () => {
  const payload = [...new TextEncoder().encode('PING'), 0x00, 0xff]
  const source = messageRow(payload, { display_text: 'custom:length-prefixed/v1\nframe', flag_spans: [] })
  const [message] = messageRows({ items: [source], next_cursor: null })

  // Server summary is used verbatim, even though it cannot show the bytes.
  expect(messageDisplayText(message)).toBe('custom:length-prefixed/v1\nframe')
  expect(messageDisplayText(message)).not.toContain('protocol.payload')
  // Inspect fields still expose the typed payload and nothing else.
  expect(fields(message)).toEqual([{ key: 'protocol.payload', type: 'bytes', value: JSON.stringify(payload) }])
  // The Inspect table serialises the typed value as JSON; it must parse back to
  // the exact payload bytes. (`fieldOf` is the raw message field, not the table
  // cell, so it holds the number[] itself — asserted untouched below.)
  expect(JSON.parse(fields(message)[0]!.value as string)).toEqual(payload)
  // The raw field bytes stay in the message object; the summary did not replace them.
  expect(fieldOf(message, 'protocol.payload')).toEqual({ type: 'bytes', value: payload })
  expect(message.fields).toEqual(source.fields as Record<string, unknown>)
  // Unrelated row metadata is untouched by the annotation.
  expect(message).toMatchObject({
    message_id: 'msg-1',
    stream_sequence: 7,
    occurred_ms: 1_700_000_000_000,
    direction: 1,
    protocol_id: 'custom:length-prefixed/v1',
    message_type: 'frame',
    sample_id: 'sample-1',
    original_size: framed(payload).length,
    truncated: false,
    blocked: false,
  })
})

test('without display_text the existing decoded-field fallback is preserved verbatim', () => {
  const payload = [...new TextEncoder().encode('PING'), 0x00, 0xff]
  const literal = '<script>alert("x")</script> & <b>not markup</b>'
  const source = messageRow(payload, {
    fields: { 'protocol.payload': typedBytes(payload), 'utf8.text': typedString(literal) },
  })
  const [message] = messageRows({ items: [source], next_cursor: null })
  const rendered = messageDisplayText(message)

  expect(message.display_text).toBeUndefined()
  expect(rendered).toBe(`protocol.payload: ${JSON.stringify(payload)}\nutf8.text: ${literal}`)
  expect(rendered).toBe(streamText(message))
  // User bytes are returned as plain text; nothing here parses or executes HTML.
  expect(rendered).toContain('<script>alert("x")</script>')
  expect(message.fields).toEqual(source.fields as Record<string, unknown>)
})

test('UTF-8 display_text and flag_spans annotate without rewriting the text field', () => {
  const text = 'user <img src=x onerror=alert(1)> FLAG{abc123}'
  const flag = 'FLAG{abc123}'
  const spans: FlagSpan[] = [[text.indexOf(flag), text.indexOf(flag) + flag.length]]
  const source = messageRow([], {
    direction: 2,
    original_size: new TextEncoder().encode(text).length,
    fields: { 'utf8.text': typedString(text) },
    display_text: text,
    flag_spans: spans,
  })
  const [message] = messageRows({ items: [source], next_cursor: null })

  expect(messageDisplayText(message)).toBe(text)
  expect(message.flag_spans).toEqual(spans)
  expect(fieldOf(message, 'utf8.text')).toEqual({ type: 'string', value: text })
  expect(fields(message)).toEqual([{ key: 'utf8.text', type: 'string', value: JSON.stringify(text) }])
  // The annotation is not the fallback's field-labelled rendering.
  expect(messageDisplayText(message)).not.toContain('utf8.text:')
})

test('per-row summaries and payloads stay independent across a page', () => {
  const request = Array.from(new TextEncoder().encode('PING'))
  const response = Array.from(new TextEncoder().encode('PONG'))
  const messages = messageRows({
    items: [
      messageRow(request, { message_id: 'req', stream_sequence: 1, display_text: 'request frame' }),
      messageRow(response, { message_id: 'res', stream_sequence: 2, direction: 2, occurred_ms: 1_700_000_000_001, display_text: 'response frame' }),
    ],
    next_cursor: null,
  })

  expect(messages.map(messageDisplayText)).toEqual(['request frame', 'response frame'])
  expect(messages.map(message => fieldOf(message, 'protocol.payload').value)).toEqual([request, response])
  expect(messages[1]).toMatchObject({ message_id: 'res', stream_sequence: 2, direction: 2, occurred_ms: 1_700_000_000_001 })
  expect(fields(messages[1]!)[0]!.value).toBe(JSON.stringify(response))
})
