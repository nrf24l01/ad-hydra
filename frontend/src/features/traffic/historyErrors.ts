/**
 * Terminal retained-capture failures Core reports on the live socket.
 *
 * `ws/history.rs` emits `history_pending` only after its bounded poll budget
 * (`HISTORY_RETRIES`) is spent and sets `history_done`, so the frame is
 * terminal: Core does not keep retrying. Every `error` frame carries the same
 * generic `message`, so the `code` names the failing phase and never the flow.
 *
 * A failed history read is not evidence that captured bytes were lost, and it
 * is never evidence that bytes were evicted: only an actual snapshot gap marker
 * reports eviction. These notices therefore never claim byte loss.
 */
const preserved='Decoded messages already loaded stay visible; unavailable history is not a missing flow'

function withRequest(text:string,requestId?:string|null){
 if(!requestId)return text
 const base=text.endsWith('.')?text.slice(0,-1):text
 return `${base} (request ${requestId}).`
}

/**
 * Map one WS `error` frame code to the Captured-bytes state, or `null` when the
 * code is not a history failure (including `flow_unavailable`, which is flow
 * metadata, not retained bytes).
 */
export function captureHistoryError(code:string|undefined,requestId?:string|null):{status:'unavailable';notice:string}|null {
 switch(code){
  case 'history_pending':
   return {status:'unavailable',notice:withRequest(`Captured bytes not yet persisted: the Proxy is still saving this session's history. ${preserved}. Refresh to try again.`,requestId)}
  case 'history_unavailable':
   return {status:'unavailable',notice:withRequest(`Captured bytes unavailable: the history read did not complete. ${preserved}. Refresh to try again.`,requestId)}
  case 'active_bytes_unavailable':
   return {status:'unavailable',notice:withRequest(`Captured bytes unavailable for this session: the retained tail was not served. ${preserved}.`,requestId)}
  case 'flow_not_found':
   return {status:'unavailable',notice:withRequest(`Captured bytes unavailable. No retained capture is stored for this session. ${preserved}.`,requestId)}
  default:return null
 }
}
