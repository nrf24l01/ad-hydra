import type { FlowHistory, HistorySegment, Page } from '@/generated/contracts'
import { readFlagSpans, type FlagSpan } from './highlight'
export function record(value: unknown): Record<string, unknown> { return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {} }
export function text(value: unknown) { return typeof value === 'string' || typeof value === 'number' ? String(value) : '' }
export function direction(value: unknown) { return value === 1 ? 'Client → server' : value === 2 ? 'Server → client' : 'Unknown direction' }
export function transportLabel(value:unknown):string { return value===1?'TCP':value===2?'UDP':'Unknown transport' }
export function timestamp(value: unknown) { return typeof value === 'number' && Number.isFinite(value) ? new Date(value).toISOString() : 'Unknown time' }
/** Safe labels for known proxy socket failures; unknown error text is never rendered. */
export function closureReasonText(value: unknown): string | undefined {
 const messages: Record<string,string> = {
  'io:ConnectionRefused':'Upstream refused the connection.',
  'io:BrokenPipe':'Connection ended while Hydra was writing to a peer.',
  'io:ConnectionReset':'A peer reset the connection.',
  'io:ConnectionAborted':'A peer aborted the connection.',
  'io:TimedOut':'The connection timed out.',
  'io:UnexpectedEof':'A peer ended the connection unexpectedly.',
 }
 return typeof value==='string'&&Object.hasOwn(messages,value)?messages[value]:undefined
}
/** Safe display copy for an allowlisted decoder warning returned by flow detail. */
export function decoderWarningText(value: unknown): string | undefined {
 return Array.isArray(value)&&value.includes('http_response_decoder_disabled')
  ? 'The HTTP response decoder stopped; parsed response content may be incomplete.'
  : undefined
}
export function endpoint(value: unknown) { const v=record(value), ip=text(v.ip); return ip ? `${ip.includes(':')?'['+ip+']':ip}:${text(v.port)}` : 'Unknown endpoint' }
export function flowRows(page: Page) { return page.items.map(item => ({ ...item, closed:item.closed, favorite:item.favorite===true, raw_history_state:text(item.raw_history_state), decoded_incomplete:item.decoded_incomplete===true, blocked:item.blocked===true, has_flag:item.has_flag===true, flow_id:text(item.flow_id), service_id:text(item.service_id), transport:Number(item.transport??0), client:endpoint(item.client), destination:endpoint(item.upstream), started:timestamp(item.occurred_ms), state:item.closed?'Closed':'Open', bytesUp:text(record(item.closed).bytes_client_to_server), bytesDown:text(record(item.closed).bytes_server_to_client) })) }
export type Flow = ReturnType<typeof flowRows>[number]
/** A favourite can outlive its last lifecycle event after a Proxy interruption. */
export function canReadSavedHistory(flow: Pick<Flow,'state'|'favorite'|'raw_history_state'> | undefined, historyAvailable = false): boolean {
 return historyAvailable||flow?.state==='Closed'||Boolean(flow?.favorite&&['ready','partial'].includes(flow.raw_history_state))
}
export type FlowByteTotals = {bytesUp:string;bytesDown:string}
/** Show counters only when Core or a completed Proxy history supplies both directions. */
export function flowByteTotals(flow: Pick<Flow,'closed'|'bytesUp'|'bytesDown'> | undefined, history?: Pick<FlowHistory,'closed_at_ms'|'bytes_client_to_server'|'bytes_server_to_client'>): FlowByteTotals | undefined {
 if(history&&history.closed_at_ms>0)return {bytesUp:text(history.bytes_client_to_server),bytesDown:text(history.bytes_server_to_client)}
 if(flow?.closed&&flow.bytesUp!==''&&flow.bytesDown!=='')return {bytesUp:flow.bytesUp,bytesDown:flow.bytesDown}
 return undefined
}
export function mergeFlows(current: Flow[], incoming: Flow[], older = false): Flow[] {
 const unique = new Map<string, Flow>()
 for (const flow of older ? [...current,...incoming] : [...incoming,...current]) {
  if (!unique.has(flow.flow_id)) unique.set(flow.flow_id,flow)
 }
 return [...unique.values()].slice(0,500)
}
export function messageRows(page: Page) { return page.items.map(item => ({ ...item, blocked:item.blocked===true, message_id:text(item.message_id), stream_sequence:item.stream_sequence, occurred_ms:item.occurred_ms, direction:item.direction, message_type:text(item.message_type), protocol_id:text(item.protocol_id), fields:record(item.fields), truncated:item.truncated===true, display_text:typeof item.display_text==='string'?item.display_text:undefined, flag_spans:readFlagSpans(item.flag_spans) })) }
export type Message = ReturnType<typeof messageRows>[number]
/** Server display annotation only; never used as finding truth. */
export function messageDisplayText(message: Pick<Message,'display_text'|'fields'>): string {
 return typeof message.display_text==='string'?message.display_text:streamText(message)
}
export type { FlagSpan }
/** Saved native transfer summaries describe read sizes, independent of the service's current profile. */
export function onlyRawSummaryMessages(messages: Message[]): boolean {
 return messages.length>0&&messages.every(message=>message.protocol_id==='transfer'&&message.message_type==='byte_summary')
}
export function serviceProtocol(service:{id:string;protocol_id:string},activations:{service_id:string;id:string;version:string;applied:boolean}[]):string {
 const active=activations.find(item=>item.service_id===service.id&&item.applied)
 return active?`custom:${active.id}/${active.version}`:service.protocol_id
}
export function initialStreamMode(protocolId: string): 'bytes'|'decoded'|'terminal' { return protocolId==='raw'?'bytes':protocolId==='utf8'?'terminal':'decoded' }
export function fields(message: Pick<Message,'fields'>) { return Object.entries(message.fields).map(([key,value])=>({key,type:text(record(value).type),value:JSON.stringify(record(value).value)??''})) }
/** Retained HTTP headers, preserving observed spelling, duplicates and whitespace. */
export function httpHeaderLines(field: unknown): string[] | undefined {
 const list=record(field)
 if(list.type!=='list'||!Array.isArray(list.value))return undefined
 const lines:string[]=[];let remaining=16384
 for(const item of list.value.slice(0,128)) {
  const object=record(item),header=record(object.value),name=record(header.name),value=record(header.value)
  if(object.type!=='object'||name.type!=='string'||typeof name.value!=='string'||value.type!=='string'||typeof value.value!=='string')continue
  const line=`${name.value}:${value.value}`
  if(line.length+2>remaining){lines.push('[headers truncated]');return lines}
  lines.push(line);remaining-=line.length+2
 }
 if(list.value.length>128)lines.push('[headers truncated]')
 return lines
}
/** Decoded fields only. This is not a reconstructed wire payload. */
export function streamText(message: Pick<Message,'fields'>): string {
 const lines:string[]=[];let length=0,truncated=false
 for(const [key,field] of Object.entries(message.fields)) {
  const headers=key==='http.headers'||key==='http.trailers'?httpHeaderLines(field):undefined
  if(headers){lines.push(`${key}:`,...headers);continue}
  if(truncated)continue
  const value=record(field).value
  const line=`${key}: ${typeof value==='string'?value:JSON.stringify(value)??''}`
  lines.push(line.slice(0,Math.max(0,8192-length)));length+=line.length+1
  if(length>8192){lines.push('[preview truncated]');truncated=true}
 }
 return lines.join('\n')||'(no decoded fields)'
}
export function bytePreview(data:number[]) {
 const bytes=data,lines:string[]=[]
 const ascii=bytes.map(b=>b===9||b===10||b===13||b>=32&&b<127?String.fromCharCode(b):'·').join('')
 const utf8=new TextDecoder('utf-8').decode(new Uint8Array(bytes))
 for(let offset=0;offset<bytes.length;offset+=16){const row=bytes.slice(offset,offset+16);lines.push(`${offset.toString(16).padStart(8,'0')}  ${row.map(b=>b.toString(16).padStart(2,'0')).join(' ').padEnd(47,' ')}  ${row.map(b=>b>=32&&b<127?String.fromCharCode(b):'.').join('')}`)}
 return {ascii,hex:lines.join('\n'),utf8}
}
// Optional fields remain compatible with histories produced before UDP framing.
export type SavedHistorySegment = Pick<HistorySegment,'direction'|'data'> & { datagram?: boolean; occurred_at_ms?: number; sequence?: number }
export type HistoryDisplaySegment = SavedHistorySegment & { truncated: boolean; previewText?: string }
/** Preserves arrival order while optionally limiting a saved stream to one direction. */
export function historySegments(history: Pick<FlowHistory,'truncated_client_to_server'|'truncated_server_to_client'> & { segments: SavedHistorySegment[] }, side: 'both'|'1'|'2'): HistoryDisplaySegment[] {
 return (history.segments as SavedHistorySegment[]).filter(segment=>side==='both'||String(segment.direction)===side).map(segment=>({...segment,truncated:segment.direction===1?history.truncated_client_to_server:history.truncated_server_to_client}))
}
/** Decode saved read chunks incrementally per TCP direction, without changing their byte arrays or order. */
function withUtf8Previews(segments: HistoryDisplaySegment[]): HistoryDisplaySegment[] {
 const result=segments.map(segment=>({...segment,previewText:''}))
 const decoders=new Map<number,TextDecoder>(),lastByDirection=new Map<number,number>()
 for(const [index,segment] of result.entries()) {
  if(segment.datagram) {
   segment.previewText=new TextDecoder('utf-8').decode(new Uint8Array(segment.data))
   continue
  }
  let decoder=decoders.get(segment.direction)
  if(!decoder){decoder=new TextDecoder('utf-8');decoders.set(segment.direction,decoder)}
  segment.previewText=decoder.decode(new Uint8Array(segment.data),{stream:true})
  lastByDirection.set(segment.direction,index)
 }
 for(const [direction,decoder] of decoders) {
  const last=result[lastByDirection.get(direction)!]
  if(last)last.previewText+=decoder.decode()
 }
 return result
}
/** Captured-byte UTF-8 view: direction tails decode incrementally while each UDP datagram resets decoding. */
export function historyUtf8Segments(history: Pick<FlowHistory,'truncated_client_to_server'|'truncated_server_to_client'> & { segments: SavedHistorySegment[] }, side: 'both'|'1'|'2'): HistoryDisplaySegment[] {
 return withUtf8Previews(historySegments(history,side))
}
/** Combine saved chunks per direction so split UTF-8 code points decode correctly. */
export function historyTextSegments(history: Pick<FlowHistory,'truncated_client_to_server'|'truncated_server_to_client'> & { segments: SavedHistorySegment[] }, side: 'both'|'1'|'2'): HistoryDisplaySegment[] {
 const segments=historySegments(history,side)
 // Datagram text must never combine adjacent packets or split UTF-8 between packets.
 if(segments.some(segment=>segment.datagram))return withUtf8Previews(segments)
 // New TCP records carry one global sequence; keep the true conversation order.
 if(segments.length&&segments.every(segment=>Number(segment.sequence)>0))return withUtf8Previews(segments)
 const grouped=new Map<number,HistoryDisplaySegment>()
 for(const segment of segments) {
  const existing=grouped.get(segment.direction)
  if(existing){existing.data.push(...segment.data);existing.truncated ||= segment.truncated}
  else grouped.set(segment.direction,{...segment,data:[...segment.data]})
 }
 return withUtf8Previews([...grouped.values()])
}
/** Raw defaults to the hex dump; text decoding is an explicit operator choice. */
export function historyPreview(segment: Pick<HistorySegment,'data'>, mode: 'hex'|'utf8'|'ascii'): string {
 const preview=bytePreview(segment.data)
 return mode==='utf8'?preview.utf8:mode==='ascii'?preview.ascii:preview.hex
}
export function differences(a:Pick<Message,'fields'>,b:Pick<Message,'fields'>) {
 return [...new Set([...Object.keys(a.fields),...Object.keys(b.fields)])].sort().map(key=>{
  const left=JSON.stringify(a.fields[key]),right=JSON.stringify(b.fields[key])
  return {key,left:left??'Absent',right:right??'Absent',change:left===right?'Unchanged':left===undefined?'Added':right===undefined?'Removed':'Changed'}
 })
}

export function visibleFlows(rows: Flow[], showBlocked: boolean): Flow[] {
 return showBlocked ? rows : rows.filter(flow=>!flow.blocked)
}
/** Stable across service ordering, pagination, sessions and reloads. */
export { defaultServiceColor as serviceColor } from '@/features/services/colors'
export function isBlockedHit(event: Record<string,unknown>): boolean {
 return event.kind==='rule_hit'&&event.mode===2&&(event.blocked===true||[3,4,5].includes(Number(event.action)))
}
