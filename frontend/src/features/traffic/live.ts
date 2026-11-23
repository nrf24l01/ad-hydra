import { flowRows, messageRows, record, isBlockedHit, type Flow, type Message } from './traffic'
import { parseEventFrame } from '@/shared/api/sse'
export type TrafficEvent = Record<string, unknown> & { kind: string; flow_id: string; service_id: string }
export function trafficEvent(frame: string): TrafficEvent | undefined {
 const parsed=parseEventFrame(frame)
 if(parsed?.event!=='traffic')return
 return liveEvent(JSON.parse(parsed.data))
}
/** Typed view of one WebSocket `flow`/`message` event (same shape as SSE traffic data). */
export function liveEvent(value: unknown): TrafficEvent | undefined {
 const event=record(value)
 if(typeof event.kind!=='string'||typeof event.flow_id!=='string'||typeof event.service_id!=='string')throw new Error('Invalid live traffic event')
 return event as TrafficEvent
}
/** Apply committed parsed events directly; no timer or REST round trip per message. */
export function updateFlows(rows: Flow[],event:TrafficEvent):Flow[] {
 if(event.kind==='favorite_changed'&&typeof event.favorite==='boolean')return rows.map(row=>row.flow_id===event.flow_id?{...row,favorite:event.favorite===true,raw_history_state:typeof event.raw_history_state==='string'?event.raw_history_state:row.raw_history_state,decoded_incomplete:typeof event.decoded_incomplete==='boolean'?event.decoded_incomplete:row.decoded_incomplete}:row)
 if(event.kind==='flow_opened') {
  const flow=flowRows({items:[event],next_cursor:null})[0]
  if(!flow)return rows
  const old=rows.find(r=>r.flow_id===flow.flow_id)
  const merged={...flow,favorite:typeof event.favorite==='boolean'?flow.favorite:old?.favorite??false,raw_history_state:flow.raw_history_state||old?.raw_history_state||'',decoded_incomplete:flow.decoded_incomplete||old?.decoded_incomplete===true,blocked:flow.blocked||old?.blocked===true,has_flag:flow.has_flag||old?.has_flag===true}
  return [old?.closed?{...merged,closed:old.closed,state:old.state,bytesUp:old.bytesUp,bytesDown:old.bytesDown}:merged,...rows.filter(r=>r.flow_id!==flow.flow_id)].slice(0,500)
 }
 if(event.kind==='flow_closed')return rows.map(row=>row.flow_id===event.flow_id?{...row,blocked:row.blocked||event.blocked===true,has_flag:row.has_flag||event.has_flag===true,closed:event,state:'Closed',bytesUp:String(event.bytes_client_to_server??''),bytesDown:String(event.bytes_server_to_client??'')}:row)
 if(isBlockedHit(event))return rows.map(row=>row.flow_id===event.flow_id?{...row,blocked:true}:row)
 if(event.kind==='flag_detected')return rows.map(row=>row.flow_id===event.flow_id?{...row,has_flag:true}:row)
 return rows
}
export function mergeMessages(current:Message[],incoming:Message[]):Message[] {
 const byId=new Map(current.map(m=>[m.message_id,m]))
 for(const message of incoming)byId.set(message.message_id,message)
 return [...byId.values()].sort((a,b)=>typeof a.stream_sequence==='number'&&typeof b.stream_sequence==='number'?a.stream_sequence-b.stream_sequence:Number(a.occurred_ms??0)-Number(b.occurred_ms??0)).slice(-500)
}
export function eventMessage(event:TrafficEvent):Message|undefined {
 return event.kind==='message'?messageRows({items:[event],next_cursor:null})[0]:undefined
}
