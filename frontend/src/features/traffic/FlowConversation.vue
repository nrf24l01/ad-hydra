<script setup lang="ts">
import { computed,defineAsyncComponent,nextTick,onMounted,onUnmounted,ref,watch } from 'vue'
import type { FlowHistory,ListResponse,Page } from '@/generated/contracts'
import { api,ApiError,errorMessage } from '@/shared/api/client'
import { openEditor } from '@/shared/editor'
import DataTable from '@/components/DataTable.vue'
import { Button } from '@/components/ui/button'
import { Alert,AlertDescription } from '@/components/ui/alert'
import { looksLikeTls } from './tlsDetect'
import { Select,SelectContent,SelectItem,SelectTrigger,SelectValue } from '@/components/ui/select'
import FlowCapture from './FlowCapture.vue'
import HttpMessage from './HttpMessage.vue'
import {isHttpMessage} from './http'
import {terminalMessages,terminalHistory} from './terminal'
const TerminalStream=defineAsyncComponent(()=>import('./TerminalStream.vue'))
import FlowFavoriteButton, { type FavoriteChange } from './FlowFavoriteButton.vue'
import { eventMessage,mergeMessages,type TrafficEvent } from './live'
import { canReadSavedHistory,closureReasonText,decoderWarningText,differences,direction,fields,flowByteTotals,flowRows,historyPreview,historySegments,historyTextSegments,historyUtf8Segments,initialStreamMode,messageDisplayText,messageRows,onlyRawSummaryMessages,record,timestamp,type Flow,type Message } from './traffic'
import { segments } from './highlight'
import type { WorkspaceHistory,WorkspaceStreamSource } from './liveSocket'
const props=defineProps<{flowId:string;flow?:Flow;protocolId?:string;showBlocked?:boolean;flagPattern?:string;serviceTls?:boolean;wsSource?:WorkspaceStreamSource}>()
const emit=defineEmits<{favorite:[value:FavoriteChange];'configure-tls':[]}>()
const detail=ref<Flow|undefined>(props.wsSource?.flow??props.flow),messages=ref<Message[]>([]),cursor=ref<number|null>()
const usingWs=computed(()=>props.wsSource!==undefined)
const wsHistory=computed(()=>props.wsSource?.history)
const wsActive=computed(()=>wsHistory.value?.active===true)
const wsPending=computed(()=>wsHistory.value?.pending===true)
const wsLoading=computed(()=>usingWs.value&&props.wsSource?.status==='loading')
const wsGap=computed(()=>Boolean(wsHistory.value&&(wsHistory.value.gap_bytes>0||wsHistory.value.client_head_dropped>0||wsHistory.value.server_head_dropped>0)))
const wsGapBytes=computed(()=>wsHistory.value?.gap_bytes??0)
/** Set only when Core reported a retained-capture failure; the flow and decoded messages remain valid. */
const wsHistoryNotice=computed(()=>props.wsSource?.historyNotice??'')
/** A known-closed flow can never still be capturing, even before the next snapshot arrives. */
const flowClosed=computed(()=>detail.value?.state==='Closed')
/** True only from an actual active_bytes snapshot (`active:true`) for a flow that is not closed. */
const liveCapture=computed(()=>usingWs.value&&wsActive.value&&!flowClosed.value)
const captureTruncated=computed(()=>Boolean(wsHistory.value?.truncated_client_to_server||wsHistory.value?.truncated_server_to_client))
const inspect=ref<Message>(),selected=ref<Message>(),comparison=ref<ReturnType<typeof differences>>([]),comparisonTitle=ref('')
const streamBody=ref<HTMLElement>()
const udp=computed(()=>detail.value?.transport===2)
const rawProfile=computed(()=>props.protocolId==='raw')
const closureHistoryLost=computed(()=>record(detail.value?.closed).history_truncated===true)
const closureReason=computed(()=>closureReasonText(record(detail.value?.closed).reason))
const savedStream=computed(()=>usingWs.value||canReadSavedHistory(detail.value,history.value!==undefined))
const rawSummariesOnly=computed(()=>onlyRawSummaryMessages(messages.value))
const hasHistoryTextFallback=computed(()=>savedStream.value&&(!messages.value.length||rawSummariesOnly.value)&&history.value!==undefined)
const rawSummaryMessages=computed(()=>rawSummariesOnly.value?visible.value:[])
const decodedMessages=computed(()=>hasHistoryTextFallback.value&&rawSummariesOnly.value?[]:visible.value)
const orderedTcpHistory=computed(()=>Boolean(history.value?.segments.length&&!history.value.segments.some(segment=>segment.datagram)&&history.value.segments.every(segment=>Number(segment.sequence)>0)))
const mode=ref(initialStreamMode(props.protocolId??'raw')),side=ref<'both'|'1'|'2'>('both'),loading=ref(false),error=ref(''),notice=ref('')
const byteMode=ref<'hex'|'utf8'|'ascii'>('hex'),history=ref<FlowHistory>(),historyState=ref<'idle'|'loading'|'ready'|'missing'|'active'|'unavailable'>('idle')
const completedCaptureForOpenFlow=computed(()=>history.value!==undefined&&history.value.closed_at_ms>0&&detail.value?.state==='Open')
const decoderWarning=computed(()=>decoderWarningText(record(detail.value).decoder_warnings))
const forwardedTotals=computed(()=>flowByteTotals(detail.value,history.value))
/** Retained capture bounds; forwarded totals only exist for Core-saved closed history. */
const captureBounds=computed(()=>{
 const h=history.value
 if(!h)return undefined
 return {
  retained:usingWs.value?h.bytes_client_to_server+h.bytes_server_to_client:h.segments.reduce((n,s)=>n+s.data.length,0),
  forwarded:h.closed_at_ms!==0&&!usingWs.value,
  c2s:h.bytes_client_to_server,
  s2c:h.bytes_server_to_client,
  truncated:Boolean(h.history_truncated||h.truncated_client_to_server||h.truncated_server_to_client),
 }
})
const visible=computed(()=>messages.value.filter(m=>(props.showBlocked||m.blocked!==true)&&(side.value==='both'||String(m.direction)===side.value)))
const terminalTranscript=computed(()=>{
 if(history.value){const saved=terminalHistory(history.value.segments,side.value);if(saved.ordered||side.value!=='both')return saved}
 return terminalMessages(visible.value,side.value)
})
const tlsUndecoded=computed(()=>!props.serviceTls&&looksLikeTls(terminalTranscript.value.chunks))
const terminalLegacyOrder=computed(()=>historyState.value==='ready'&&history.value&&history.value.segments.filter(s=>s.data.length>0).length>1&&!history.value.segments.filter(s=>s.data.length>0).every(s=>s.sequence>0)&&!terminalTranscript.value.chunks.length&&side.value==='both')
const path=`/traffic/flows/${encodeURIComponent(props.flowId)}`
function activeAsFlowHistory(value:WorkspaceHistory):FlowHistory {
 return {flow_id:value.flow_id,service_id:detail.value?.service_id??'',protocol_id:props.protocolId??'',opened_at_ms:0,closed_at_ms:value.active?0:value.generated_at_ms,transport:detail.value?.transport??0,segments:value.segments,bytes_client_to_server:value.retained_client_to_server_bytes,bytes_server_to_client:value.retained_server_to_client_bytes,truncated_client_to_server:value.truncated_client_to_server,truncated_server_to_client:value.truncated_server_to_client,history_truncated:value.truncated_client_to_server||value.truncated_server_to_client,datagrams_client_to_server:0,datagrams_server_to_client:0}
}
function applyWsSource(source:WorkspaceStreamSource|undefined) {
 if(!source)return
 if(source.flow)detail.value=source.flow
 messages.value=source.messages
 cursor.value=source.messageCursor??null
 history.value=source.history?activeAsFlowHistory(source.history):undefined
 historyState.value=source.status==='active'?'ready':source.status
}
function refresh(){if(usingWs.value){props.wsSource?.onRefresh?.();return}void load()}
function moreMessages(){if(usingWs.value){props.wsSource?.onMore?.();return}void load(true)}
function messageSegments(message:Message){return segments(messageDisplayText(message),message.flag_spans)}
function favoriteChanged(change:FavoriteChange){if(detail.value)detail.value={...detail.value,...change};if(savedStream.value&&!usingWs.value)void loadHistory();emit('favorite',change)}
watch(()=>props.wsSource,applyWsSource,{immediate:true})
watch(()=>props.flow,value=>{
 if(detail.value&&value){
  const wasSaved=savedStream.value
  detail.value={...detail.value,favorite:value.favorite,raw_history_state:value.raw_history_state,decoded_incomplete:value.decoded_incomplete}
  if(!wasSaved&&savedStream.value)void loadHistory()
 }
})
let disposed=false,request=0,dirty=false
function receive(event:TrafficEvent) {
 if(event.flow_id!==props.flowId)return
 const message=eventMessage(event)
 const body=streamBody.value,follow=!body||body.scrollHeight-body.scrollTop-body.clientHeight<64
 if(message){messages.value=mergeMessages(messages.value,[message]);if(follow)void nextTick(()=>{if(streamBody.value)streamBody.value.scrollTop=streamBody.value.scrollHeight})}
 if(event.kind==='flow_closed'&&detail.value){detail.value={...detail.value,closed:event,state:'Closed',bytesUp:String(event.bytes_client_to_server??''),bytesDown:String(event.bytes_server_to_client??'')};void load()}
}
function resync(){if(usingWs.value){props.wsSource?.onRefresh?.();return}if(loading.value){dirty=true;return}void load()}
defineExpose({receive,resync})
async function load(more=false) {
 if(usingWs.value)return
 const current=++request;loading.value=true;error.value=''
 try {
  if(more&&messages.value.length>=500)return
  const flow=await api<Record<string,unknown>>(path)
  if(disposed)return
  const received=flowRows({items:[flow],next_cursor:null})[0]
  if(!detail.value?.closed||received?.closed)detail.value=received
  void loadHistory()
  const page=await api<Page>(`${path}/messages?limit=100&include_blocked=${props.showBlocked===true}&newest_first=true${more&&cursor.value?'&after='+cursor.value:''}`)
  if(disposed||current!==request)return
  const initial=!messages.value.length
  messages.value=mergeMessages(messages.value,messageRows(page));cursor.value=page.next_cursor
  if(initial)void nextTick(()=>{if(streamBody.value)streamBody.value.scrollTop=streamBody.value.scrollHeight})
 }catch(e){if(!disposed&&current===request)error.value=errorMessage(e)}
 finally{if(current===request){loading.value=false;if(dirty&&!disposed){dirty=false;void load()}}}
}
async function loadHistory() {
 if(usingWs.value)return
 if(historyState.value==='loading')return
 historyState.value='loading'
 try {history.value=await api<FlowHistory>(`${path}/history`);historyState.value='ready'}
 catch(e) {historyState.value=e instanceof ApiError&&e.status===404?'missing':e instanceof ApiError&&e.status===409?'active':'unavailable'}
}
function compare(message:Message) {
 if(!selected.value){selected.value=message;comparison.value=[];comparisonTitle.value='';return}
 comparisonTitle.value=`${selected.value.message_id} ↔ ${message.message_id}`
 comparison.value=differences(selected.value,message);selected.value=undefined
}
function createFilter(message:Message,field:string) {
 openEditor('Create filter from saved field',{message_id:message.message_id,field,name:'Saved '+field,action:{type:'alert'}},async value=>{
  const filters=await api<ListResponse>('/filters')
  await api('/filters/from-message','POST',{...value,expected_revision:filters.revision})
  notice.value='Filter saved in Enforce mode. Check the applied revision in Filters.'
 })
}
onMounted(()=>{if(!usingWs.value)void load()})
onUnmounted(()=>{disposed=true;request++})
</script>
<template>
 <section id="traffic-stream" aria-label="Session stream" class="@container flex min-h-0 min-w-0 flex-col">
  <header class="space-y-3 border-b p-4">
   <div class="flex items-start justify-between gap-3"><div class="min-w-0"><h2 class="!m-0 text-base">{{udp?'Follow UDP conversation':'Follow TCP stream'}}</h2><p class="truncate font-mono text-xs text-muted-foreground">{{detail?.client}} ↔ {{detail?.destination}}</p></div><div class="flex flex-wrap justify-end gap-1"><FlowFavoriteButton v-if="detail" :flow-id="flowId" :favorite="detail.favorite" @changed="favoriteChanged" @error="error=$event" /><Button variant="ghost" size="sm" :disabled="loading" @click="refresh">Refresh</Button></div></div>
   <p v-if="closureReason" role="status" class="text-xs text-muted-foreground">Connection ended: {{closureReason}}</p>
   <p v-if="completedCaptureForOpenFlow" role="status" class="text-xs text-muted-foreground">Proxy has a saved capture; Core still lists this flow as open.</p>
   <p v-if="decoderWarning" role="status" class="text-xs text-muted-foreground">{{decoderWarning}} <Button size="sm" variant="link" class="h-auto p-0" @click="mode='bytes'">Captured bytes</Button></p>
   <p v-if="detail?.decoded_incomplete&&!detail.favorite" role="status" class="text-xs text-muted-foreground">Decoded history is partial; older data was rotated at this service’s retention limit.</p>
   <p v-if="detail?.favorite" class="text-xs text-muted-foreground">Saved to favourites; ordinary traffic rotation does not remove this stream.<span v-if="detail?.raw_history_state==='pending'"> Capture is pending completion; the saved snapshot is retained.</span><span v-if="detail?.raw_history_state==='partial'"> The connection was interrupted; its saved snapshot is partial and remains available under Captured bytes.</span><span v-if="detail?.raw_history_state==='unavailable'"> Captured bytes were unavailable when saved.</span><span v-if="detail?.decoded_incomplete"> Decoded history is incomplete because a capture limit was reached.</span></p>
   <div class="flex flex-wrap items-center gap-2 text-xs text-muted-foreground"><span>{{detail?.service_id}}</span><span v-if="flow?.has_flag||detail?.has_flag" class="rounded border border-amber-500/60 bg-amber-500/10 px-2 py-1 font-medium text-amber-500">Flag detected</span><span v-if="flow?.blocked||detail?.blocked" class="text-destructive">Blocked</span><span>· {{detail?.state??'Loading'}}</span><span v-if="forwardedTotals">· {{forwardedTotals.bytesUp}} B sent / {{forwardedTotals.bytesDown}} B received</span><span>· {{messages.length}} {{rawSummariesOnly?'raw metadata records':'decoded messages'}}</span></div>
   <div v-if="liveCapture||wsPending||wsGap||wsHistoryNotice||(captureBounds&&mode!=='bytes')" class="space-y-1" role="status">
    <p v-if="liveCapture&&!wsPending" class="text-xs text-muted-foreground">Live capture (partial, bounded). This is the retained tail for the selected direction, not a complete packet record.</p>
    <p v-if="wsPending" class="text-xs text-muted-foreground">The Proxy is still saving this closed flow's history. Retrying over the live connection…</p>
    <p v-if="liveCapture&&!wsPending&&wsHistory" class="text-xs text-muted-foreground">Retained tail {{wsHistory.retained_client_to_server_bytes}} B client → server / {{wsHistory.retained_server_to_client_bytes}} B server → client (not forwarded totals).<span v-if="captureTruncated"> A direction tail was truncated at the capture bound.</span></p>
    <p v-if="wsGap" class="text-xs text-amber-500">Earlier captured bytes were evicted from the bounded tail{{wsGapBytes?' ('+wsGapBytes+' bytes newly dropped)':''}}. Rendering resumes at the retained head.</p>
    <p v-if="wsHistoryNotice" role="status" class="text-xs text-amber-500">{{wsHistoryNotice}}</p>
    <p v-if="captureBounds&&!liveCapture&&!wsPending&&mode!=='bytes'" class="text-xs text-muted-foreground">{{captureBounds?.retained??0}} retained bytes<span v-if="captureBounds?.forwarded"> · {{captureBounds?.c2s??0}} B client → server / {{captureBounds?.s2c??0}} B server → client</span><span v-if="captureBounds?.truncated"> · capture truncated; only the bounded tail is retained</span></p>
   </div>
   <div class="flex flex-wrap gap-2"><Select v-model="mode"><SelectTrigger aria-label="Stream view" class="w-48"><SelectValue /></SelectTrigger><SelectContent><SelectItem value="decoded">Decoded conversation</SelectItem><SelectItem value="bytes">Captured bytes</SelectItem><SelectItem value="terminal">Stream · terminal</SelectItem></SelectContent></Select><Select v-if="mode==='decoded'||mode==='terminal'||savedStream" v-model="side"><SelectTrigger aria-label="Stream direction" class="w-48"><SelectValue /></SelectTrigger><SelectContent><SelectItem value="both">Both directions</SelectItem><SelectItem value="1">Client → server</SelectItem><SelectItem value="2">Server → client</SelectItem></SelectContent></Select><Select v-if="mode==='bytes'&&savedStream" v-model="byteMode"><SelectTrigger aria-label="Raw byte format" class="w-36"><SelectValue /></SelectTrigger><SelectContent><SelectItem value="hex">Hex dump</SelectItem><SelectItem value="utf8">UTF-8</SelectItem><SelectItem value="ascii">ASCII</SelectItem></SelectContent></Select></div>
   <p class="truncate text-[10px] text-muted-foreground" :title="flowId">Session {{flowId}} · {{detail?.started}}</p>
  </header>
  <div ref="streamBody" class="min-h-0 flex-1" :class="mode==='terminal'?'flex flex-col gap-3 overflow-hidden p-3':'space-y-3 overflow-auto p-4'">
   <Alert v-if="error" variant="destructive"><AlertDescription>{{error}}</AlertDescription></Alert>
   <Alert v-if="notice"><AlertDescription>{{notice}}</AlertDescription></Alert>
   <Alert v-if="tlsUndecoded" data-tls-prompt>
    <AlertDescription>
     This stream starts with a TLS handshake, so the proxy forwards it unread. A certificate cannot decrypt a session that was already negotiated.
     Add a certificate to this service and connections opened after saving are read as HTTP.
     <Button size="sm" variant="outline" class="mt-2" @click="emit('configure-tls')">Add certificate</Button>
    </AlertDescription>
   </Alert>
   <template v-if="mode==='terminal'">
    <p v-if="(loading||historyState==='loading'||wsLoading)&&!terminalTranscript.chunks.length" role="status">Loading terminal stream…</p>
    <p v-if="terminalLegacyOrder" class="text-sm text-muted-foreground">This older capture saved separate direction buffers without conversation order. Select Client or Server to inspect it in a terminal.</p>
    <p v-else-if="!loading&&!wsLoading&&historyState!=='loading'&&!terminalTranscript.chunks.length" class="text-sm text-muted-foreground">{{historyState==='ready'&&history?.segments.length===0?'The saved capture contains no payload bytes.':savedStream?'No readable text is available in the saved capture. Use Captured bytes to inspect it.':protocolId==='utf8'?'Waiting for captured UTF-8 text from either direction…':'Live terminal playback requires the UTF-8 decoder. Saved bytes can be replayed after the session closes.'}}</p>
    <p v-if="history?.history_truncated||detail?.decoded_incomplete" class="text-xs text-muted-foreground">Capture history is incomplete; only retained text can be replayed.</p>
    <TerminalStream :chunks="terminalTranscript.chunks" :truncated="terminalTranscript.truncated" :flag-pattern="flagPattern" />
   </template>
   <FlowCapture v-if="mode==='bytes'&&!savedStream&&!usingWs" :flow-id="flowId" :live="true" :auto-capture="rawProfile" />
   <section v-if="mode==='bytes'&&savedStream" class="space-y-3">
    <p v-if="historyState==='loading'" role="status" class="text-sm text-muted-foreground">Loading saved stream history…</p>
    <Alert v-else-if="historyState==='missing'" :variant="closureHistoryLost?'destructive':'default'"><AlertDescription>{{closureHistoryLost?'No history record was saved; capture capacity or storage limits caused history loss.':'No saved history is available. Retention may have been disabled, or the record may have expired or been evicted.'}}</AlertDescription></Alert>
    <Alert v-else-if="historyState==='active'"><AlertDescription>The stream is still active; refresh after it closes to load its saved history.</AlertDescription></Alert>
    <Alert v-else-if="historyState==='unavailable'" variant="destructive"><AlertDescription>Saved stream history is temporarily unavailable. Refresh to retry.</AlertDescription></Alert>
    <template v-if="history">
     <p class="text-xs text-muted-foreground">{{captureBounds?.retained??0}} retained bytes<span v-if="udp"> in {{history.segments.length}} datagrams</span><template v-if="captureBounds?.forwarded"> · {{captureBounds?.c2s??0}} bytes forwarded client → server / {{captureBounds?.s2c??0}} server → client</template><span v-else-if="!liveCapture||wsPending"> · Final forwarded totals are unavailable for this snapshot.</span></p>
     <p class="text-xs text-muted-foreground">Captured before rule enforcement; retained bytes may include blocked traffic. Forwarded byte totals exclude blocked traffic.</p>
     <Alert v-if="history.history_truncated||history.truncated_client_to_server||history.truncated_server_to_client"><AlertDescription>History is truncated. Only the latest bounded bytes are retained; some data may have been lost.</AlertDescription></Alert>
     <p v-if="!history.segments.length&&!wsPending" class="text-sm text-muted-foreground">No payload bytes were retained for this capture.</p>
     <article v-for="(segment,index) in (byteMode==='utf8'?historyUtf8Segments(history,side):historySegments(history,side))" :key="index"  :data-direction="segment.direction" class="space-y-1 rounded-sm border-l-2 p-3 @3xl:w-[78%]" :class="segment.direction===1?'border-red-400/60 bg-red-500/5 @3xl:mr-auto':'border-blue-400/60 bg-blue-500/5 @3xl:ml-auto'">
      <div class="text-[11px] text-muted-foreground">{{direction(segment.direction)}} · {{segment.data.length}} bytes<span v-if="segment.datagram"> · datagram #{{segment.sequence}} · {{timestamp(segment.occurred_at_ms)}}</span><span v-else-if="Number(segment.sequence)>0"> · chunk #{{segment.sequence}}</span><span v-if="segment.truncated"> · direction tail truncated</span></div>
      <pre class="!max-h-none !rounded-none !border-0 !bg-transparent !p-0 font-mono !text-[13px] !leading-6">{{segment.previewText!==undefined?segment.previewText:(historyPreview(segment,byteMode)||'(empty)')}}</pre>
     </article>
    </template>
   </section>
   <template v-if="mode==='decoded'">
    <p v-if="messages.length>=500" class="text-xs text-muted-foreground">Showing the latest 500 decoded messages in this session.</p>
    <p class="text-xs text-muted-foreground">Decoded fields in session order. Client traffic is red; server traffic is blue. Byte samples are available under Captured bytes.</p>
    <p v-if="(loading||wsLoading)&&!messages.length" role="status">Loading conversation…</p>
    <template v-if="hasHistoryTextFallback&&history">
     <p class="text-xs text-muted-foreground">No parser messages were retained. Showing saved bytes as UTF-8. <span v-if="udp">UDP datagrams retain their boundaries and arrival order.</span><span v-else-if="orderedTcpHistory">TCP read chunks retain capture order.</span><span v-else>Older TCP captures did not retain cross-direction order, so their bytes are grouped by direction.</span> Switch to Captured bytes for the default hex view.</p>
     <article v-for="(segment,index) in historyTextSegments(history,side)" :key="index"  :data-direction="segment.direction" class="space-y-1 rounded-sm border-l-2 p-3 @3xl:w-[78%]" :class="segment.direction===1?'border-red-400/60 bg-red-500/5 @3xl:mr-auto':'border-blue-400/60 bg-blue-500/5 @3xl:ml-auto'">
      <div class="text-[11px] text-muted-foreground">{{direction(segment.direction)}} · {{segment.data.length}} retained bytes<span v-if="segment.datagram"> · datagram #{{segment.sequence}} · {{timestamp(segment.occurred_at_ms)}}</span><span v-else-if="Number(segment.sequence)>0"> · chunk #{{segment.sequence}}</span><span v-if="segment.truncated"> · direction tail truncated</span></div>
      <pre class="!max-h-none !rounded-none !border-0 !bg-transparent !p-0 font-mono !text-[13px] !leading-6">{{segment.previewText!==undefined?segment.previewText:(historyPreview(segment,'utf8')||'(empty)')}}</pre>
     </article>
    </template>
    <details v-if="hasHistoryTextFallback&&rawSummaryMessages.length" class="rounded-sm border p-3">
     <summary class="cursor-pointer text-xs font-medium">Raw metadata · {{rawSummaryMessages.length}} read summaries</summary>
     <article v-for="message in rawSummaryMessages" :key="message.message_id" class="mt-3 space-y-1">
      <div class="text-[11px] text-muted-foreground">{{direction(message.direction)}} · {{timestamp(message.occurred_ms)}}</div>
      <DataTable :columns="[{key:'key',label:'Field'},{key:'type',label:'Type'},{key:'value',label:'Value'}]" :rows="fields(message)" />
     </article>
    </details>
    <p v-else-if="!loading&&!wsLoading&&!messages.length" class="py-8 text-sm text-muted-foreground">No decoded messages were retained for this session. Unknown and encrypted protocols still have session metadata. Closed conversations appear here as UTF-8 when saved history is available; use Captured bytes for hex or to inspect the next chunk on a live session.</p>
    <article v-for="message in decodedMessages" :key="message.message_id" :data-message-id="message.message_id" :data-direction="message.direction" class="rounded-sm border-l-2 p-3 @3xl:w-[78%]" :class="message.direction===1?'border-red-400/60 bg-red-500/5 @3xl:mr-auto':message.direction===2?'border-blue-400/60 bg-blue-500/5 @3xl:ml-auto':'border-border'">
     <div class="mb-1 flex flex-wrap items-center gap-2 text-[11px] text-muted-foreground"><span :class="message.direction===1?'text-red-300':message.direction===2?'text-blue-300':''">{{direction(message.direction)}}</span><span>{{timestamp(message.occurred_ms)}}</span><span>{{message.message_type||message.protocol_id}}</span><span v-if="message.truncated">Truncated</span></div>
     <HttpMessage v-if="isHttpMessage(message)" :message="message" @bytes="mode='bytes'" />
     <pre v-else class="!max-h-none !rounded-none !border-0 !bg-transparent !p-0 font-mono !text-[13px] !leading-6"><template v-for="(part,index) in messageSegments(message)" :key="index"><mark v-if="part.flag" class="rounded-sm bg-amber-500/25 px-0.5 text-inherit">{{part.text}}</mark><template v-else>{{part.text}}</template></template></pre>
     <div class="flex gap-2"><Button size="sm" variant="ghost" @click="inspect=inspect?.message_id===message.message_id?undefined:message">{{inspect?.message_id===message.message_id?'Hide fields':'Inspect fields'}}</Button><Button size="sm" variant="ghost" @click="compare(message)">Compare</Button></div>
     <DataTable v-if="inspect?.message_id===message.message_id" :columns="[{key:'key',label:'Field'},{key:'type',label:'Type'},{key:'value',label:'Value'},{key:'filter',label:'Filter'}]" :rows="fields(message)"><template #cell-filter="{row}"><Button size="sm" variant="outline" @click="createFilter(message,row.key)">Create filter</Button></template></DataTable>
    </article>
    <p v-if="selected" class="text-xs text-muted-foreground">Choose another message to compare with {{selected.message_id}}.</p>
    <section v-if="comparisonTitle"><h3 class="text-xs">{{comparisonTitle}}</h3><DataTable :columns="[{key:'key',label:'Field'},{key:'left',label:'Selected'},{key:'right',label:'Current'},{key:'change',label:'Difference'}]" :rows="comparison" /></section>
    <Button v-if="cursor" variant="outline" :disabled="loading||messages.length>=500" @click="moreMessages">More messages</Button>
    <p v-if="messages.length>=500" class="text-xs text-muted-foreground">500-message preview limit reached.</p>
   </template>
  </div>
 </section>
</template>
