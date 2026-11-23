<script setup lang="ts">
import { computed,onMounted,onUnmounted,ref,watch } from 'vue'
import type { ListResponse } from '@/generated/contracts'
import { api,errorMessage } from '@/shared/api/client'
import { Button } from '@/components/ui/button'
import { Alert,AlertDescription } from '@/components/ui/alert'
import TrafficWorkspace from './TrafficWorkspace.vue'
import { useScrollLock } from '@vueuse/core'
import TrafficQuery from './TrafficQuery.vue'
import FlowConversation from './FlowConversation.vue'
import SessionList from './SessionList.vue'
import GlobalSearch from './GlobalSearch.vue'
import { flowRows,mergeFlows,messageRows,visibleFlows,text,serviceProtocol,type Flow,type Message } from './traffic'
import { eventMessage,liveEvent,mergeMessages,updateFlows } from './live'
import { applyActiveHistory,openLive,type LiveQuery,type LiveSocket,type LiveState,type SearchPage,type SearchItem,type SearchScope,type ServerFrame,type WorkspaceHistory,type WorkspaceStreamSource } from './liveSocket'
import { useRouter } from 'vue-router'
import { toggleStream, type StreamSelection } from './streamSelection'
import type { FavoriteChange } from './FlowFavoriteButton.vue'
import { captureHistoryError } from './historyErrors'
const rows=ref<Flow[]>([]),services=ref<{id:string;name:string;protocol_id:string;color?:string|null;tls?:boolean}[]>([]),service=ref('all'),selected=ref<Flow>()
const cursor=ref<number|null>(null),query=ref(''),loading=ref(true),error=ref(''),serviceError=ref(''),advanced=ref(false)
const flagPattern=ref(''),liveState=ref<LiveState>('connecting'),liveNotice=ref(''),counters=ref('')
const router=useRouter(),filterSelection=ref<StreamSelection|null>(null)
const wsMessages=ref<Message[]>([]),wsHistory=ref<WorkspaceHistory>(),wsMessageCursor=ref<number|null>(null),wsSelectionStatus=ref<WorkspaceStreamSource['status']>('idle')
const searchOpen=ref(false),searchResult=ref<SearchPage>(),searchLoading=ref(false),searchError=ref('')
const pendingSelect=ref<SearchItem>(),selectionError=ref('')
const wsHistoryError=ref('')
const selectedProtocol=computed(()=>services.value.find(s=>s.id===selected.value?.service_id)?.protocol_id??'raw')
const selectedServiceTls=computed(()=>services.value.find(s=>s.id===selected.value?.service_id)?.tls===true)
function configureTls(){const id=selected.value?.service_id;if(id)void router.push({path:'/services',query:{tls:id}})}
const expanded=ref(false)
const scrollLocked=useScrollLock(document.body)
watch(expanded,value=>{scrollLocked.value=value})
function escapeExpanded(event:KeyboardEvent){if(event.key==='Escape'&&!event.defaultPrevented&&!document.querySelector('[role=dialog],[role=listbox]'))expanded.value=false}
const showBlocked=ref(false)
const favoritesOnly=ref(false)
const visible=computed(()=>visibleFlows(rows.value,showBlocked.value))
let live:LiveSocket|undefined,disposed=false
let searchRequest:{q:string;scope:SearchScope;service_id:string|null}|null=null,autoContinues=0
let selectionTimer:ReturnType<typeof setTimeout>|undefined
const wsSource=computed<WorkspaceStreamSource|undefined>(()=>selected.value?{flow:selected.value,messages:wsMessages.value,history:wsHistory.value,status:wsSelectionStatus.value,historyNotice:wsHistoryError.value||undefined,messageCursor:wsMessageCursor.value,onMore:()=>loadMessages(true),onRefresh:()=>reopenSelected()}:undefined)
function liveQuery():LiveQuery {
 const parameters=new URLSearchParams(query.value)
 const integer=(key:string)=>{const raw=parameters.get(key);if(raw===null||raw==='')return null;const value=Number(raw);return Number.isFinite(value)?value:null}
 const string=(key:string)=>parameters.get(key)||null
 const flag=(key:string)=>{const raw=parameters.get(key);return raw===null||raw===''?null:raw==='true'||raw==='1'}
 return {
  service_id:service.value==='all'?null:service.value,
  favorite:favoritesOnly.value?true:null,
  include_blocked:showBlocked.value,
  newest_first:parameters.get('newest_first')!=='false',
  limit:100,
  client_ip:string('client_ip'),
  since_ms:integer('since_ms'),
  until_ms:integer('until_ms'),
  transport:integer('transport'),
  source_ip:string('source_ip'),
  destination_ip:string('destination_ip'),
  direction:integer('direction'),
  field_path:string('field_path'),
  field_value:string('field_value'),
  matched_rule:string('matched_rule'),
  has_flag:flag('has_flag'),
 }
}
function clearSelectionTimer(){if(selectionTimer!==undefined){clearTimeout(selectionTimer);selectionTimer=undefined}}
function startSelectionTimer(flowId:string){
 clearSelectionTimer()
 selectionTimer=setTimeout(()=>{
  if(pendingSelect.value?.flow_id!==flowId)return
  selectionError.value='Flow metadata did not arrive over the live connection. The result may have expired or the connection may be recovering.'
  liveNotice.value=selectionError.value
  wsSelectionStatus.value='unavailable'
 },6000)
}
function subscribe(){
 clearSelectionTimer();pendingSelect.value=undefined;selectionError.value='';wsHistoryError.value=''
 live?.select(null,false);rows.value=[];cursor.value=null;selected.value=undefined;wsMessages.value=[];wsHistory.value=undefined;wsMessageCursor.value=null;wsSelectionStatus.value='idle';loading.value=true;live?.subscribe(liveQuery())
}
function loadMore(){if(cursor.value!==null){loading.value=true;live?.page(cursor.value)}}
function selectFlow(flow:Flow){
 clearSelectionTimer();pendingSelect.value=undefined;selectionError.value='';wsHistoryError.value=''
 selected.value=flow;wsMessages.value=[];wsHistory.value=undefined;wsMessageCursor.value=null;wsSelectionStatus.value='loading'
 live?.select(flow.flow_id,true);live?.messages(flow.flow_id)
}
function loadMessages(more=false){const flow=selected.value;if(!flow)return;if(more&&wsMessageCursor.value===null)return;wsSelectionStatus.value='loading';live?.messages(flow.flow_id,more?wsMessageCursor.value??undefined:undefined)}
function reopenSelected(){const flow=selected.value;if(!flow)return;wsHistoryError.value='';wsMessages.value=[];wsHistory.value=undefined;wsMessageCursor.value=null;wsSelectionStatus.value='loading';live?.select(flow.flow_id,true);live?.messages(flow.flow_id)}
function selectVisible() {
 if(pendingSelect.value)return
 if(selected.value){const match=visible.value.find(flow=>flow.flow_id===selected.value?.flow_id);if(match)selected.value=match;else{selected.value=undefined;wsMessages.value=[];wsHistory.value=undefined;wsHistoryError.value='';wsSelectionStatus.value='idle'}}
 if(!selected.value&&visible.value[0])selectFlow(visible.value[0])
}
function applySelectedFlow(item:Record<string,unknown>){
 const flow=flowRows({items:[item],next_cursor:null})[0];if(!flow)return
 rows.value=mergeFlows(rows.value,[flow])
 if(pendingSelect.value?.flow_id===flow.flow_id){clearSelectionTimer();pendingSelect.value=undefined;selectionError.value='';selected.value=flow;return}
 if(selected.value?.flow_id===flow.flow_id)selected.value={...selected.value,...flow}
}
function readEvent(raw:Record<string,unknown>):ReturnType<typeof liveEvent>{
 try{return liveEvent(raw)}catch{liveNotice.value='A live event could not be read.';return undefined}
}
function applyFlowEvent(raw:Record<string,unknown>){
 const event=readEvent(raw);if(!event)return
 if(service.value!=='all'&&event.service_id!==service.value)return
 rows.value=updateFlows(rows.value,event).filter(flow=>!favoritesOnly.value||flow.favorite)
 selectVisible()
 if(event.flow_id===selected.value?.flow_id){const match=rows.value.find(flow=>flow.flow_id===event.flow_id);if(match)selected.value=match}
}
function onFrame(frame:ServerFrame){
 switch(frame.type){
  case 'snapshot':
   rows.value=mergeFlows([],flowRows({items:frame.items,next_cursor:null}))
   cursor.value=frame.next_cursor??null;loading.value=false;selectVisible();break
  case 'flows_page':
   rows.value=mergeFlows(rows.value,flowRows({items:frame.items,next_cursor:null}),true)
   cursor.value=frame.next_cursor??null;loading.value=false;selectVisible();break
  case 'flow':
   if(frame.flow&&!Object.keys(frame.event).length)applySelectedFlow(frame.flow)
   else applyFlowEvent(frame.event)
   break
  case 'message':{
   if(frame.flow_id&&frame.flow_id!==selected.value?.flow_id&&frame.flow_id!==pendingSelect.value?.flow_id)break
   const event=readEvent(frame.event)
   if(!event)break
   const message=eventMessage(event)
   if(message){
    if(frame.display_text!==undefined)message.display_text=frame.display_text
    if(frame.flag_spans!==undefined)message.flag_spans=frame.flag_spans
    wsMessages.value=mergeMessages(wsMessages.value,[message])
   }
   break
  }
  case 'messages_page':
   if(frame.flow_id&&frame.flow_id!==selected.value?.flow_id&&frame.flow_id!==pendingSelect.value?.flow_id)break
   wsMessages.value=mergeMessages(wsMessages.value,messageRows({items:frame.items,next_cursor:null}))
   wsMessageCursor.value=frame.next_cursor??null;wsSelectionStatus.value='ready';break
  case 'active_bytes':
   if(frame.snapshot.flow_id!==selected.value?.flow_id&&frame.snapshot.flow_id!==pendingSelect.value?.flow_id)break
   wsHistoryError.value=''
   wsHistory.value=applyActiveHistory(wsHistory.value,frame.snapshot);wsSelectionStatus.value=frame.snapshot.pending===true?'loading':'ready';break
  case 'selected':{
   const flow=frame.flow
   const target=frame.flow_id??(flow?text(flow.flow_id):'')
   if(pendingSelect.value&&target&&pendingSelect.value.flow_id!==target)break
   if(!pendingSelect.value&&selected.value&&target&&selected.value.flow_id!==target)break
   if(flow&&Object.keys(flow).length)applySelectedFlow(flow)
   break
  }
  case 'search_page':
   if(!searchRequest)break
   {
    const previous=searchResult.value?.items??[]
    const seen=new Set(previous.map(item=>item.kind+':'+(item.message_id??item.flow_id)))
    const items=[...previous]
    for(const item of frame.items){const key=item.kind+':'+(item.message_id??item.flow_id);if(!seen.has(key)){seen.add(key);items.push(item)}}
    searchResult.value={items:items.slice(0,500),next_cursor:frame.next_cursor,coverage:frame.coverage,took_ms:frame.took_ms,request_id:frame.request_id}
    if(frame.coverage.continuation&&frame.next_cursor&&autoContinues<3){autoContinues++;live?.search({...searchRequest,after:frame.next_cursor,limit:50});break}
    searchLoading.value=false
   }
   break
  case 'counters':counters.value=`${frame.flows_open??0} open · ${frame.flows_seen??0} seen · ${frame.messages_seen??0} messages`;break
  case 'loss':liveNotice.value='Some live events were lost. Resyncing retained traffic.';break
  case 'error':{
   // Every Core error frame carries the same generic `message`, so the `code`
   // names the phase. Terminal history codes end the selection as unavailable;
   // the flow record, its metadata and loaded decoded messages stay valid.
   const history=captureHistoryError(frame.code,frame.request_id)
   if(history){wsHistoryError.value=history.notice;wsSelectionStatus.value=history.status;liveNotice.value=history.notice;break}
   const message=frame.message||'Live traffic request failed.'
   const request=frame.request_id?` (request ${frame.request_id})`:''
   if(frame.code==='flow_unavailable'){clearSelectionTimer();selectionError.value=`Flow metadata is unavailable; the result may have expired or the live connection may be recovering${request}.`;wsSelectionStatus.value='unavailable';liveNotice.value=selectionError.value;break}
   if(frame.code?.startsWith('search')||frame.code==='invalid_query'){searchLoading.value=false;searchError.value=message;break}
   if(selected.value||pendingSelect.value){clearSelectionTimer();selectionError.value=message;wsSelectionStatus.value='unavailable';liveNotice.value=message;break}
   if(loading.value)loading.value=false
   liveNotice.value=message
   break
  }
 }
}
function onState(state:LiveState){
 liveState.value=state
 if(state==='live'){liveNotice.value='';if(!document.hidden&&selected.value&&wsMessages.value.length===0&&wsSelectionStatus.value!=='loading')loadMessages()}
}
async function coreHealth(){try{await api('/system/settings');return true}catch{return false}}
function receiveSearch(value:{q:string;scope:SearchScope;service_id:string|null}){
 searchRequest=value;autoContinues=0;searchLoading.value=true;searchError.value='';searchResult.value=undefined
 if(liveState.value!=='live'){searchLoading.value=false;searchError.value='The live search connection is not ready yet.';return}
 live?.search({...value,after:null,limit:50})
}
function clearSearch(){searchRequest=null;autoContinues=0;searchResult.value=undefined;searchLoading.value=false;searchError.value=''}
function moreSearch(){if(!searchRequest)return;const next=searchResult.value?.next_cursor;if(!next)return;searchLoading.value=true;live?.search({...searchRequest,after:next,limit:50})}
function openSearchResult(item:SearchItem){
 const existing=rows.value.find(flow=>flow.flow_id===item.flow_id)
 if(existing){selectFlow(existing);return}
 // Do not fabricate transport/endpoint/state for an out-of-page hit; wait for Core's `selected` metadata.
 pendingSelect.value=item;selectionError.value='';wsHistoryError.value='';selected.value=undefined;wsMessages.value=[];wsHistory.value=undefined;wsMessageCursor.value=null;wsSelectionStatus.value='loading'
 live?.select(item.flow_id,true);live?.messages(item.flow_id)
 startSelectionTimer(item.flow_id)
}
function retryPending(){if(pendingSelect.value)openSearchResult(pendingSelect.value)}
function search(value:string){query.value=value;selected.value=undefined;subscribe()}
function toggleFilter(flow:Flow,checked:boolean){filterSelection.value=toggleStream(filterSelection.value,flow,checked)}
function favoriteChanged(change:FavoriteChange){
 rows.value=rows.value.map(flow=>flow.flow_id===change.flow_id?{...flow,...change}:flow).filter(flow=>!favoritesOnly.value||flow.favorite)
 if(selected.value?.flow_id===change.flow_id)selected.value={...selected.value,...change}
 selectVisible()
}
function createFilterFromStreams(){if(!filterSelection.value)return;sessionStorage.setItem('hydra.filter.stream-selection',JSON.stringify(filterSelection.value));void router.push('/filters?createFromTraffic=1')}
watch(showBlocked,()=>{subscribe()})
watch(favoritesOnly,()=>{subscribe()})
watch(service,()=>{subscribe()})
function visibility(){if(document.hidden)live?.pause();else live?.resume()}
onMounted(async()=>{
 document.addEventListener('keydown',escapeExpanded);document.addEventListener('visibilitychange',visibility)
 try {
  let after=''
  do {
   const page=await api<ListResponse>('/services?limit=100'+(after?'&after='+encodeURIComponent(after):''))
   if(disposed)return
   services.value.push(...page.items.map(s=>({id:text(s.id),name:text(s.name)||text(s.id),protocol_id:text(s.protocol_id)||'raw',color:typeof s.color==='string'?s.color:null,tls:Boolean(s.tls)})))
   after=page.next_cursor??''
  }while(after&&services.value.length<128)
 }catch(e){serviceError.value=errorMessage(e)}
 if(disposed)return
 try {
  const catalog=await api<{activations:{service_id:string;id:string;version:string;applied:boolean}[]}>('/protocols')
  if(disposed)return
  services.value=services.value.map(item=>({...item,protocol_id:serviceProtocol(item,catalog.activations)}))
 }catch(e){serviceError.value=errorMessage(e)}
 try {
  const settings=await api<{flag_pattern?:string|null}>('/system/settings')
  if(!disposed&&typeof settings.flag_pattern==='string')flagPattern.value=settings.flag_pattern
 }catch(e){serviceError.value=errorMessage(e)}
 if(disposed)return
 live=openLive({query:liveQuery(),onFrame,onState,health:coreHealth,onNotice:notice=>{liveNotice.value=notice}})
})
onUnmounted(()=>{scrollLocked.value=false;clearSelectionTimer();document.removeEventListener('keydown',escapeExpanded);document.removeEventListener('visibilitychange',visibility);disposed=true;live?.close()})
</script>
<template>
 <section id="traffic-workspace" :class="expanded?'fixed inset-0 z-40 flex flex-col gap-3 bg-background p-3':'space-y-3'">
  <div class="flex items-center justify-between"><div><h1 class="!mb-0">Traffic</h1><p id="traffic-live-status" role="status" class="text-xs text-muted-foreground">{{liveState==='live'?'Live':liveState==='retrying'?'Reconnecting live traffic…':'Connecting live traffic…'}}<span v-if="counters"> · {{counters}}</span></p></div><div class="flex flex-wrap justify-end gap-2"><Button id="traffic-expand" variant="outline" size="sm" :aria-pressed="expanded" @click="expanded=!expanded">{{expanded?'Exit expanded view':'Expand traffic'}}</Button><Button variant="outline" size="sm" :aria-pressed="searchOpen" @click="searchOpen=!searchOpen">{{searchOpen?'Hide search':'Search all traffic'}}</Button><Button v-if="filterSelection" variant="outline" size="sm" @click="filterSelection=null">Clear {{filterSelection.flow_ids.length }} selected</Button><Button v-if="filterSelection" size="sm" @click="createFilterFromStreams">Create filter from {{filterSelection.flow_ids.length }} streams</Button><Button variant="ghost" size="sm" :aria-expanded="advanced" @click="advanced=!advanced">{{advanced?'Hide filters':'Filters'}}</Button></div></div>
  <p v-if="liveNotice" role="status" class="text-xs text-muted-foreground">{{liveNotice}}</p>
  <TrafficQuery v-if="advanced" @search="search" />
  <GlobalSearch v-if="searchOpen" :result="searchResult" :loading="searchLoading" :error="searchError" :services="services" @search="receiveSearch" @more="moreSearch" @select="openSearchResult" @clear="clearSearch" />
  <Alert v-if="error||serviceError" variant="destructive"><AlertDescription>{{error||serviceError}}</AlertDescription></Alert>
  <div class="overflow-hidden rounded-md border bg-card" :class="expanded?'min-h-0 flex-1':'h-[calc(100dvh-230px)] min-h-[640px] md:min-h-[480px]'"><TrafficWorkspace><template #sessions>
   <SessionList :rows="visible" :show-blocked="showBlocked" :favorites-only="favoritesOnly" @favorites-only="favoritesOnly=$event" @favorite="favoriteChanged" @error="error=$event" @show-blocked="showBlocked=$event" :services="services" :service="service" :selected="selected?.flow_id??''" :loading="loading" :more="cursor!==null" :filter-selection="filterSelection?.flow_ids??[]" :filter-selection-service="filterSelection?.service_id??''" @service="service=$event" @select="selectFlow" @toggle-filter="toggleFilter" @refresh="subscribe" @more="loadMore" />
   </template><FlowConversation class="flex-1" v-if="selected" :key="selected.flow_id+':'+selectedProtocol" :flow-id="selected.flow_id" :flow="selected" :protocol-id="selectedProtocol" :show-blocked="showBlocked" :flag-pattern="flagPattern" :service-tls="selectedServiceTls" :ws-source="wsSource" @configure-tls="configureTls" @favorite="favoriteChanged" />
   <section v-else-if="pendingSelect" id="traffic-stream" role="status" class="flex min-h-80 items-center justify-center p-8 text-center"><div><h2 class="!mt-0">{{selectionError?'Selected flow unavailable':'Loading selected flow…'}}</h2><p class="text-sm text-muted-foreground">{{selectionError||'Resolving this search result over the live connection.'}}</p><Button v-if="selectionError" class="mt-2" size="sm" variant="outline" @click="retryPending">Retry</Button></div></section>
   <section v-else id="traffic-stream" class="flex min-h-80 items-center justify-center p-8 text-center"><div><h2 class="!mt-0">Follow traffic session</h2><p class="text-sm text-muted-foreground">Choose a service and select a session to inspect its conversation.</p></div></section>
  </TrafficWorkspace></div>
 </section>
</template>
