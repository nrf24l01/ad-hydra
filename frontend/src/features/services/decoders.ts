import type { ServiceInput } from '@/generated/contracts'
import { api, activateProtocol } from '@/shared/api/client'
export type DecoderVersion = { id:string; version:string; state:string; error?:string|null; profile?:unknown }
export type DecoderActivation = { service_id:string; id:string; version:string; applied:boolean; error?:string|null }
export type DecoderCatalog = { items:DecoderVersion[]; activations:DecoderActivation[] }
export const builtins = [{value:'raw',label:'Raw bytes (hex)'},{value:'utf8',label:'UTF-8 text'},{value:'http',label:'HTTP/1.1'},{value:'grpc',label:'gRPC over HTTP/2'}]
export function decoderValue(id:string,version:string):string { return `custom:${id}/${version}` }
export function decoderChoices(catalog:DecoderCatalog) {
 return [...builtins,...catalog.items.filter(item=>item.state==='installed').map(item=>({value:decoderValue(item.id,item.version),label:`${item.id} / ${item.version} · ${item.profile?'framing':'WASM'}`}))]
}
export function selectedDecoder(service:{id:string;protocol_id:string},catalog:DecoderCatalog):string {
 const active=catalog.activations.find(item=>item.service_id===service.id)
 return active?decoderValue(active.id,active.version):service.protocol_id
}
export function decoderLabel(service:{id:string;protocol_id:string},catalog:DecoderCatalog):string {
 const value=selectedDecoder(service,catalog),active=catalog.activations.find(item=>item.service_id===service.id)
 const label=decoderChoices(catalog).find(item=>item.value===value)?.label??value
 return active&&!active.applied?`${label} · ${active.error||'pending'}`:label
}
type Saved = { id?:string; services:{revision:number;services:({id:string}&Omit<ServiceInput,'expected_revision'>)[]} }
export async function saveWithDecoder(input:ServiceInput,id:string|undefined,choice:string,catalog:DecoderCatalog,onSaved:(id:string,revision:number)=>void) {
 const custom=catalog.items.find(item=>decoderValue(item.id,item.version)===choice&&item.state==='installed')
 if(!custom&&!builtins.some(item=>item.value===choice))throw new Error('Select an installed decoder from the list')
 const saved=await api<Saved>(`/services${id?'/'+encodeURIComponent(id):''}`,id?'PATCH':'POST',{...input,protocol_id:custom?'raw':choice})
 const service=saved.services.services.find(item=>item.id===(id??saved.id))
 if(!service)throw new Error('Service saved; refresh Services to check its state')
 onSaved(service.id,saved.services.revision)
 try {
 const current=await api<DecoderCatalog>('/protocols')
 const active=current.activations.find(item=>item.service_id===service.id)
 if(custom&&active?.id===custom.id&&active.version===custom.version&&active.applied)return
 if(!custom&&!active)return
  // A new listener must exist in Proxy before its decoder can be activated.
  for(let attempt=0;attempt<20;attempt++) {
   const status=await api<{applied_revision:number;apply_error:string|null}>('/services')
   if(status.apply_error)throw new Error(status.apply_error)
   if(status.applied_revision>=saved.services.revision)break
   if(attempt===19)throw new Error('Listener configuration is still pending')
   await new Promise(resolve=>setTimeout(resolve,250))
  }
  const result=custom?await activateProtocol(custom.id,custom.version,service.id):await api<{applied:boolean;error?:string|null}>(`/protocols/activations/${encodeURIComponent(service.id)}`,'DELETE',undefined,true,{'Idempotency-Key':crypto.randomUUID()})
  if(!result.applied)throw new Error(result.error||'Decoder activation is pending')
 }catch(cause){throw new Error(`Service saved; decoder was not confirmed. Retry Save service or refresh to check its state. ${cause instanceof Error?cause.message:''}`)}
}
