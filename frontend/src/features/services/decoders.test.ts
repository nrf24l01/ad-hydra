import {expect,test} from 'bun:test'
import {decoderChoices,decoderLabel,selectedDecoder,type DecoderCatalog} from './decoders'
const catalog:DecoderCatalog={items:[{id:'crypter',version:'v1',state:'installed'},{id:'broken',version:'v1',state:'error'},{id:'framed',version:'2',state:'installed',profile:{}}],activations:[{service_id:'svc',id:'crypter',version:'v1',applied:true}]}
test('only installed modules join built-ins and framing in the service decoder choices',()=>{
 expect(decoderChoices(catalog).map(item=>item.value)).toEqual(['raw','utf8','http','grpc','custom:crypter/v1','custom:framed/2'])
})
test('service editor and table show activated module instead of native fallback',()=>{
 const service={id:'svc',protocol_id:'raw'}
 expect(selectedDecoder(service,catalog)).toBe('custom:crypter/v1')
 expect(decoderLabel(service,catalog)).toBe('crypter / v1 · WASM')
 expect(selectedDecoder({id:'other',protocol_id:'http'},catalog)).toBe('http')
 expect(decoderLabel(service,{...catalog,activations:[{...catalog.activations[0]!,applied:false,error:'not acknowledged'}]})).toContain('not acknowledged')
})
