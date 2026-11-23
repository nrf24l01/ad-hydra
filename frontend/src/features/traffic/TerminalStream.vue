<script setup lang="ts">
import {computed,onMounted,onUnmounted,ref,watch} from 'vue'
import {Terminal} from '@xterm/xterm'
import {FitAddon} from '@xterm/addon-fit'
import '@xterm/xterm/css/xterm.css'
import {Button} from '@/components/ui/button'
import type {TerminalChunk} from './terminal'
import {TranscriptWriter} from './terminalWriter'
import {useTheme} from '@/app/theme'
import {compileFlagPattern} from './flagPattern'
const props=defineProps<{chunks:TerminalChunk[];truncated?:boolean;flagPattern?:string}>()
const flag=computed(()=>compileFlagPattern(props.flagPattern))
const flagRegex=computed(()=>flag.value.regex)
const {theme}=useTheme()
const host=ref<HTMLElement>(),status=ref(''),following=ref(true)
let terminal:Terminal|undefined,fit:FitAddon|undefined,observer:ResizeObserver|undefined,frame:number|undefined,disposed=false
let writer:TranscriptWriter|undefined,rebuilding=false
function resize(){if(!host.value||host.value.clientWidth<30||host.value.clientHeight<40)return;const dimensions=fit?.proposeDimensions();if(dimensions&&terminal)terminal.resize(Math.min(400,Math.max(20,dimensions.cols)),Math.min(120,Math.max(4,dimensions.rows)))}
function render(){writer?.set(props.chunks)}
function follow(){following.value=true;terminal?.scrollToBottom()}
watch(()=>props.chunks,render)
watch(()=>props.flagPattern,render)
watch(theme,(value)=>{if(terminal)terminal.options.theme={background:value==='oled'?'#000000':'#151515',foreground:value==='oled'?'#ededed':'#e8e8e8'}})
onMounted(()=>{
 terminal=new Terminal({disableStdin:true,convertEol:true,cursorBlink:false,cursorInactiveStyle:'none',scrollback:2000,fontSize:13,fontFamily:'ui-monospace, SFMono-Regular, Menlo, monospace',screenReaderMode:true,theme:{background:theme.value==='oled'?'#000000':'#151515',foreground:theme.value==='oled'?'#ededed':'#e8e8e8'},linkHandler:{activate:()=>{}},allowProposedApi:false,altClickMovesCursor:false,windowOptions:{}})
 fit=new FitAddon();terminal.loadAddon(fit);terminal.open(host.value!);terminal.onScroll(()=>{if(terminal&&!rebuilding)following.value=terminal.buffer.active.viewportY>=terminal.buffer.active.baseY})
 let viewport:number|undefined
 writer=new TranscriptWriter({reset:()=>{if(!terminal)return;rebuilding=true;viewport=terminal.buffer.active.viewportY;const follow=following.value;terminal.reset();following.value=follow},write:(text,done)=>terminal?.write(text,done)},()=>{if(disposed)return;if(following.value)terminal?.scrollToBottom();else if(viewport!==undefined)terminal?.scrollToLine(viewport);viewport=undefined;rebuilding=false;status.value=`${props.chunks.length} captured text chunks`},()=>flagRegex.value)
 observer=new ResizeObserver(()=>{cancelAnimationFrame(frame??0);frame=requestAnimationFrame(resize)});observer.observe(host.value!);resize();render()
})
onUnmounted(()=>{disposed=true;observer?.disconnect();if(frame!==undefined)cancelAnimationFrame(frame);writer?.dispose();terminal?.dispose();terminal=undefined})
</script>
<template>
 <section class="flex min-h-0 flex-1 flex-col gap-2" aria-label="Read-only captured terminal">
  <div class="flex shrink-0 items-center justify-between gap-2 text-xs text-muted-foreground"><span>Client: red · Server: blue · Invalid UTF-8: yellow hex · {{flag.regex?'Flags highlighted':'Flag highlighting unavailable'}} · read-only</span><Button v-if="!following" size="sm" variant="outline" @click="follow">Follow output</Button></div>
  <p v-if="flag.status.includes('unsupported')" role="status" class="text-xs text-amber-500">{{flag.status}}</p>
  <p v-if="truncated" role="status" class="text-xs text-muted-foreground">The terminal preview is bounded; older text has been omitted.</p>
  <div ref="host" id="traffic-terminal" class="min-h-[240px] min-w-0 flex-1 overflow-hidden rounded-md border bg-background p-2" aria-label="Terminal stream"/>
  <span class="sr-only" role="status">{{status}}</span>
 </section>
</template>
<style scoped>
:deep(.xterm){height:100%}
:deep(.xterm-viewport){overscroll-behavior:contain}
</style>
