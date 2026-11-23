import type {TerminalChunk} from './terminal'
const CLIENT='\x1b[38;2;248;113;113m',SERVER='\x1b[38;2;96;165;250m',HEX='\x1b[38;2;250;204;21m'
interface ParserState {escape:string;string:boolean;discard:boolean}
/** Keep cursor controls, consume payload SGR and string controls; never inject colors inside an escape. */
export function coloredChunks(chunks:TerminalChunk[]):TerminalChunk[]{
 const states=new Map<number,ParserState>()
 return chunks.map(chunk=>{
  let state=states.get(chunk.direction)
  if(!state){state={escape:'',string:false,discard:false};states.set(chunk.direction,state)}
  if(chunk.kind==='hex'){state.escape='';state.string=false;state.discard=false;return {...chunk,text:HEX+chunk.text}}
  let text=''
  for(const char of chunk.text.replace(/[\u0080-\u009f]/g,c=>({'\u0085':'\n','\u0090':'\x1bP','\u0098':'\x1bX','\u009b':'\x1b[','\u009c':'\x1b\\','\u009d':'\x1b]','\u009e':'\x1b^','\u009f':'\x1b_'}[c]??''))){
   if(!state.escape){if(char==='\x1b'){state.escape=char;continue}text+=char;continue}
   state.escape+=char
   if(state.escape.length===2){state.string=']PX^_'.includes(char);if(state.string)continue;if(char==='[')continue}
   if(state.string){if(char==='\x07'||state.escape.endsWith('\x1b\\')){state.escape='';state.string=false;state.discard=false}else if(state.escape.length>4096){state.escape='\x1b]';state.discard=true}continue}
   if(state.escape[1]==='['){if(char<'@'||char>'~')continue;if(char!=='m'&&state.escape!=='\x1b[!p'&&!state.discard)text+=state.escape}
   else if(char>='0'&&char<='~'){if(state.escape!=='\x1bc'&&!state.discard)text+=state.escape}
   else continue
   state.escape='';state.discard=false
  }
  return {...chunk,text:text?(chunk.direction===1?CLIENT:SERVER)+text:''}
 })
}
/** Amber-500 wash matching the "Flag detected" badge, over the terminal background. */
const FLAG_BACKGROUND='\x1b[48;2;61;46;18m',FLAG_RESET='\x1b[49m'
const FLAG_SCAN_LIMIT=64*1024,FLAG_MATCH_LIMIT=256

/**
 * Wash every flag match with the badge background without touching the text colour
 * that direction colouring already applied. Matching runs over one bounded visible-text
 * tail per direction, so a flag split across read chunks is still highlighted; it never
 * evaluates a regex inside an escape sequence.
 */
export function flagChunks(chunks:TerminalChunk[],pattern:RegExp|null):TerminalChunk[]{
 if(!pattern)return chunks
 const flagged=new Map<number,Set<number>>()
 let matches=0
 const directions=new Set(chunks.filter(chunk=>chunk.kind!=='hex').map(chunk=>chunk.direction))
 for(const direction of directions){
  const indices:number[]=[]
  for(let index=0;index<chunks.length;index++){const chunk=chunks[index]!;if(chunk.kind!=='hex'&&chunk.direction===direction)indices.push(index)}
  const positions:{chunk:number;index:number}[]=[]
  let visible=''
  for(const chunkIndex of indices){
   const text=chunks[chunkIndex]!.text
   for(let i=0;i<text.length;){
    if(text[i]==='\x1b'){i=controlEnd(text,i);continue}
    positions.push({chunk:chunkIndex,index:i});visible+=text[i];i++
   }
  }
  const start=Math.max(0,visible.length-FLAG_SCAN_LIMIT)
  const scan=visible.slice(start),mapping=positions.slice(start)
  pattern.lastIndex=0
  while(matches<FLAG_MATCH_LIMIT){
   const match=pattern.exec(scan)
   if(!match)break
   if(!match[0]){pattern.lastIndex++;continue}
   for(let offset=match.index;offset<match.index+match[0].length&&offset<mapping.length;offset++){
    const position=mapping[offset]!
    let set=flagged.get(position.chunk)
    if(!set){set=new Set();flagged.set(position.chunk,set)}
    set.add(position.index)
   }
   matches++
  }
  pattern.lastIndex=0
 }
 if(!matches)return chunks
 return chunks.map((chunk,index)=>{
  const set=flagged.get(index)
  if(!set||chunk.kind==='hex')return chunk
  return {...chunk,text:applyFlagWash(chunk.text,set)}
 })

 function applyFlagWash(text:string,set:Set<number>):string{
  let result='',open=false
  for(let i=0;i<text.length;){
   if(text[i]==='\x1b'){const end=controlEnd(text,i);result+=text.slice(i,end);i=end;continue}
   const hit=set.has(i)
   if(hit&&!open){result+=FLAG_BACKGROUND;open=true}
   else if(!hit&&open){result+=FLAG_RESET;open=false}
   result+=text[i];i++
  }
  if(open)result+=FLAG_RESET
  return result
 }
}

/** Find the next complete terminal control boundary; never inspect bytes inside it. */
function controlEnd(text:string,start:number):number{
 const kind=text[start+1]
 if(!kind)return text.length
 if(kind==='['){for(let i=start+2;i<text.length;i++){const code=text.charCodeAt(i);if(code>=0x40&&code<=0x7e)return i+1}return text.length}
 if(']PX^_'.includes(kind)){for(let i=start+2;i<text.length;i++){if(text[i]==='\x07')return i+1;if(text[i]==='\x1b'&&text[i+1]==='\\')return i+2}return text.length}
 return Math.min(text.length,start+2)
}
