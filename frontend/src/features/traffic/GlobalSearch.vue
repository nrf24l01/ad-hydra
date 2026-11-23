<script setup lang="ts">
import { computed, onUnmounted, ref, watch } from 'vue'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { segments } from './highlight'
import type { SearchItem, SearchPage, SearchScope } from './liveSocket'
import { direction, timestamp } from './traffic'

const props = defineProps<{
  result?: SearchPage
  loading: boolean
  error?: string
  services: { id: string; name: string }[]
}>()
const emit = defineEmits<{
  search: [query: { q: string; scope: SearchScope; service_id: string | null }]
  more: []
  select: [item: SearchItem]
  clear: []
}>()

const q = ref('')
const scope = ref<SearchScope>('both')
const service = ref('all')
let timer: ReturnType<typeof setTimeout> | undefined

const tooShort = computed(() => q.value.trim().length > 0 && q.value.trim().length < 2)
const items = computed(() => props.result?.items ?? [])
const coverage = computed(() => props.result?.coverage)
const partial = computed(() => Boolean(coverage.value?.truncated || coverage.value?.continuation))
const serviceName = (id: string) => props.services.find(item => item.id === id)?.name ?? id
function snippetSegments(item: SearchItem) {
  return segments(item.snippet, [...(item.spans ?? []), ...(item.flag_spans ?? [])])
}

function submit(): void {
  const value = q.value.trim()
  if (value.length < 2) { emit('clear'); return }
  if (value.length > 256) return
  emit('search', { q: value, scope: scope.value, service_id: service.value === 'all' ? null : service.value })
}

function schedule(): void {
  clearTimeout(timer)
  timer = setTimeout(submit, 250)
}

watch([q, scope, service], schedule)
onUnmounted(() => clearTimeout(timer))
</script>

<template>
  <section class="space-y-3 rounded-md border bg-card p-4" aria-label="Search all retained traffic">
    <div class="flex flex-wrap items-end gap-2">
      <div class="min-w-48 flex-1 space-y-1">
        <label for="traffic-global-search" class="text-xs font-medium">Search retained traffic</label>
        <Input id="traffic-global-search" v-model="q" maxlength="256" placeholder="Literal text, at least 2 characters…" autocomplete="off" />
      </div>
      <Select v-model="scope">
        <SelectTrigger aria-label="Search scope" class="w-36"><SelectValue /></SelectTrigger>
        <SelectContent>
          <SelectItem value="both">Messages + flows</SelectItem>
          <SelectItem value="messages">Messages</SelectItem>
          <SelectItem value="flows">Flows</SelectItem>
        </SelectContent>
      </Select>
      <Select v-model="service">
        <SelectTrigger aria-label="Search service" class="w-44"><SelectValue placeholder="All services" /></SelectTrigger>
        <SelectContent>
          <SelectItem value="all">All services</SelectItem>
          <SelectItem v-for="item in services" :key="item.id" :value="item.id">{{ item.name }}</SelectItem>
        </SelectContent>
      </Select>
    </div>
    <p v-if="tooShort" class="text-xs text-muted-foreground">Enter at least two characters.</p>
    <p class="text-xs text-muted-foreground">Searches decoded traffic still retained by Core. Proxy raw capture files, rotated bytes and uncommitted live tail are not searched. Matching is literal, case-insensitive text; it is not a regex.</p>
    <p v-if="error" role="alert" class="text-xs text-destructive">{{ error }}</p>
    <p v-if="loading" role="status" class="text-xs text-muted-foreground">Searching retained traffic…</p>
    <p v-if="partial" role="status" class="text-xs text-amber-500">Partial results — more available. This scan did not cover every retained row yet.</p>
    <div v-if="result" class="flex flex-wrap gap-3 text-xs text-muted-foreground">
      <span>{{ items.length }} matches in this page</span>
      <span>{{ coverage?.scanned ?? 0 }} rows scanned</span>
      <span v-if="coverage?.undecodable">· {{ coverage.undecodable }} undecodable rows skipped</span>
      <span v-if="coverage?.raw_history_searched === false">· raw capture not searched</span>
    </div>
    <ul v-if="items.length" class="space-y-2" aria-label="Search results">
      <li v-for="item in items" :key="item.kind + ':' + (item.message_id ?? item.flow_id)" class="rounded-sm border p-3">
        <div class="flex flex-wrap items-center gap-2 text-[11px] text-muted-foreground">
          <Badge variant="secondary">{{ item.kind }}</Badge>
          <span>{{ serviceName(item.service_id) }}</span>
          <span>{{ timestamp(item.occurred_ms) }}</span>
          <span v-if="item.direction">{{ direction(item.direction) }}</span>
          <span v-if="item.protocol_id">{{ item.protocol_id }}</span>
          <Badge v-if="item.has_flag" class="border-amber-500/60 bg-amber-500/10 text-amber-500">Flag</Badge>
        </div>
        <p class="mt-1 break-words font-mono text-xs"><template v-for="(part, index) in snippetSegments(item)" :key="index"><mark v-if="part.flag" class="rounded-sm bg-amber-500/25 px-0.5 text-inherit">{{ part.text }}</mark><template v-else>{{ part.text }}</template></template></p>
        <p v-if="item.snippet_truncated" class="mt-1 text-[11px] text-muted-foreground">Snippet is a display window; matching used the full retained text.</p>
        <Button size="sm" variant="outline" class="mt-2" @click="emit('select', item)">Open flow</Button>
      </li>
    </ul>
    <p v-else-if="result && !loading" class="text-sm text-muted-foreground">No matching decoded retained traffic in the scanned range. This does not mean the text never existed; retention and decoding limits still apply.</p>
    <Button v-if="partial" variant="outline" size="sm" :disabled="loading" @click="emit('more')">More results</Button>
  </section>
</template>
