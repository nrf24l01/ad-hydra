/**
 * Regression tests for `installBrowserGlobals` cleanup (CI run 7a6514c).
 *
 * Why this exists: the harness installs a fake `document` that has no
 * `createElement`. Vue's `runtime-dom` evaluates `doc.createElement('template')`
 * at module scope, so a process that imports Vue while the stub is still
 * installed dies with `doc.createElement is not a function`. `bun test` keeps a
 * module registry, so once Vue has been imported the failure can hide behind the
 * cache. The decisive case therefore runs in a fresh `process.execPath` child
 * that does install -> cleanup -> `import('vue')`, instead of asserting on this
 * already-populated test process.
 *
 * Contract encoded here (the shared helper's new signature):
 *   const cleanup = installBrowserGlobals()
 *   cleanup() // deletes globals that were absent, restores every touched
 *             // global's exact descriptor, is idempotent, and never restores
 *             // a fake over a pre-existing value
 *
 * The harness is installed in `beforeEach` and cleaned up once in `afterAll`, so
 * "the snapshot that cleanup restores" must be the state from *before the first
 * install*, not the previous fake. That is why the repeated-install test below
 * matters: a per-call capture would restore a leftover fake and re-open the bug.
 * Nested installs are asserted only where the contract is meaningful: an inner
 * cleanup may restore the outer stub or the pre-install value, but it must never
 * delete a pre-existing `document` or leave a third, foreign value behind.
 *
 * Nothing here claims the stub can render a DOM; only that globalThis is
 * pristine before a legitimate `import('vue')`.
 */
import { expect, test } from 'bun:test'
import { spawnSync } from 'node:child_process'
import { join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { installBrowserGlobals } from './helpers/live-socket-harness'

const CHILD_TIMEOUT_MS = 30_000
const TEST_TIMEOUT_MS = 45_000

/** Every own property of globalThis with its exact descriptor. */
function snapshotOwnDescriptors(): Map<string, PropertyDescriptor> {
  const snapshot = new Map<string, PropertyDescriptor>()
  for (const key of Object.getOwnPropertyNames(globalThis)) {
    const descriptor = Object.getOwnPropertyDescriptor(globalThis, key)
    if (descriptor) snapshot.set(key, descriptor)
  }
  return snapshot
}

function sameDescriptor(a: PropertyDescriptor | undefined, b: PropertyDescriptor | undefined): boolean {
  if (!a || !b) return a === b
  return a.configurable === b.configurable
    && a.enumerable === b.enumerable
    && a.writable === b.writable
    && Object.is(a.value, b.value)
    && Object.is(a.get, b.get)
    && Object.is(a.set, b.set)
}

function describeDescriptor(descriptor: PropertyDescriptor | undefined): string {
  if (!descriptor) return 'absent'
  if (descriptor.get || descriptor.set) {
    return `accessor(get=${descriptor.get ? 'fn' : 'undefined'}, set=${descriptor.set ? 'fn' : 'undefined'}, enumerable=${descriptor.enumerable}, configurable=${descriptor.configurable})`
  }
  return `data(value=${typeof descriptor.value}, writable=${descriptor.writable}, enumerable=${descriptor.enumerable}, configurable=${descriptor.configurable})`
}

/** Human-readable list of every key whose descriptor differs between two snapshots. */
function descriptorDiff(before: Map<string, PropertyDescriptor>, after: Map<string, PropertyDescriptor>): string[] {
  const keys = new Set([...before.keys(), ...after.keys()])
  return [...keys]
    .filter(key => !sameDescriptor(before.get(key), after.get(key)))
    .map(key => `${key}: ${describeDescriptor(before.get(key))} -> ${describeDescriptor(after.get(key))}`)
}

/** Best-effort exact restore of globalThis; throws only if a leak cannot be undone. */
function restoreSnapshot(before: Map<string, PropertyDescriptor>): void {
  const failed: string[] = []
  for (const key of Object.getOwnPropertyNames(globalThis)) {
    if (before.has(key)) continue
    try {
      if (!delete (globalThis as unknown as Record<string, unknown>)[key]) failed.push(key)
    } catch {
      failed.push(key)
    }
  }
  for (const [key, descriptor] of before) {
    if (sameDescriptor(Object.getOwnPropertyDescriptor(globalThis, key), descriptor)) continue
    try {
      Object.defineProperty(globalThis, key, descriptor)
    } catch {
      failed.push(key)
    }
  }
  if (failed.length > 0) {
    throw new Error(`test leaked globals that could not be restored: ${[...new Set(failed)].join(', ')}`)
  }
}

/** Runs `body` and always returns globalThis to its exact pre-test descriptor map. */
function withCleanGlobals<T>(body: () => T): T {
  const before = snapshotOwnDescriptors()
  try {
    return body()
  } finally {
    restoreSnapshot(before)
  }
}

/** Removes a global so a test can establish a known-absent baseline. */
function deleteOwnGlobal(key: string): void {
  const descriptor = Object.getOwnPropertyDescriptor(globalThis, key)
  if (!descriptor) return
  if (!descriptor.configurable) {
    throw new Error(`cannot establish a clean baseline: globalThis.${key} is non-configurable`)
  }
  delete (globalThis as unknown as Record<string, unknown>)[key]
}

/** Defines a pre-existing `document` fixture that must survive a cleanup untouched. */
function defineParentDocument(value: Record<string, unknown>): PropertyDescriptor {
  Object.defineProperty(globalThis, 'document', {
    value,
    writable: true,
    enumerable: false,
    configurable: true,
  })
  const descriptor = Object.getOwnPropertyDescriptor(globalThis, 'document')
  if (!descriptor) throw new Error('failed to define the parent document fixture')
  return descriptor
}

test('cleanup deletes the globals that did not exist before install', () => {
  withCleanGlobals(() => {
    deleteOwnGlobal('document')
    deleteOwnGlobal('location')
    expect(Object.getOwnPropertyDescriptor(globalThis, 'document')).toBeUndefined()
    expect(Object.getOwnPropertyDescriptor(globalThis, 'location')).toBeUndefined()

    const cleanup = installBrowserGlobals()
    try {
      expect(typeof cleanup).toBe('function')
      expect(Object.getOwnPropertyDescriptor(globalThis, 'document')).toBeDefined()
      expect(Object.getOwnPropertyDescriptor(globalThis, 'location')).toBeDefined()
    } finally {
      cleanup()
    }

    expect(() => cleanup()).not.toThrow() // idempotent: a second call is a no-op
    expect(Object.getOwnPropertyDescriptor(globalThis, 'document')).toBeUndefined()
    expect(Object.getOwnPropertyDescriptor(globalThis, 'location')).toBeUndefined()
  })
})

test('cleanup restores every touched global descriptor, not just document', () => {
  withCleanGlobals(() => {
    deleteOwnGlobal('document')
    deleteOwnGlobal('location')
    const nativeWebSocket = globalThis.WebSocket
    const before = snapshotOwnDescriptors()

    const cleanup = installBrowserGlobals()
    const installed = snapshotOwnDescriptors()
    try {
      const touched = [...installed.keys()].filter(key => !sameDescriptor(before.get(key), installed.get(key)))
      expect(touched).toEqual(expect.arrayContaining(['document', 'location']))
      expect(touched).toContain('WebSocket')
    } finally {
      cleanup()
    }

    const after = snapshotOwnDescriptors()
    expect(descriptorDiff(before, after)).toEqual([])
    expect(globalThis.WebSocket).toBe(nativeWebSocket)
  })
})

test('cleanup restores a preexisting document value and property flags exactly', () => {
  withCleanGlobals(() => {
    const parentDocument = { cookie: 'hydra_csrf=parent-fixture', hidden: false, visibilityState: 'visible' }
    defineParentDocument(parentDocument)

    const cleanup = installBrowserGlobals()
    try {
      expect<unknown>(globalThis.document).not.toBe(parentDocument)
    } finally {
      cleanup()
    }

    const restored = Object.getOwnPropertyDescriptor(globalThis, 'document')
    expect(restored).toBeDefined()
    expect(restored?.value).toBe(parentDocument)
    expect(restored?.writable).toBe(true)
    expect(restored?.enumerable).toBe(false) // an assignment-based restore would flip this to true
    expect(restored?.configurable).toBe(true)
    expect(restored?.get).toBeUndefined()
    expect(restored?.set).toBeUndefined()
  })
})

test('cleanup is idempotent and never deletes a restored preexisting document', () => {
  withCleanGlobals(() => {
    const parentDocument = { cookie: 'hydra_csrf=parent-fixture' }
    const parentDescriptor = defineParentDocument(parentDocument)

    const cleanup = installBrowserGlobals()
    try {
      expect<unknown>(globalThis.document).not.toBe(parentDocument)
    } finally {
      // A failure above must not leave the harness stubs installed.
      cleanup()
    }

    const afterFirst = Object.getOwnPropertyDescriptor(globalThis, 'document')
    expect(afterFirst?.value).toBe(parentDocument)

    expect(() => cleanup()).not.toThrow()
    const afterSecond = Object.getOwnPropertyDescriptor(globalThis, 'document')
    expect(sameDescriptor(afterFirst, afterSecond)).toBe(true)
    expect(afterSecond?.value).toBe(parentDocument)
    expect(sameDescriptor(afterSecond, parentDescriptor)).toBe(true)
  })
})

test('repeated installs without an intervening cleanup still restore the true pre-install state', () => {
  withCleanGlobals(() => {
    deleteOwnGlobal('document')
    expect(Object.getOwnPropertyDescriptor(globalThis, 'document')).toBeUndefined()

    const firstCleanup = installBrowserGlobals()
    const secondCleanup = installBrowserGlobals()
    try {
      expect(Object.getOwnPropertyDescriptor(globalThis, 'document')).toBeDefined()
    } finally {
      secondCleanup()
    }

    // A per-install snapshot would restore the first fake here and leave the bug open.
    expect(Object.getOwnPropertyDescriptor(globalThis, 'document')).toBeUndefined()

    expect(() => firstCleanup()).not.toThrow() // stale cleanup is a no-op, not a reinstall
    expect(Object.getOwnPropertyDescriptor(globalThis, 'document')).toBeUndefined()
  })
})

test('nested installs never delete or replace a preexisting document', () => {
  withCleanGlobals(() => {
    const parentDocument = { cookie: 'hydra_csrf=parent-fixture' }
    const parentDescriptor = defineParentDocument(parentDocument)

    const cleanupOuter = installBrowserGlobals()
    const outerDocument = globalThis.document
    const cleanupInner = installBrowserGlobals()
    try {
      expect<unknown>(globalThis.document).not.toBe(parentDocument)
      cleanupInner()

      // Inner cleanup may restore the outer stub or the pre-install fixture, but it
      // must never delete the fixture or leave a third, foreign value in place.
      expect([outerDocument, parentDocument]).toContain(globalThis.document)
    } finally {
      cleanupInner() // idempotent no-op when the body already ran it
      cleanupOuter()
    }

    expect<unknown>(globalThis.document).toBe(parentDocument)
    expect(sameDescriptor(Object.getOwnPropertyDescriptor(globalThis, 'document'), parentDescriptor)).toBe(true)
  })
})

test('a fresh Bun process imports vue after install and cleanup', () => {
  const helperUrl = pathToFileURL(join(import.meta.dir, 'helpers', 'live-socket-harness.ts')).href
  const childSource = [
    "const fail = (code, message) => { console.error('BROWSER_GLOBALS_CHILD_FAIL: ' + message); process.exit(code) }",
    `const helper = await import(${JSON.stringify(helperUrl)})`,
    'const nativeWebSocket = globalThis.WebSocket',
    "if ('document' in globalThis) fail(11, 'helper module evaluation installed document before install')",
    'const cleanup = helper.installBrowserGlobals()',
    "if (typeof cleanup !== 'function') fail(12, 'installBrowserGlobals() did not return a cleanup function')",
    "if (!globalThis.document || typeof globalThis.document !== 'object') fail(13, 'document stub was not installed')",
    "if (globalThis.WebSocket === nativeWebSocket) fail(14, 'WebSocket stub was not installed')",
    'cleanup()',
    "if ('document' in globalThis) fail(15, 'document was not deleted by cleanup (assigning undefined would still hide the bug from Vue)')",
    "if ('location' in globalThis) fail(16, 'location was not deleted by cleanup')",
    'if (globalThis.WebSocket !== nativeWebSocket) fail(17, "cleanup did not restore the native WebSocket")',
    "const vue = await import('vue')",
    "if (!vue || typeof vue.createApp !== 'function') fail(18, 'vue did not expose createApp after cleanup')",
    "console.log('BROWSER_GLOBALS_CHILD_OK')",
  ].join('\n')

  const child = spawnSync(process.execPath, ['-e', childSource], {
    cwd: resolve(import.meta.dir, '..'),
    encoding: 'utf8',
    timeout: CHILD_TIMEOUT_MS,
    killSignal: 'SIGKILL',
  })
  const stdout = String(child.stdout ?? '')
  const stderr = String(child.stderr ?? '')
  if (child.error || child.status !== 0) {
    throw new Error([
      `fresh Bun child failed: status=${child.status} signal=${child.signal} error=${child.error?.message ?? 'none'}`,
      `stdout:\n${stdout}`,
      `stderr:\n${stderr}`,
    ].join('\n'))
  }
  expect(child.signal).toBeNull()
  expect(stdout).toContain('BROWSER_GLOBALS_CHILD_OK')
}, TEST_TIMEOUT_MS)
