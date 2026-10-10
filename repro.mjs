// A streaming response that does not end, and one client that reads all of it.
//
// The source is paced, like live audio, so `write()` always returns true.
// With PACED=0 the source produces as fast as it can: `write()` then returns
// false from time to time.
//
// Every 20 000 chunks, the script prints the live Promise objects and the heap size.
import v8 from 'node:v8'
import { serve } from '@hono/node-server'
import { Hono } from 'hono'
import { readFileSync } from 'node:fs'

// The package does not export its package.json: read the installed file.
const { version } = JSON.parse(
  readFileSync(new URL('./node_modules/@hono/node-server/package.json', import.meta.url), 'utf8')
)

const CHUNKS = Number(process.env.CHUNKS ?? 100_000)
const STEP = 20_000
const PACED = process.env.PACED !== '0'
const chunk = new Uint8Array(400)

let sent = 0
let report = () => {}
const app = new Hono()
app.get('/stream', () => {
  const send = (controller) => {
    controller.enqueue(chunk)
    if (++sent % STEP === 0) report()
  }
  let timer
  const body = PACED
    ? new ReadableStream({
        start(controller) {
          // 10 chunks each millisecond, less than a local socket accepts.
          timer = setInterval(() => {
            for (let i = 0; i < 10; i++) send(controller)
          }, 1)
        },
        cancel() {
          clearInterval(timer)
        },
      })
    : new ReadableStream({
        pull(controller) {
          send(controller)
        },
      })
  return new Response(body, { headers: { 'Content-Type': 'application/octet-stream' } })
})

const server = serve({ fetch: app.fetch, port: 0 }, async ({ port }) => {
  console.log(`node ${process.version}, @hono/node-server ${version}, ${PACED ? 'paced' : 'unpaced'} source`)
  console.log('chunks sent | live Promise objects | heap used (MiB)')
  const done = new Promise((resolve) => {
    report = () => {
      // `queryObjects` collects garbage first: it counts only the promises still reachable.
      const promises = v8.queryObjects(Promise, { format: 'count' })
      const heap = (process.memoryUsage().heapUsed / 1048576).toFixed(1)
      console.log(`${String(sent).padStart(11)} | ${String(promises).padStart(20)} | ${heap.padStart(15)}`)
      if (sent >= CHUNKS) resolve()
    }
  })
  const response = await fetch(`http://127.0.0.1:${port}/stream`)
  const reader = response.body.getReader()
  let received = 0
  const reading = (async () => {
    for (;;) {
      const { done: end, value } = await reader.read()
      if (end) break
      received += value.byteLength
    }
  })()
  await done
  await reader.cancel()
  await reading.catch(() => {})
  console.log(`client received ${received} bytes`)
  server.close()
  process.exit(0)
})
