# @hono/node-server: one promise kept for each chunk of a paced stream

Reproduction for a memory growth in `writeFromReadableStreamDefaultReader`
(`@hono/node-server` 2.1.4; 1.19.14 shows the same growth).

A streaming response whose source is **paced** (live audio, for example) keeps one
`Promise` for each chunk for as long as `write()` returns `true`. A stream that does
not end grows until the connection closes.

## Run

Needs Node.js 20.13 or later, or 22 or later (`v8.queryObjects`, which prints an
`ExperimentalWarning` on some versions; `npm start` hides it).

```bash
npm install
npm start            # paced source: the count grows
PACED=0 npm start    # source as fast as possible: the count stays flat
```

The script starts a Hono app with one streaming route, reads it with `fetch`, and prints
the number of live `Promise` objects (`v8.queryObjects`, which collects garbage first)
and the heap size every 20 000 chunks. Each chunk is 400 bytes.

## Results

`@hono/node-server` 2.1.4, `hono` 4.12.31, macOS arm64. Each cell is
`live Promise objects / heap used in MiB`.

Paced source, published build:

| Chunks sent | Node 20.20.2 | Node 22.23.3 | Node 24.21.0 |
| ---: | ---: | ---: | ---: |
| 20 000 | 20 091 / 8.4 | 20 092 / 9.1 | 20 053 / 9.8 |
| 40 000 | 40 091 / 10.4 | 40 092 / 11.1 | 40 053 / 11.8 |
| 60 000 | 60 091 / 12.4 | 60 092 / 13.1 | 60 053 / 13.8 |
| 80 000 | 80 091 / 14.3 | 80 092 / 15.1 | 80 053 / 15.8 |
| 100 000 | 100 091 / 16.3 | 100 092 / 17.1 | 100 053 / 17.8 |

One more promise for each chunk, and about 100 bytes of heap for each.

Paced source, with the fix below built into `node_modules`:

| Chunks sent | Node 20.20.2 | Node 22.23.3 | Node 24.21.0 |
| ---: | ---: | ---: | ---: |
| 20 000 | 101 / 6.5 | 102 / 7.2 | 63 / 7.8 |
| 100 000 | 101 / 6.4 | 102 / 7.2 | 63 / 7.9 |

Unpaced source (`PACED=0`), published build, Node 24.21.0: 133, 207, 119, 193 and
105 promises at the five steps, with the heap between 7.7 and 7.8 MiB. `write()` returns
`false` from time to time, the chain settles, and `onDrain` starts a new one.

## Cause

`src/utils.ts`:

```ts
function flow({ done, value }) {
  try {
    if (done) {
      writable.end()
    } else if (!writable.write(value)) {
      writable.once('drain', onDrain)
    } else {
      return reader.read().then(flow, handleStreamError)
    }
  } catch (e) {
    handleStreamError(e)
  }
}
```

`flow` runs as a `then` callback and returns the promise of the next read. A promise
returned from a `then` callback makes the promise of that `then` wait for it
([NewPromiseResolveThenableJob](https://tc39.es/ecma262/#sec-newpromiseresolvethenablejob)).
While `write()` keeps returning `true`, each chunk adds one promise to a chain. The chain
settles only when the stream ends, when `write()` returns `false`, or on an error.

## Fix

Start the next read without returning its promise:

```diff
-      return reader.read().then(flow, handleStreamError)
+      reader.read().then(flow, handleStreamError)
```

Nothing reads the value that `flow` returns, and the errors still reach
`handleStreamError`.
