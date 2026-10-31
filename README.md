# @libinprasanth/worker-pool

Package version: `1.0.0`. License: MIT.

```sh
npm install @libinprasanth/worker-pool
```

A bounded task scheduler for browser-style Web Workers. It owns a fixed number
of workers, queues work up to a configurable limit, correlates responses by
task ID, and supports task timeouts, `AbortSignal`, worker replacement, and
explicit shutdown.

The manifest requires Node.js 20+ for Node consumers; native Node worker_threads
is not this browser-style interface. ESM and CommonJS have separate declarations.
TypeScript consumers need ES2022 and DOM types, not Node ambient declarations.
There are no runtime dependencies, React requirements, CSS, or separate
worker-helper packages. Importing creates no workers or browser-global accesses.

## Requirements and worker protocol

The package uses the Web Worker API and structured-clone messaging. Provide a
`workerFactory` that creates an independent worker each time it is called. The
worker receives `{ id, type: "task", payload }`. It must answer with either
`{ id, type: "result", value }` or
`{ id, type: "error", error: { name, message, stack? } }`. For example:

```ts
// worker.ts
self.addEventListener("message", (event: MessageEvent) => {
  const request = event.data;
  if (request?.type !== "task") return;
  try {
    const value = request.payload * request.payload;
    self.postMessage({ id: request.id, type: "result", value });
  } catch (error) {
    const failure = error instanceof Error ? error : new Error(String(error));
    self.postMessage({
      id: request.id,
      type: "error",
      error: { name: failure.name, message: failure.message, stack: failure.stack },
    });
  }
});
```

```ts
import { createWorkerPool } from "@libinprasanth/worker-pool";

const pool = createWorkerPool<number, number>({
  size: 2,
  maxQueue: 20,
  workerFactory: () =>
    new Worker(new URL("./worker.js", import.meta.url), { type: "module" }),
});

const square = await pool.run(9, { timeoutMs: 5_000 });
pool.close();
```

The worker script is application code; this library does not bundle, generate,
or load one for you. The worker must follow the response protocol and respond
once per task. Ensure your bundler serves the worker URL correctly.

## API

- `createWorkerPool<Request, Response>(options)` creates `size` workers
  immediately (`size` defaults to `1`). Construction fails if a worker cannot
  be created.
- `workerFactory` is required. Each worker implements `postMessage`,
  `addEventListener`, `removeEventListener`, and `terminate`; this also enables
  deterministic tests with mock workers.
- `maxQueue` limits waiting tasks, defaults to unbounded, and may be zero.
  Active tasks are limited to `size`. A full queue rejects with
  `WorkerQueueFullError`.
- `run(payload, { signal?, timeoutMs?, transfer? })` returns a promise for the
  response value. `timeoutMs` overrides `defaultTimeoutMs`; timeouts begin only
  after dispatch, not while waiting in the queue. Timeout must be positive.
- `activeCount`, `pendingCount`, and `size` report the current pool state.
- `close()` is idempotent, terminates workers, rejects queued and active tasks
  with `WorkerPoolClosedError`, and prevents further submissions.

Worker error responses reject with `WorkerTaskError`; its `workerErrorName`
contains the worker's serialized error name. Unexpected worker errors reject
the current task and replace that worker. If replacement creation fails, the
pool stops and rejects outstanding work. Invalid responses for a matching
task ID reject with `WorkerProtocolError` and replace the worker. Responses
with stale or unrelated IDs are ignored.

## Cancellation and limits

Aborting a queued task removes it without dispatch. Aborting an active task
rejects it with an `AbortError`, terminates that worker, and creates a
replacement. A timeout does the same and rejects with `WorkerTimeoutError`.
Terminating a worker stops its JavaScript execution, but cannot undo side
effects that already reached a server or other external system. This is not a
cooperative cancellation protocol. Use idempotent tasks when retries or
external effects are possible.

Worker payloads, results, errors, and transferables must satisfy the platform's
structured-clone rules. Worker-level crashes fail the current task; task-level
error responses do not replace the worker. This library does not implement
worker scripts, retries, task priorities, persistence, or graceful draining.

`WorkerPoolError` is the package base error. Additional errors are
`WorkerPoolClosedError`, `WorkerQueueFullError`, `WorkerTaskError`,
`WorkerTimeoutError`, and `WorkerProtocolError`.

## Complete public types and defaults

Runtime named exports are `WorkerPool`, `createWorkerPool` and the six error
classes listed above. `new WorkerPool<Request, Response>(options)` is equivalent
to the factory.

| `WorkerPoolOptions` field | Default / contract |
| --- | --- |
| `workerFactory: () => WorkerLike` | Required; also called for replacements |
| `size?: number` | `1`; positive safe integer |
| `maxQueue?: number` | `Infinity`; otherwise non-negative safe integer |
| `defaultTimeoutMs?: number` | Absent; positive finite milliseconds if supplied |
| `idFactory?: () => string` | Generated IDs; must return a non-empty ID not in flight |

Invalid factory types throw `TypeError`; invalid numeric construction options
throw `RangeError`. Creation failure terminates previously created workers.
An empty/duplicate task ID rejects with `WorkerPoolError`; an ID factory error
rejects the task. IDs may be reused only after the previous task settles.
`run(payload, options?: TaskOptions): Promise<Response>` has no timeout by default.
`TaskOptions` fields are optional `signal: AbortSignal`, `timeoutMs: number`,
and `transfer: Transferable[]`. An already-aborted signal rejects without queuing.
Aborting preserves an Error reason; other reasons become an `AbortError`.
Invalid per-task timeouts reject with `RangeError`.

The protocol type-only exports are:

```ts
type WorkerRequest<Request> = { id: string; type: "task"; payload: Request };
type WorkerResult<Response> = { id: string; type: "result"; value: Response };
type WorkerFailure = {
  id: string;
  type: "error";
  error: string | { name?: string; message?: string; stack?: string };
};
type WorkerResponse<Response> = WorkerResult<Response> | WorkerFailure;
```

`WorkerLike` requires `postMessage(message: unknown, transfer?: Transferable[]): void`,
`addEventListener(type: string, listener: EventListener): void`,
`removeEventListener(type: string, listener: EventListener): void`, and
`terminate(): void`. Event handling covers `message`, `error`, and `messageerror`.
Transferred buffers are detached according to platform rules; don't reuse them
after dispatch. A postMessage failure rejects the task and replaces the worker.

`WorkerPoolError(message, options?: ErrorOptions)` extends Error. Closed, queue
full, timeout, and protocol errors inherit that signature. `WorkerTaskError`
accepts `(message, workerErrorName = "Error")`; serialized string stacks, when
provided in a worker error response, replace its local stack.
The readonly counters are numbers; `size` remains the slot count after close.
Completion removes task abort handlers and timers. Always observe task promise
rejections and close the pool in a `finally` block or application cleanup.
No styling or accessibility behavior is provided by this non-visual utility.

## License

[MIT](./LICENSE).

## Support

If this library is useful to you, consider supporting its maintenance with a [donation via PayPal](https://www.paypal.me/LibinPrasanth).

## Author

Created by [Libin Prasanth](https://www.linkedin.com/in/libinprasanth/). Connect on LinkedIn for updates and discussion.
