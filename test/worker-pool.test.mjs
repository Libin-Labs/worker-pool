import test from 'node:test';
import assert from 'node:assert/strict';
import {
  createWorkerPool,
  WorkerPoolClosedError,
  WorkerQueueFullError,
  WorkerTaskError,
  WorkerTimeoutError,
} from '../dist/esm/index.js';

class MockWorker {
  listeners = new Map();
  requests = [];
  terminated = false;

  addEventListener(type, listener) {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type, listener) {
    this.listeners.get(type)?.delete(listener);
  }

  postMessage(message) {
    if (this.terminated) throw new Error('worker terminated');
    this.requests.push(message);
  }

  terminate() {
    this.terminated = true;
  }

  reply(data) {
    for (const listener of this.listeners.get('message') ?? []) {
      listener({ data });
    }
  }

  fail(error) {
    for (const listener of this.listeners.get('error') ?? []) {
      listener({ error, message: error.message, preventDefault() {} });
    }
  }
}

function makePool(options = {}) {
  const workers = [];
  const pool = createWorkerPool({
    workerFactory: () => {
      const worker = new MockWorker();
      workers.push(worker);
      return worker;
    },
    ...options,
  });
  return { pool, workers };
}

test('bounds active work, queues tasks, and dispatches as slots become free', async () => {
  const { pool, workers } = makePool({ size: 2, maxQueue: 2 });
  const first = pool.run(1);
  const second = pool.run(2);
  const third = pool.run(3);
  assert.equal(pool.activeCount, 2);
  assert.equal(pool.pendingCount, 1);
  assert.deepEqual(
    workers.map((worker) => worker.requests.length),
    [1, 1],
  );

  workers[0].reply({ id: workers[0].requests[0].id, type: 'result', value: 10 });
  assert.equal(await first, 10);
  assert.equal(workers[0].requests.length, 2);
  workers[0].reply({ id: workers[0].requests[1].id, type: 'result', value: 30 });
  workers[1].reply({ id: workers[1].requests[0].id, type: 'result', value: 20 });
  assert.deepEqual(await Promise.all([second, third]), [20, 30]);
  assert.equal(pool.activeCount, 0);
  pool.close();
});

test('rejects overflow and removes an aborted queued task', async () => {
  const { pool, workers } = makePool({ maxQueue: 1 });
  const active = pool.run('active');
  const controller = new AbortController();
  const queued = pool.run('queued', { signal: controller.signal });
  await assert.rejects(pool.run('overflow'), WorkerQueueFullError);
  controller.abort();
  await assert.rejects(queued, { name: 'AbortError' });
  assert.equal(pool.pendingCount, 0);

  workers[0].reply({
    id: workers[0].requests[0].id,
    type: 'result',
    value: 'done',
  });
  assert.equal(await active, 'done');
  pool.close();
});

test('active cancellation terminates and replaces its worker', async () => {
  const { pool, workers } = makePool();
  const controller = new AbortController();
  const active = pool.run('cancel me', { signal: controller.signal });
  controller.abort('not needed');
  await assert.rejects(active, { name: 'AbortError', message: 'not needed' });
  assert.equal(workers[0].terminated, true);
  assert.equal(workers.length, 2);

  const next = pool.run('next');
  workers[1].reply({
    id: workers[1].requests[0].id,
    type: 'result',
    value: 'ok',
  });
  assert.equal(await next, 'ok');
  pool.close();
});

test('timeouts replace unresponsive workers and worker error responses reject tasks', async () => {
  const { pool, workers } = makePool({ defaultTimeoutMs: 15 });
  const slow = pool.run('slow');
  await assert.rejects(slow, WorkerTimeoutError);
  assert.equal(workers[0].terminated, true);
  assert.equal(workers.length, 2);

  const failed = pool.run('fail', { timeoutMs: 1000 });
  const request = workers[1].requests[0];
  workers[1].reply({
    id: request.id,
    type: 'error',
    error: { name: 'RangeError', message: 'bad input' },
  });
  await assert.rejects(failed, (error) => {
    assert.ok(error instanceof WorkerTaskError);
    assert.equal(error.workerErrorName, 'RangeError');
    assert.equal(error.message, 'bad input');
    return true;
  });
  assert.equal(workers[1].terminated, false);
  pool.close();
});

test('worker crashes reject its task and pool close rejects remaining work', async () => {
  const { pool, workers } = makePool({ maxQueue: 1 });
  const active = pool.run('active');
  const queued = pool.run('queued');
  workers[0].fail(new Error('worker crashed'));
  await assert.rejects(active, /worker crashed/);
  assert.equal(workers.length, 2);
  assert.equal(workers[1].requests.length, 1);
  pool.close();
  await assert.rejects(queued, WorkerPoolClosedError);
  await assert.rejects(pool.run('after close'), WorkerPoolClosedError);
  assert.equal(
    workers.every((worker) => worker.terminated),
    true,
  );
});
