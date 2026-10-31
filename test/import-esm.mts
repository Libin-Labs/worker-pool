import { createWorkerPool } from '@libinprasanth/worker-pool';

const pool = createWorkerPool<number, string>({
  workerFactory: () => ({
    postMessage() {},
    addEventListener() {},
    removeEventListener() {},
    terminate() {},
  }),
});
const result: Promise<string> = pool.run(1);
void result;
// @ts-expect-error The request type is numeric.
pool.run('wrong');
pool.close();

const browserPool = createWorkerPool<number, string>({
  workerFactory: () =>
    new Worker(new URL('./worker.js', import.meta.url), {
      type: 'module',
    }),
});
browserPool.close();
