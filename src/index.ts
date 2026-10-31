export type WorkerRequest<Request> = {
  id: string;
  type: 'task';
  payload: Request;
};

export type WorkerResult<Response> = {
  id: string;
  type: 'result';
  value: Response;
};

export type WorkerFailure = {
  id: string;
  type: 'error';
  error: string | { name?: string; message?: string; stack?: string };
};

export type WorkerResponse<Response> = WorkerResult<Response> | WorkerFailure;

export type WorkerLike = {
  postMessage(message: unknown, transfer?: Transferable[]): void;
  addEventListener(type: string, listener: EventListener): void;
  removeEventListener(type: string, listener: EventListener): void;
  terminate(): void;
};

export type WorkerPoolOptions = {
  workerFactory: () => WorkerLike;
  size?: number;
  maxQueue?: number;
  defaultTimeoutMs?: number;
  idFactory?: () => string;
};

export type TaskOptions = {
  signal?: AbortSignal;
  timeoutMs?: number;
  transfer?: Transferable[];
};

export class WorkerPoolError extends Error {
  override name = 'WorkerPoolError';

  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
  }
}

export class WorkerPoolClosedError extends WorkerPoolError {
  override name = 'WorkerPoolClosedError';
}

export class WorkerQueueFullError extends WorkerPoolError {
  override name = 'WorkerQueueFullError';
}

export class WorkerTaskError extends WorkerPoolError {
  override name = 'WorkerTaskError';

  constructor(
    message: string,
    readonly workerErrorName = 'Error',
  ) {
    super(message);
  }
}

export class WorkerTimeoutError extends WorkerPoolError {
  override name = 'WorkerTimeoutError';
}

export class WorkerProtocolError extends WorkerPoolError {
  override name = 'WorkerProtocolError';
}

type Task<Request, Response> = {
  id: string;
  payload: Request;
  transfer: Transferable[] | undefined;
  timeoutMs: number | undefined;
  signal: AbortSignal | undefined;
  resolve: (value: Response) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout> | undefined;
  abortHandler: (() => void) | undefined;
  slot: Slot<Request, Response> | undefined;
  settled: boolean;
};

type Slot<Request, Response> = {
  worker: WorkerLike;
  task: Task<Request, Response> | undefined;
  onMessage: EventListener;
  onError: EventListener;
  onMessageError: EventListener;
};

export class WorkerPool<Request, Response> {
  private readonly slots: Slot<Request, Response>[] = [];
  private readonly queue: Task<Request, Response>[] = [];
  private readonly workerFactory: () => WorkerLike;
  private readonly idFactory: () => string;
  private readonly maxQueue: number;
  private readonly defaultTimeoutMs: number | undefined;
  private closed = false;

  constructor(options: WorkerPoolOptions) {
    if (typeof options.workerFactory !== 'function') {
      throw new TypeError('workerFactory must be a function.');
    }
    const size = options.size ?? 1;
    if (!Number.isSafeInteger(size) || size < 1) {
      throw new RangeError('Worker pool size must be a positive safe integer.');
    }
    const maxQueue = options.maxQueue ?? Number.POSITIVE_INFINITY;
    if (
      maxQueue !== Number.POSITIVE_INFINITY &&
      (!Number.isSafeInteger(maxQueue) || maxQueue < 0)
    ) {
      throw new RangeError('maxQueue must be a non-negative safe integer.');
    }
    if (
      options.defaultTimeoutMs !== undefined &&
      (!Number.isFinite(options.defaultTimeoutMs) || options.defaultTimeoutMs <= 0)
    ) {
      throw new RangeError('defaultTimeoutMs must be a positive finite number.');
    }
    this.workerFactory = options.workerFactory;
    this.idFactory = options.idFactory ?? createId;
    this.maxQueue = maxQueue;
    this.defaultTimeoutMs = options.defaultTimeoutMs;
    try {
      for (let index = 0; index < size; index += 1) {
        this.slots.push(this.createSlot());
      }
    } catch (error) {
      for (const slot of this.slots) this.disposeWorker(slot);
      throw asError(error, 'Could not create worker pool.');
    }
  }

  get size(): number {
    return this.slots.length;
  }

  get activeCount(): number {
    return this.slots.filter((slot) => slot.task !== undefined).length;
  }

  get pendingCount(): number {
    return this.queue.length;
  }

  run(payload: Request, options: TaskOptions = {}): Promise<Response> {
    if (this.closed) {
      return Promise.reject(new WorkerPoolClosedError('This worker pool is closed.'));
    }
    if (options.signal?.aborted) {
      return Promise.reject(abortError(options.signal.reason));
    }
    const timeoutMs = options.timeoutMs ?? this.defaultTimeoutMs;
    if (timeoutMs !== undefined && (!Number.isFinite(timeoutMs) || timeoutMs <= 0)) {
      return Promise.reject(new RangeError('timeoutMs must be a positive finite number.'));
    }
    const hasIdleSlot = this.slots.some((slot) => slot.task === undefined);
    if (!hasIdleSlot && this.queue.length >= this.maxQueue) {
      return Promise.reject(new WorkerQueueFullError('The worker pool queue is full.'));
    }
    let id: string;
    try {
      id = this.idFactory();
    } catch (error) {
      return Promise.reject(asError(error, 'Could not create task ID.'));
    }
    if (!id || this.hasTaskId(id)) {
      return Promise.reject(
        new WorkerPoolError('idFactory returned an empty or duplicate task ID.'),
      );
    }

    return new Promise<Response>((resolve, reject) => {
      const task: Task<Request, Response> = {
        id,
        payload,
        transfer: options.transfer,
        timeoutMs,
        signal: options.signal,
        resolve,
        reject,
        timer: undefined,
        abortHandler: undefined,
        slot: undefined,
        settled: false,
      };
      if (task.signal) {
        task.abortHandler = () => this.abortTask(task);
        task.signal.addEventListener('abort', task.abortHandler, { once: true });
      }
      this.queue.push(task);
      this.dispatch();
    });
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    const error = new WorkerPoolClosedError('Worker pool was closed.');
    for (const task of this.queue.splice(0)) this.finishTask(task, error);
    for (const slot of this.slots) {
      if (slot.task) {
        this.finishTask(slot.task, error);
        slot.task = undefined;
      }
      this.disposeWorker(slot);
    }
  }

  private createSlot(): Slot<Request, Response> {
    const worker = this.workerFactory();
    const slot = {} as Slot<Request, Response>;
    slot.worker = worker;
    slot.task = undefined;
    slot.onMessage = (event) => this.handleMessage(slot, event as MessageEvent);
    slot.onError = (event) => this.handleWorkerError(slot, event as ErrorEvent);
    slot.onMessageError = () =>
      this.handleWorkerError(slot, {
        error: undefined,
        message: 'Worker message could not be deserialized.',
        preventDefault() {},
      } as ErrorEvent);
    worker.addEventListener('message', slot.onMessage);
    worker.addEventListener('error', slot.onError);
    worker.addEventListener('messageerror', slot.onMessageError);
    return slot;
  }

  private dispatch(): void {
    if (this.closed) return;
    for (const slot of this.slots) {
      if (slot.task) continue;
      let task = this.queue.shift();
      while (task && task.signal?.aborted) {
        this.finishTask(task, abortError(task.signal.reason));
        task = this.queue.shift();
      }
      if (!task) continue;
      slot.task = task;
      task.slot = slot;
      if (task.timeoutMs !== undefined) {
        task.timer = setTimeout(() => this.timeoutTask(slot, task), task.timeoutMs);
      }
      try {
        const request: WorkerRequest<Request> = {
          id: task.id,
          type: 'task',
          payload: task.payload,
        };
        slot.worker.postMessage(request, task.transfer);
      } catch (error) {
        this.finishTask(task, asError(error, 'Could not post task to worker.'));
        slot.task = undefined;
        this.replaceWorker(slot);
      }
    }
  }

  private handleMessage(slot: Slot<Request, Response>, event: MessageEvent): void {
    if (this.closed || !slot.task) return;
    const task = slot.task;
    const message = event.data as Record<string, unknown> | null;
    if (!message || typeof message !== 'object' || message.id !== task.id) return;
    if (message.type === 'result' && Object.hasOwn(message, 'value')) {
      this.finishTask(task, undefined, message.value as Response);
      slot.task = undefined;
      this.dispatch();
      return;
    }
    if (message.type === 'error' && Object.hasOwn(message, 'error')) {
      this.finishTask(task, workerFailure(message.error));
      slot.task = undefined;
      this.dispatch();
      return;
    }
    this.finishTask(
      task,
      new WorkerProtocolError(`Worker returned an invalid response for task "${task.id}".`),
    );
    slot.task = undefined;
    this.replaceWorker(slot);
  }

  private handleWorkerError(slot: Slot<Request, Response>, event: ErrorEvent): void {
    if (this.closed || !this.slots.includes(slot)) return;
    event.preventDefault();
    const error =
      event.error instanceof Error
        ? event.error
        : new WorkerPoolError(event.message || 'Worker failed.');
    if (slot.task) this.finishTask(slot.task, error);
    slot.task = undefined;
    this.replaceWorker(slot);
  }

  private timeoutTask(slot: Slot<Request, Response>, task: Task<Request, Response>): void {
    if (slot.task !== task) return;
    this.finishTask(
      task,
      new WorkerTimeoutError(`Task "${task.id}" exceeded ${task.timeoutMs} ms.`),
    );
    slot.task = undefined;
    this.replaceWorker(slot);
  }

  private abortTask(task: Task<Request, Response>): void {
    if (task.settled) return;
    const queuedIndex = this.queue.indexOf(task);
    if (queuedIndex !== -1) {
      this.queue.splice(queuedIndex, 1);
      this.finishTask(task, abortError(task.signal?.reason));
      return;
    }
    const slot = task.slot;
    if (!slot || slot.task !== task) return;
    this.finishTask(task, abortError(task.signal?.reason));
    slot.task = undefined;
    this.replaceWorker(slot);
  }

  private replaceWorker(slot: Slot<Request, Response>): void {
    if (this.closed) return;
    this.disposeWorker(slot);
    try {
      const replacement = this.createSlot();
      const index = this.slots.indexOf(slot);
      this.slots[index] = replacement;
      this.dispatch();
    } catch (error) {
      this.failPool(asError(error, 'Could not replace failed worker.'));
    }
  }

  private failPool(error: Error): void {
    if (this.closed) return;
    this.closed = true;
    const unavailable = new WorkerPoolError('Worker pool stopped after a worker failure.', {
      cause: error,
    });
    for (const task of this.queue.splice(0)) this.finishTask(task, unavailable);
    for (const slot of this.slots) {
      if (slot.task) {
        this.finishTask(slot.task, unavailable);
        slot.task = undefined;
      }
      this.disposeWorker(slot);
    }
  }

  private finishTask(task: Task<Request, Response>, error?: Error, value?: Response): void {
    if (task.settled) return;
    task.settled = true;
    if (task.timer !== undefined) clearTimeout(task.timer);
    if (task.signal && task.abortHandler) {
      task.signal.removeEventListener('abort', task.abortHandler);
    }
    task.slot = undefined;
    if (error) task.reject(error);
    else task.resolve(value as Response);
  }

  private disposeWorker(slot: Slot<Request, Response>): void {
    slot.worker.removeEventListener('message', slot.onMessage);
    slot.worker.removeEventListener('error', slot.onError);
    slot.worker.removeEventListener('messageerror', slot.onMessageError);
    try {
      slot.worker.terminate();
    } catch {
      // Disposal is best-effort; the pool no longer uses this worker.
    }
  }

  private hasTaskId(id: string): boolean {
    return (
      this.queue.some((task) => task.id === id) || this.slots.some((slot) => slot.task?.id === id)
    );
  }
}

export function createWorkerPool<Request, Response>(
  options: WorkerPoolOptions,
): WorkerPool<Request, Response> {
  return new WorkerPool<Request, Response>(options);
}

function workerFailure(value: unknown): WorkerTaskError {
  if (typeof value === 'string') return new WorkerTaskError(value);
  if (typeof value === 'object' && value !== null) {
    const failure = value as { name?: unknown; message?: unknown; stack?: unknown };
    const error = new WorkerTaskError(
      typeof failure.message === 'string' ? failure.message : 'Worker task failed.',
      typeof failure.name === 'string' ? failure.name : 'Error',
    );
    if (typeof failure.stack === 'string') error.stack = failure.stack;
    return error;
  }
  return new WorkerTaskError('Worker task failed.');
}

function abortError(reason: unknown): Error {
  if (reason instanceof Error) return reason;
  if (typeof DOMException === 'function') {
    return new DOMException(
      reason === undefined ? 'The operation was aborted.' : String(reason),
      'AbortError',
    );
  }
  const error = new Error(reason === undefined ? 'The operation was aborted.' : String(reason));
  error.name = 'AbortError';
  return error;
}

function asError(error: unknown, fallback: string): Error {
  if (error instanceof Error) return error;
  return new WorkerPoolError(fallback, { cause: error });
}

let idCounter = 0;

function createId(): string {
  const randomUUID = globalThis.crypto?.randomUUID;
  if (randomUUID) return randomUUID.call(globalThis.crypto);
  idCounter += 1;
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}-${idCounter}`;
}
