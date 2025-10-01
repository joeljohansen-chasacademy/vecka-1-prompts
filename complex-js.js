/**
 * createAdvancedTaskOrchestrator(options)
 *
 * En kraftfull task-orchestrator som kör asynkrona "tasks" med:
 * - beroendehantering (deps)
 * - prioriterad kö
 * - concurrency (antal parallella workers)
 * - rate limiting (max per tidsfönster)
 * - retries med backoff och jitter
 * - per-task timeout och AbortSignal
 * - result caching med TTL
 * - events (on/off/emit)
 * - snapshot/persist hooks
 *
 * Usage:
 *   const orchestrator = createAdvancedTaskOrchestrator({ concurrency: 4 });
 *   orchestrator.addTask({ id: 'A', run: async ({signal}) => {...} });
 *   orchestrator.start();
 *
 * OBS: varje task måste erbjuda en `run`-funktion som tar ett objekt { signal, meta } och returnerar en Promise.
 */
function createAdvancedTaskOrchestrator(options = {}) {
	// --- Konfiguration och defaults ---
	const config = {
		concurrency: options.concurrency ?? 4,
		rateLimit: options.rateLimit ?? { max: 100, per: 60_000 }, // max tasks per `per` ms
		defaultTimeout: options.defaultTimeout ?? 30_000, // ms
		defaultRetries: options.defaultRetries ?? 2,
		defaultBackoff: options.defaultBackoff ?? {
			base: 200,
			type: "exponential",
			cap: 30_000,
			jitter: true,
		},
		cacheTTL: options.cacheTTL ?? 60_000,
		persistenceHook:
			typeof options.persistenceHook === "function"
				? options.persistenceHook
				: null,
	};

	// --- Interna datastrukturer ---
	const tasks = new Map(); // id -> task meta
	const dependents = new Map(); // id -> Set(of tasks that depend on id)
	const readyQueue = new PriorityQueue();
	const cache = new Map(); // cacheKey -> { value, expiresAt }
	const rateTimestamps = []; // sliding window timestamps (ms)
	let activeCount = 0;
	let running = false;
	let paused = false;
	let seq = 0; // för FIFO inom samma priority
	const listeners = new Map(); // eventName -> Set(callback)
	const metrics = {
		started: 0,
		succeeded: 0,
		failed: 0,
		cancelled: 0,
		totalRunTime: 0,
	};

	// --- EventEmitter-API ---
	function on(event, cb) {
		if (!listeners.has(event)) listeners.set(event, new Set());
		listeners.get(event).add(cb);
		return () => off(event, cb);
	}
	function off(event, cb) {
		if (!listeners.has(event)) return;
		listeners.get(event).delete(cb);
	}
	function emit(event, payload) {
		if (!listeners.has(event)) return;
		for (const cb of Array.from(listeners.get(event))) {
			try {
				cb(payload);
			} catch (e) {
				/* swallow handler errors */
			}
		}
	}

	// --- PriorityQueue (min-heap where higher priority number == earlier) ---
	function PriorityQueue() {
		this._heap = [];
	}
	PriorityQueue.prototype.size = function () {
		return this._heap.length;
	};
	PriorityQueue.prototype._swap = function (i, j) {
		[this._heap[i], this._heap[j]] = [this._heap[j], this._heap[i]];
	};
	PriorityQueue.prototype._compare = function (a, b) {
		// higher priority value => earlier, if equal use seq (lower seq => earlier)
		if (a.priority !== b.priority) return a.priority > b.priority;
		return a.seq < b.seq;
	};
	PriorityQueue.prototype.push = function (item) {
		this._heap.push(item);
		let idx = this._heap.length - 1;
		while (idx > 0) {
			const parent = Math.floor((idx - 1) / 2);
			if (this._compare(this._heap[idx], this._heap[parent])) {
				this._swap(idx, parent);
				idx = parent;
			} else break;
		}
	};
	PriorityQueue.prototype.pop = function () {
		if (this._heap.length === 0) return null;
		const top = this._heap[0];
		const last = this._heap.pop();
		if (this._heap.length > 0) {
			this._heap[0] = last;
			// sift down
			let idx = 0;
			while (true) {
				const left = 2 * idx + 1;
				const right = left + 1;
				let candidate = idx;
				if (
					left < this._heap.length &&
					this._compare(this._heap[left], this._heap[candidate])
				)
					candidate = left;
				if (
					right < this._heap.length &&
					this._compare(this._heap[right], this._heap[candidate])
				)
					candidate = right;
				if (candidate === idx) break;
				this._swap(idx, candidate);
				idx = candidate;
			}
		}
		return top;
	};

	// --- Hjälpfunktioner ---
	function now() {
		return Date.now();
	}
	function sleep(ms) {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	function calculateBackoff(attempt) {
		const b = config.defaultBackoff;
		let delay = b.base * Math.pow(2, attempt - 1);
		if (b.cap) delay = Math.min(delay, b.cap);
		if (b.jitter) {
			const jitter = Math.random() * delay * 0.3; // upp till 30% jitter
			delay = delay - jitter + Math.random() * jitter;
		}
		return Math.max(0, Math.floor(delay));
	}

	function rateAllows() {
		const nowMs = now();
		const windowStart = nowMs - config.rateLimit.per;
		while (rateTimestamps.length && rateTimestamps[0] < windowStart)
			rateTimestamps.shift();
		return rateTimestamps.length < config.rateLimit.max;
	}

	function touchRate() {
		rateTimestamps.push(now());
	}

	// --- Cycle detection (DFS) vid inläggning av tasks ---
	function detectCycle(startId, visiting = new Set(), stack = []) {
		if (visiting.has(startId)) {
			const cycleStart = stack.indexOf(startId);
			const cyclePath = stack.slice(cycleStart).concat([startId]);
			return cyclePath;
		}
		if (!tasks.has(startId)) return null;
		visiting.add(startId);
		stack.push(startId);
		const deps = tasks.get(startId).deps || [];
		for (const d of deps) {
			const c = detectCycle(d, visiting, stack);
			if (c) return c;
		}
		visiting.delete(startId);
		stack.pop();
		return null;
	}

	// --- Add task ---
	/**
	 * taskSpec: {
	 *   id: string,
	 *   run: async ({signal, meta}) => any,
	 *   deps?: string[],
	 *   priority?: number,
	 *   retries?: number,
	 *   timeout?: number,
	 *   cacheKey?: string
	 * }
	 */
	function addTask(taskSpec) {
		if (!taskSpec || !taskSpec.id || typeof taskSpec.run !== "function") {
			throw new TypeError("Task must have id and run() function.");
		}
		if (tasks.has(taskSpec.id))
			throw new Error(`Task id "${taskSpec.id}" already exists.`);
		const meta = {
			id: taskSpec.id,
			run: taskSpec.run,
			deps: Array.isArray(taskSpec.deps) ? Array.from(taskSpec.deps) : [],
			priority: typeof taskSpec.priority === "number" ? taskSpec.priority : 0,
			retries:
				typeof taskSpec.retries === "number"
					? taskSpec.retries
					: config.defaultRetries,
			timeout:
				typeof taskSpec.timeout === "number"
					? taskSpec.timeout
					: config.defaultTimeout,
			cacheKey:
				typeof taskSpec.cacheKey === "string" ? taskSpec.cacheKey : null,
			state: "idle", // idle | queued | running | succeeded | failed | cancelled
			attempts: 0,
			result: undefined,
			error: undefined,
			_controller: null,
			_resolve: null,
			_reject: null,
			_promise: null,
			createdAt: now(),
		};
		// prepare promise for external awaiting
		meta._promise = new Promise((resolve, reject) => {
			meta._resolve = resolve;
			meta._reject = reject;
		});
		tasks.set(meta.id, meta);

		// register dependents
		for (const d of meta.deps) {
			if (!dependents.has(d)) dependents.set(d, new Set());
			dependents.get(d).add(meta.id);
		}

		// cycle check: if there's a cycle that involves this task, reject add
		const cycle = detectCycle(meta.id);
		if (cycle) {
			// cleanup
			for (const d of meta.deps) dependents.get(d)?.delete(meta.id);
			tasks.delete(meta.id);
			throw new Error(`Dependency cycle detected: ${cycle.join(" -> ")}`);
		}

		// if no deps => ready immediately
		if (!meta.deps.length) enqueue(meta);

		persistStateIfNeeded();
		return meta._promise;
	}

	function enqueue(meta) {
		if (meta.state === "succeeded" || meta.state === "running") return;
		meta.state = "queued";
		readyQueue.push({ priority: meta.priority, seq: seq++, taskId: meta.id });
		emit("task:queued", { id: meta.id, meta });
		tryStartNext();
	}

	// --- Start/Stop/Pause/Resume ---
	function start() {
		if (running) return;
		running = true;
		paused = false;
		emit("orchestrator:start");
		tryStartNext();
	}
	function stop() {
		running = false;
		emit("orchestrator:stop");
	}
	function pause() {
		paused = true;
		emit("orchestrator:pause");
	}
	function resume() {
		paused = false;
		emit("orchestrator:resume");
		tryStartNext();
	}

	// --- Cancel task ---
	function cancelTask(id) {
		if (!tasks.has(id)) return false;
		const t = tasks.get(id);
		if (
			t.state === "succeeded" ||
			t.state === "failed" ||
			t.state === "cancelled"
		)
			return false;
		if (t._controller) {
			try {
				t._controller.abort();
			} catch (e) {
				/* ignore */
			}
		}
		t.state = "cancelled";
		metrics.cancelled++;
		t._reject?.(new Error("cancelled"));
		emit("task:cancelled", { id });
		persistStateIfNeeded();
		return true;
	}

	// --- Run loop ---
	async function tryStartNext() {
		if (!running || paused) return;
		// loop while we can start new tasks
		while (
			activeCount < config.concurrency &&
			readyQueue.size() > 0 &&
			rateAllows()
		) {
			const job = readyQueue.pop();
			const meta = tasks.get(job.taskId);
			if (!meta) continue; // missing (shouldn't happen)
			// double-check deps (some may have failed/cancelled)
			const depsOk = meta.deps.every(
				(d) => tasks.has(d) && tasks.get(d).state === "succeeded"
			);
			if (!depsOk) {
				// if any deps are failed/cancelled -> fail this one
				const failedDep = meta.deps.find(
					(d) => !tasks.has(d) || tasks.get(d).state !== "succeeded"
				);
				if (failedDep) {
					meta.state = "failed";
					meta.error = new Error(`Dependency "${failedDep}" did not succeed.`);
					metrics.failed++;
					meta._reject?.(meta.error);
					emit("task:failed", { id: meta.id, error: meta.error });
					persistStateIfNeeded();
					// propagate to its dependents
					for (const depId of dependents.get(meta.id) ?? []) {
						if (tasks.has(depId)) enqueue(tasks.get(depId)); // they will detect failed deps later
					}
					continue;
				} else {
					// not ready yet: maybe re-enqueue later
					enqueue(meta);
					break; // break loop to avoid busy spin
				}
			}
			// all checks passed -> start it
			activeCount++;
			touchRate();
			runTask(meta).finally(() => {
				activeCount--;
				tryStartNext();
			});
		}
	}

	async function runTask(meta) {
		if (meta.state === "succeeded" || meta.state === "running") return;
		meta.state = "running";
		meta.attempts++;
		meta._controller = new AbortController();
		const signal = meta._controller.signal;
		emit("task:started", { id: meta.id, attempt: meta.attempts });
		metrics.started++;
		const startTime = now();

		// caching
		if (meta.cacheKey) {
			const cached = cache.get(meta.cacheKey);
			if (cached && cached.expiresAt > now()) {
				meta.state = "succeeded";
				meta.result = cached.value;
				metrics.succeeded++;
				metrics.totalRunTime += 0;
				meta._resolve?.(meta.result);
				emit("task:succeeded", {
					id: meta.id,
					cached: true,
					result: meta.result,
				});
				// notify dependents
				for (const d of dependents.get(meta.id) ?? []) maybeEnqueueDependent(d);
				persistStateIfNeeded();
				return;
			}
		}

		// Wrap the user's run with timeout
		let timeoutId = null;
		const timeoutMs = meta.timeout ?? config.defaultTimeout;
		const timeoutPromise = new Promise((_, reject) => {
			timeoutId = setTimeout(() => {
				try {
					meta._controller.abort();
				} catch (e) {}
				reject(new Error("task-timeout"));
			}, timeoutMs);
		});

		try {
			const result = await Promise.race([
				Promise.resolve().then(() =>
					meta.run({ signal, meta: { id: meta.id } })
				),
				timeoutPromise,
			]);
			clearTimeout(timeoutId);
			meta.state = "succeeded";
			meta.result = result;
			metrics.succeeded++;
			metrics.totalRunTime += now() - startTime;
			if (meta.cacheKey)
				cache.set(meta.cacheKey, {
					value: result,
					expiresAt: now() + config.cacheTTL,
				});
			meta._resolve?.(result);
			emit("task:succeeded", { id: meta.id, result });
			// enqueue dependents that now become ready
			for (const d of dependents.get(meta.id) ?? []) maybeEnqueueDependent(d);
		} catch (err) {
			clearTimeout(timeoutId);
			meta.error = err;
			// if it was an abort triggered by cancelTask, mark cancelled
			if (err && err.name === "AbortError") {
				meta.state = "cancelled";
				metrics.cancelled++;
				meta._reject?.(err);
				emit("task:cancelled", { id: meta.id, reason: err });
			} else if (meta.attempts <= meta.retries) {
				// schedule retry with backoff
				meta.state = "queued";
				const backoffDelay = calculateBackoff(meta.attempts);
				emit("task:retry", {
					id: meta.id,
					attempt: meta.attempts,
					delay: backoffDelay,
					error: err,
				});
				await sleep(backoffDelay);
				enqueue(meta);
			} else {
				meta.state = "failed";
				metrics.failed++;
				meta._reject?.(err);
				emit("task:failed", { id: meta.id, error: err });
				// propagate: dependents will fail when checking deps
				for (const d of dependents.get(meta.id) ?? []) {
					if (tasks.has(d)) enqueue(tasks.get(d));
				}
			}
		} finally {
			persistStateIfNeeded();
		}
	}

	function maybeEnqueueDependent(depId) {
		const depMeta = tasks.get(depId);
		if (!depMeta) return;
		const depsOk = depMeta.deps.every(
			(d) => tasks.has(d) && tasks.get(d).state === "succeeded"
		);
		if (depsOk) enqueue(depMeta);
	}

	// --- Snapshot/persist support ---
	function snapshot() {
		// produce a minimal serializable snapshot (no functions, no controllers)
		const snap = {
			config,
			createdAt: now(),
			tasks: Array.from(tasks.values()).map((t) => ({
				id: t.id,
				deps: t.deps,
				priority: t.priority,
				retries: t.retries,
				timeout: t.timeout,
				cacheKey: t.cacheKey,
				state: t.state,
				attempts: t.attempts,
				result: t.state === "succeeded" ? t.result : undefined,
				error:
					t.state === "failed"
						? t.error
							? String(t.error)
							: "failed"
						: undefined,
				createdAt: t.createdAt,
			})),
			metrics,
			rateTimestamps: [...rateTimestamps],
			now: now(),
		};
		return snap;
	}

	function persistStateIfNeeded() {
		if (config.persistenceHook) {
			try {
				config.persistenceHook(snapshot());
			} catch (e) {
				/* ignore hook errors */
			}
		}
	}

	// --- Helpers: query state, clear cache, waitForAll ---
	function getTaskState(id) {
		return tasks.has(id) ? { ...tasks.get(id) } : null;
	}
	function clearCache(key) {
		if (typeof key === "undefined") cache.clear();
		else cache.delete(key);
	}
	function waitForAll(timeoutMs) {
		return new Promise((resolve, reject) => {
			const check = () => {
				const nonTerminal = Array.from(tasks.values()).filter(
					(t) => !["succeeded", "failed", "cancelled"].includes(t.state)
				);
				if (nonTerminal.length === 0) return resolve(snapshot());
				return false;
			};
			if (check()) return;
			const offStart = on("task:succeeded", check);
			const offFail = on("task:failed", check);
			const offCancel = on("task:cancelled", check);
			let to = null;
			if (typeof timeoutMs === "number") {
				to = setTimeout(() => {
					offStart();
					offFail();
					offCancel();
					reject(new Error("waitForAll-timeout"));
				}, timeoutMs);
			}
			// when resolved or rejected handlers will be removed automatically by the resolve/reject paths
		});
	}

	// --- Public API ---
	return {
		addTask,
		start,
		stop,
		pause,
		resume,
		cancelTask,
		getTaskState,
		on,
		off,
		snapshot,
		clearCache,
		waitForAll,
		getMetrics: () => ({ ...metrics }),
		_internal: {
			/* exposed for testing/debugging only */ tasks,
			dependents,
			readyQueue,
			cache,
		},
	};
}

/* ---------------------------------------------------------
   Exempel på användning (kopiera och kör i en Node/Browser-miljö)
--------------------------------------------------------- */

// Exempel (icke-körbart här direkt i funktionen utan extern körning):

const orch = createAdvancedTaskOrchestrator({
	concurrency: 3,
	rateLimit: { max: 5, per: 2000 },
	defaultRetries: 3,
	cacheTTL: 20_000,
	persistenceHook: (snap) => {
		console.log("Persist snapshot", snap);
	},
});

orch.on("task:succeeded", ({ id, result, cached }) =>
	console.log("succeeded", id, { cached, result })
);
orch.on("task:failed", ({ id, error }) => console.log("failed", id, error));
orch.on("task:retry", ({ id, attempt, delay }) =>
	console.log("retry", id, attempt, delay)
);

async function fakeWork(name, ms, succeed = true) {
	return new Promise((res, rej) => {
		setTimeout(
			() => (succeed ? res(`done:${name}`) : rej(new Error("boom:" + name))),
			ms
		);
	});
}

orch.addTask({ id: "A", run: async ({ signal }) => fakeWork("A", 400) });
orch.addTask({
	id: "B",
	deps: ["A"],
	run: async ({ signal }) => fakeWork("B", 600),
});
orch.addTask({
	id: "C",
	deps: ["A"],
	run: async ({ signal }) => fakeWork("C", 200),
});
orch.addTask({
	id: "D",
	deps: ["B", "C"],
	run: async ({ signal }) => fakeWork("D", 300),
});

orch.start();
