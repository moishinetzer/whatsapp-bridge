# Effect v4 notes

What this codebase leans on, and the traps met while building it against `effect@4.0.0-rc.109`. Verified against the installed `node_modules/effect/dist/*.d.ts`; training-data knowledge of Effect is mostly v3 and wrong here.

## Shape of the app

- **Services** are `class X extends Context.Service<X, Shape>()("bridge/X") {}`; implementations are `Layer.effect(X, Effect.gen(...))` returning `X.of({...})`. Long-lived work (the socket supervisor, the inbound consumer, per-chat workers) is forked into the layer scope with `Effect.forkScoped` / `Effect.forkIn(scope)`, so it lives exactly as long as the layer and is interrupted on shutdown.
- **Errors** are `Schema.TaggedError<Self>()("Tag", fields, { httpApiStatus })`; the same classes are the API's error schemas, so the wire shape, the OpenAPI document and the typed client all come from one definition.
- **Schedules** drive every retry: reconnects (`Schedule.exponential` + `jittered` + `modifyDelay` cap, reset after a healthy run), outbound sends (`upTo({ times: 5 })`), credential saves. Nothing sleeps in a loop by hand.
- **Interruption is the interrupt feature.** A turn is a child fiber; the chat worker races `Fiber.await(turn)` against a `Deferred` the inbound handler completes. Winning the race interrupts the turn, which aborts an in-flight HTTP call to Intro's handler. The commit (send reply, clear 👀) sits in `Effect.uninterruptibleMask`, so an interrupt that lands mid-send waits for it and the newer message starts a fresh turn.
- **Callback boundaries** (Baileys' event emitter) go through a `Queue` with `Queue.offerUnsafe`, then a forked `Stream.runForEach` handles them inside Effect.
- **HTTP** is `HttpApi` + `HttpApiBuilder` on the server and `HttpApiClient` on the CLI; SSE is `HttpApiSchema.StreamSse({ data })` and the handler simply returns a `Stream`.
- **Tests** use `@effect/vitest`'s `it.effect` (which provides `TestClock`), an in-memory transport, a scriptable responder, and `Deferred`/`Queue`/`Latch` for synchronisation instead of sleeps.

## Traps

1. **Structural equality swallows writes.** `Equal.equals` compares plain objects structurally in v4, and `HashMap.set` returns the *same* map when the new value is `Equal` to the old one. Two fresh `Deferred`s are structurally identical (both `{ effect: undefined, resumes: undefined }`), so storing `{ ...state, wake: newDeferred }` in a `HashMap` silently kept the old Deferred and the running turn could never be woken. Fix: a `generation` counter on the state record (see `ChatState.generation` in `src/engine/Turns.ts`). Any state that carries handles (Deferred, Latch, Fiber) in a `HashMap` needs the same.
2. **SSE headers only flush with the first chunk.** A stream that has nothing to say yet means the client's `fetch` never resolves. The events endpoint emits a `Ready` event immediately (which doubles as the cursor for `?since=`), plus a `Heartbeat` every 25 seconds.
3. **`TestConsole` swallows `Console.log` in `it.effect`.** Use `console.log` when debugging a test.
4. **`Effect.fork` is `Effect.forkChild`** in v4; forks start on the next scheduler tick unless `startImmediately: true`. Tests that need forked fibers to have run use `Effect.yieldNow` (see `settle` in `test/support/fakes.ts`) or better, wait on an observable queue.
5. **`Schema.TaggedError`, not `TaggedErrorClass`**; `Schema.Literals([...])` and `Schema.Union([...])` take arrays; `Schema.check(Schema.isNonEmpty())` for refinements; `Schema.DateTimeUtcFromString` for ISO timestamps on the wire.
6. **`Layer.scoped` does not exist**; `Layer.effect` already runs in the layer scope, so `Effect.scope`, `Effect.addFinalizer` and `Effect.forkScoped` work inside it.
7. **`Effect.retry({ schedule, while })`**: the schedule's input type must match the error channel, so narrow the error first (the socket supervisor converts `AuthStoreError` into a terminal outcome before retrying on `ReconnectNeeded`).
8. **`TestClock` sleeps of zero return immediately**, which is why tests default to `batchWindow: 0` and only the batching test advances the clock.

## Where to look things up

`node_modules/effect/dist/<Module>.d.ts` is the truth for this version. `effect/unstable/http`, `effect/unstable/httpapi` and `effect/unstable/cli` are the subpaths used here; `@effect/platform-node` supplies `NodeHttpServer`, `NodeServices` and `NodeRuntime`.
