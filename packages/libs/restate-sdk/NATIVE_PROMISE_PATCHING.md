# Native Promise Combinator Patching

This SDK exposes integration APIs that let an external library patch native Promise combinators (`race`, `any`, `all`, `allSettled`) while preserving replay determinism for Restate promises.

## APIs

Import from `@restatedev/restate-sdk`:

- `createNativeCombinatorScope(inputs)`
- `detachPendingNativeCombinatorInputs(inputs)`
- `linkNativePromiseToRestatePromise(nativePromise, sourcePromise)`
- `toTrackedNativePromise(value)`

## Required patch flow

For every patched native combinator call:

1. Snapshot inputs first (`Array.from(iterable)`).
2. Convert each input to a native promise and preserve Restate linkage.
3. Create a scope with those converted inputs.
4. Call the original combinator.
5. On settle (`finally`), call `scope.detachPending()`.

`scope.detachPending()` must always run, for both resolve and reject paths.

## Reference patch skeleton

```ts
import {
  createNativeCombinatorScope,
  linkNativePromiseToRestatePromise,
} from "@restatedev/restate-sdk";

type NativeCombinatorName = "race" | "any" | "all" | "allSettled";

function trackInput(value: unknown): Promise<unknown> {
  const native = Promise.resolve(value as PromiseLike<unknown>);
  return linkNativePromiseToRestatePromise(
    native,
    value as PromiseLike<unknown>
  ) as Promise<unknown>;
}

function patchCombinator(name: NativeCombinatorName): void {
  const original = Promise[name] as (iterable: Iterable<unknown>) => Promise<unknown>;

  Object.defineProperty(Promise, name, {
    configurable: true,
    writable: true,
    value(iterable: Iterable<unknown>) {
      const inputs = Array.from(iterable);
      const trackedInputs = inputs.map(trackInput);
      const scope = createNativeCombinatorScope(trackedInputs);

      const result = original.call(Promise, trackedInputs);
      return result.finally(() => {
        scope.detachPending();
      });
    },
  });
}

patchCombinator("race");
patchCombinator("any");
patchCombinator("all");
patchCombinator("allSettled");
```

## When to use `linkNativePromiseToRestatePromise` vs `toTrackedNativePromise`

- Use `toTrackedNativePromise(x)` when you directly wrap Restate promises into native promises.
- Use `linkNativePromiseToRestatePromise(native, source)` when you already have a conversion wrapper and need to explicitly attach the source Restate promise.

Example:

```ts
async function toPromise<T>(value: PromiseLike<T>): Promise<T> {
  return await value;
}

const rp = ctx.serviceClient(svc).call();
const native = linkNativePromiseToRestatePromise(toPromise(rp), rp);
```

## Notes

- Call this patch once during process bootstrap, before handlers execute.
- Nested combinators are supported as long as all target combinators are patched.
- `scope.detachPending()` is idempotent and safe to call after settlement.
