/*
 * Copyright (c) 2023-2025 - Restate Software, Inc., Restate GmbH
 *
 * This file is part of the Restate SDK for Node.js/TypeScript,
 * which is released under the MIT license.
 *
 * You can find a copy of the license in file LICENSE in the root
 * directory of this repository or package, or at
 * https://github.com/restatedev/sdk-typescript/blob/main/LICENSE
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

import type {
  RestatePromise,
  InvocationId,
  InvocationPromise,
} from "./context.js";
import type * as vm from "./endpoint/handlers/vm/sdk_shared_core_wasm_bindings.js";
import {
  CancelledError,
  RestateError,
  TerminalError,
  TimeoutError,
} from "./types/errors.js";
import { CompletablePromise } from "./utils/completable_promise.js";
import type { ContextImpl, RunClosuresTracker } from "./context_impl.js";
import { setImmediate } from "node:timers/promises";
import type { InputPump, OutputPump } from "./io.js";
import type { Duration } from "@restatedev/restate-sdk-core";

// A promise that is never completed
export function pendingPromise<T>(): Promise<T> {
  return new Promise<T>(() => {});
}

// ------ Restate promises ------
// These promises are "proxy promises" that will be handed over to the user,
// and moved forward by the PromiseExecutor below when the user awaits on them.

enum PromiseState {
  COMPLETED,
  NOT_COMPLETED,
}

export const RESTATE_CTX_SYMBOL = Symbol("restateContext");

export interface InternalRestatePromise<T> extends RestatePromise<T> {
  [RESTATE_CTX_SYMBOL]: ContextImpl;

  tryCancel(): void;
  tryDetach(): void;
  tryFail(error: unknown): void;
  tryComplete(): Promise<void>;
  uncompletedLeaves(): Array<number>;
  publicPromise(): Promise<T>;
}

export type AsyncResultValue =
  | "Empty"
  | { Success: Uint8Array }
  | { Failure: vm.WasmFailure }
  | { StateKeys: string[] }
  | { InvocationId: string };

export function extractContext(n: any): ContextImpl | undefined {
  if (
    n === null ||
    n === undefined ||
    (typeof n !== "object" && typeof n !== "function")
  ) {
    return undefined;
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
  return n[RESTATE_CTX_SYMBOL] as ContextImpl | undefined;
}

const LINKED_NATIVE_PROMISES = new WeakMap<
  object,
  InternalRestatePromise<unknown>
>();

export interface NativeCombinatorScope {
  detachPending(): void;
}

/**
 * Create a scope used by external Promise combinator patchers.
 * The patcher should call `detachPending()` when the patched native combinator settles.
 */
export function createNativeCombinatorScope(
  inputs: Iterable<unknown>
): NativeCombinatorScope {
  const snapshot = Array.from(inputs);
  return {
    detachPending() {
      detachPendingNativeCombinatorInputs(snapshot);
    },
  };
}

/**
 * Links a native promise to a Restate promise source, so an external combinator patcher
 * can detach unresolved losers even when native promises were wrapped/converted.
 */
export function linkNativePromiseToRestatePromise<T>(
  nativePromise: PromiseLike<T>,
  sourcePromise: PromiseLike<unknown>
): PromiseLike<T> {
  const source = asInternalRestatePromise(sourcePromise);
  if (source === undefined) {
    return nativePromise;
  }
  if (
    nativePromise !== null &&
    nativePromise !== undefined &&
    (typeof nativePromise === "object" || typeof nativePromise === "function")
  ) {
    LINKED_NATIVE_PROMISES.set(nativePromise as object, source);
  }
  return nativePromise;
}

/**
 * Convenience helper for converting to a native Promise while preserving a link
 * back to the originating Restate promise.
 */
export function toTrackedNativePromise<T>(value: PromiseLike<T>): Promise<T> {
  const nativePromise = Promise.resolve(value);
  linkNativePromiseToRestatePromise(nativePromise, value);
  return nativePromise;
}

/**
 * Detaches unresolved Restate promises referenced by native combinator inputs.
 * Intended to be invoked by an external native Promise combinator patch when
 * the combinator settles.
 */
export function detachPendingNativeCombinatorInputs(
  inputs: Iterable<unknown>
): void {
  const seen = new Set<InternalRestatePromise<unknown>>();
  for (const input of inputs) {
    const restatePromise = asInternalRestatePromise(input);
    if (restatePromise === undefined || seen.has(restatePromise)) {
      continue;
    }
    seen.add(restatePromise);
    if (restatePromise.uncompletedLeaves().length === 0) {
      continue;
    }
    restatePromise.tryDetach();
  }
}

function asInternalRestatePromise(
  value: unknown
): InternalRestatePromise<unknown> | undefined {
  if (
    value === null ||
    value === undefined ||
    (typeof value !== "object" && typeof value !== "function")
  ) {
    return undefined;
  }

  const asObject = value as object;
  const linked = LINKED_NATIVE_PROMISES.get(asObject);
  if (linked !== undefined) {
    return linked;
  }

  if (extractContext(value) === undefined) {
    return undefined;
  }
  return value as InternalRestatePromise<unknown>;
}

abstract class AbstractRestatePromise<T> implements InternalRestatePromise<T> {
  [RESTATE_CTX_SYMBOL]: ContextImpl;
  private pollingPromise?: Promise<any>;
  private cancelPromise: CompletablePromise<any> = new CompletablePromise();
  private failurePromise: CompletablePromise<any> = new CompletablePromise();

  protected constructor(ctx: ContextImpl) {
    this[RESTATE_CTX_SYMBOL] = ctx;
  }

  // --- Promise methods

  then<TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null
  ): Promise<TResult1 | TResult2> {
    this.ensurePolling();
    return this.publicPromiseOrCancelPromise().then(onfulfilled, onrejected);
  }

  catch<TResult = never>(
    onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | null
  ): Promise<T | TResult> {
    this.ensurePolling();
    return this.publicPromiseOrCancelPromise().catch(onrejected);
  }

  finally(onfinally?: (() => void) | null): Promise<T> {
    this.ensurePolling();
    return this.publicPromiseOrCancelPromise().finally(onfinally);
  }

  private publicPromiseOrCancelPromise(): Promise<T> {
    return Promise.race([
      this.cancelPromise.promise as Promise<T>,
      this.failurePromise.promise as Promise<T>,
      this.publicPromise(),
    ]);
  }

  private ensurePolling(): void {
    if (this.pollingPromise !== undefined) {
      return;
    }

    const progressPromise = this[RESTATE_CTX_SYMBOL].promisesExecutor
      .doProgress(this)
      .catch(() => {});

    this.pollingPromise = progressPromise.finally(() => {
      this.pollingPromise = undefined;
    });
  }

  // --- RestatePromise methods

  orTimeout(duration: number | Duration): RestatePromise<T> {
    return new RestateCombinatorPromise(
      this[RESTATE_CTX_SYMBOL],
      ([thisPromise, sleepPromise]) => {
        return new Promise((resolve, reject) => {
          thisPromise!.then(resolve, reject);
          sleepPromise!.then(() => {
            reject(new TimeoutError());
          }, reject);
        });
      },
      [
        this,
        this[RESTATE_CTX_SYMBOL].sleep(duration) as InternalRestatePromise<any>,
      ]
    ) as RestatePromise<T>;
  }

  map<U>(mapper: (value?: T, failure?: TerminalError) => U): RestatePromise<U> {
    return new RestateMappedPromise(this[RESTATE_CTX_SYMBOL], this, mapper);
  }

  tryCancel() {
    this.cancelPromise.reject(new CancelledError());
  }

  tryDetach() {
    this[RESTATE_CTX_SYMBOL].promisesExecutor.detachPromise(this);
  }

  tryFail(error: unknown) {
    this.failurePromise.reject(error);
  }

  abstract tryComplete(): Promise<void>;

  abstract uncompletedLeaves(): Array<number>;

  abstract publicPromise(): Promise<T>;

  abstract [Symbol.toStringTag]: string;
}

export class RestateSinglePromise<T> extends AbstractRestatePromise<T> {
  private state: PromiseState = PromiseState.NOT_COMPLETED;
  private completablePromise: CompletablePromise<T> = new CompletablePromise();

  constructor(
    ctx: ContextImpl,
    readonly handle: number,
    private readonly completer: (
      value: AsyncResultValue,
      prom: CompletablePromise<T>
    ) => Promise<void>
  ) {
    super(ctx);
  }

  uncompletedLeaves(): number[] {
    return this.state === PromiseState.COMPLETED ? [] : [this.handle];
  }

  async tryComplete(): Promise<void> {
    if (this.state === PromiseState.COMPLETED) {
      return;
    }
    const notification = this[RESTATE_CTX_SYMBOL].coreVm.take_notification(
      this.handle
    );
    if (notification === "NotReady") {
      return;
    }
    this.state = PromiseState.COMPLETED;
    await this.completer(notification, this.completablePromise);
  }

  publicPromise(): Promise<T> {
    return this.completablePromise.promise;
  }

  readonly [Symbol.toStringTag] = "RestateSinglePromise";
}

export class RestateInvocationPromise<T>
  extends RestateSinglePromise<T>
  implements InvocationPromise<T>
{
  constructor(
    ctx: ContextImpl,
    handle: number,
    completer: (
      value: AsyncResultValue,
      prom: CompletablePromise<T>
    ) => Promise<void>,
    private readonly invocationIdPromise: Promise<InvocationId>
  ) {
    super(ctx, handle, completer);
  }

  get invocationId(): Promise<InvocationId> {
    return this.invocationIdPromise;
  }
}

export class RestateCombinatorPromise extends AbstractRestatePromise<any> {
  private state: PromiseState = PromiseState.NOT_COMPLETED;
  private readonly combinatorPromise: Promise<any>;

  constructor(
    ctx: ContextImpl,
    combinatorConstructor: (promises: Promise<any>[]) => Promise<any>,
    readonly childs: Array<InternalRestatePromise<any>>
  ) {
    super(ctx);
    this.combinatorPromise = combinatorConstructor(
      childs.map((p) => p.publicPromise())
    ).finally(() => {
      this.state = PromiseState.COMPLETED;
    });
  }

  uncompletedLeaves(): number[] {
    return this.state === PromiseState.COMPLETED
      ? []
      : this.childs.flatMap((p) => p.uncompletedLeaves());
  }

  async tryComplete(): Promise<void> {
    await Promise.allSettled(this.childs.map((c) => c.tryComplete()));
  }

  publicPromise(): Promise<unknown> {
    return this.combinatorPromise;
  }

  readonly [Symbol.toStringTag] = "RestateCombinatorPromise";
}

export class RestatePendingPromise<T> implements InternalRestatePromise<T> {
  [RESTATE_CTX_SYMBOL]: ContextImpl;

  constructor(ctx: ContextImpl) {
    this[RESTATE_CTX_SYMBOL] = ctx;
  }

  // --- Promise methods

  then<TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null
  ): Promise<TResult1 | TResult2> {
    return pendingPromise<T>().then(onfulfilled, onrejected);
  }

  catch<TResult = never>(
    onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | null
  ): Promise<T | TResult> {
    return pendingPromise<T>().catch(onrejected);
  }

  finally(onfinally?: (() => void) | null): Promise<T> {
    return pendingPromise<T>().finally(onfinally);
  }

  // --- RestatePromise methods

  orTimeout(): RestatePromise<T> {
    return this;
  }

  map<U>(): RestatePromise<U> {
    return this as unknown as RestatePromise<U>;
  }

  tryCancel(): void {}
  tryDetach(): void {}
  tryFail(): void {}
  async tryComplete(): Promise<void> {}
  uncompletedLeaves(): number[] {
    return [];
  }
  publicPromise(): Promise<T> {
    return pendingPromise<T>();
  }

  readonly [Symbol.toStringTag] = "RestatePendingPromise";
}

export class InvocationPendingPromise<T>
  extends RestatePendingPromise<T>
  implements InvocationPromise<T>
{
  constructor(ctx: ContextImpl) {
    super(ctx);
  }

  get invocationId(): Promise<InvocationId> {
    return pendingPromise();
  }
}

export class RestateMappedPromise<T, U> extends AbstractRestatePromise<U> {
  private publicPromiseMapper: (
    value?: T,
    failure?: TerminalError
  ) => Promise<U>;

  constructor(
    ctx: ContextImpl,
    readonly inner: InternalRestatePromise<T>,
    mapper: (value?: T, failure?: TerminalError) => U
  ) {
    super(ctx);
    this.publicPromiseMapper = (value?: T, failure?: TerminalError) => {
      try {
        return Promise.resolve(mapper(value, failure));
      } catch (e) {
        if (e instanceof TerminalError) {
          return Promise.reject(e);
        } else {
          ctx.handleInvocationEndError(e);
          return pendingPromise();
        }
      }
    };
  }

  async tryComplete(): Promise<void> {
    await this.inner.tryComplete();
  }

  uncompletedLeaves(): number[] {
    return this.inner.uncompletedLeaves();
  }

  publicPromise(): Promise<U> {
    const promiseMapper = this.publicPromiseMapper;
    return this.inner.publicPromise().then(
      (t) => promiseMapper(t, undefined),
      (error) => {
        if (error instanceof RestateError) {
          return promiseMapper(undefined, error);
        } else {
          // Something else, just re-throw it
          throw error;
        }
      }
    );
  }

  readonly [Symbol.toStringTag] = "RestateMappedPromise";
}

/**
 * Promises executor, gluing VM with I/O and Promises given to user space.
 */
export class PromisesExecutor {
  private readonly trackedPromises = new Set<InternalRestatePromise<unknown>>();
  private readonly progressWaiters = new Map<
    InternalRestatePromise<unknown>,
    CompletablePromise<void>
  >();
  private progressLoop?: Promise<void>;

  constructor(
    private readonly coreVm: vm.WasmVM,
    private readonly inputPump: InputPump,
    private readonly outputPump: OutputPump,
    private readonly runClosuresTracker: RunClosuresTracker,
    private readonly errorCallback: (e: any) => void
  ) {}

  async doProgress(restatePromise: InternalRestatePromise<unknown>) {
    if (!this.progressWaiters.has(restatePromise)) {
      this.trackedPromises.add(restatePromise);
      this.progressWaiters.set(restatePromise, new CompletablePromise<void>());
      this.ensureProgressLoop();
    }

    // Keep flushing output while user code is awaiting on promises.
    await this.outputPump.awaitNextProgress();

    return this.progressWaiters.get(restatePromise)!.promise;
  }

  detachPromise(restatePromise: InternalRestatePromise<unknown>) {
    if (!this.progressWaiters.has(restatePromise)) {
      return;
    }
    this.stopTrackingPromise(restatePromise);
  }

  private ensureProgressLoop(): void {
    if (this.progressLoop !== undefined) {
      return;
    }

    this.progressLoop = this.progressLoopInner().finally(() => {
      this.progressLoop = undefined;
      if (this.trackedPromises.size > 0) {
        this.ensureProgressLoop();
      }
    });
  }

  private async progressLoopInner(): Promise<void> {
    while (this.trackedPromises.size > 0) {
      if (!(await this.tryCompleteTrackedPromises())) {
        return;
      }
      if (this.trackedPromises.size === 0) {
        return;
      }

      // Yield before do_progress so Promise callbacks can settle and update combinator states.
      await setImmediate();

      const handles = this.collectUncompletedHandles();
      if (handles.length === 0) {
        this.completeResolvedTrackedPromises();
        continue;
      }

      let doProgressResult: vm.WasmDoProgressResult;
      try {
        doProgressResult = this.coreVm.do_progress(new Uint32Array(handles));
      } catch (e) {
        const trackedPromises = Array.from(this.trackedPromises);
        if (isReplayAwaitMismatchError(e)) {
          const replayAwaitingHandles = replayAwaitingHandlesFromError(e);
          if (replayAwaitingHandles !== undefined) {
            const affectedPromises = this.trackedPromisesIntersectingHandles(
              trackedPromises,
              replayAwaitingHandles
            );
            if (affectedPromises.length > 0) {
              this.failTrackedPromises(affectedPromises, e);
              continue;
            }
          }
          this.failTrackedPromises(trackedPromises, e);
          continue;
        }
        if (isClosedAwaitError(e)) {
          this.failTrackedPromises(trackedPromises, e);
          return;
        }

        this.errorCallback(e);
        this.failTrackedPromises(trackedPromises, e);
        return;
      }

      if (doProgressResult === "AnyCompleted") {
        continue;
      } else if (doProgressResult === "ReadFromInput") {
        await this.inputPump.awaitNextProgress();
      } else if (doProgressResult === "WaitingPendingRun") {
        await this.runClosuresTracker.awaitNextCompletedRun();
      } else if (doProgressResult === "CancelSignalReceived") {
        this.cancelTrackedPromises(Array.from(this.trackedPromises));
        return;
      } else {
        this.runClosuresTracker.executeRun(doProgressResult.ExecuteRun);
        await setImmediate();
      }
    }
  }

  private async tryCompleteTrackedPromises(): Promise<boolean> {
    const trackedPromises = Array.from(this.trackedPromises);
    for (const restatePromise of trackedPromises) {
      if (!this.trackedPromises.has(restatePromise)) {
        continue;
      }
      try {
        await restatePromise.tryComplete();
      } catch (e) {
        if (isClosedAwaitError(e)) {
          this.failTrackedPromises(Array.from(this.trackedPromises), e);
          return false;
        }
        this.errorCallback(e);
        this.failTrackedPromises(Array.from(this.trackedPromises), e);
        return false;
      }
    }
    this.completeResolvedTrackedPromises();
    return true;
  }

  private completeResolvedTrackedPromises(): void {
    for (const restatePromise of Array.from(this.trackedPromises)) {
      if (restatePromise.uncompletedLeaves().length === 0) {
        this.stopTrackingPromise(restatePromise);
      }
    }
  }

  private collectUncompletedHandles(): number[] {
    const handles: number[] = [];
    const seen = new Set<number>();

    for (const restatePromise of this.trackedPromises) {
      for (const handle of restatePromise.uncompletedLeaves()) {
        if (!seen.has(handle)) {
          seen.add(handle);
          handles.push(handle);
        }
      }
    }

    return handles;
  }

  private trackedPromisesIntersectingHandles(
    trackedPromises: Array<InternalRestatePromise<unknown>>,
    handles: ReadonlySet<number>
  ): Array<InternalRestatePromise<unknown>> {
    return trackedPromises.filter((restatePromise) =>
      restatePromise.uncompletedLeaves().some((handle) => handles.has(handle))
    );
  }

  private failTrackedPromises(
    trackedPromises: Array<InternalRestatePromise<unknown>>,
    reason: unknown
  ): void {
    for (const restatePromise of trackedPromises) {
      if (!this.trackedPromises.has(restatePromise)) {
        continue;
      }
      restatePromise.tryFail(reason);
      this.stopTrackingPromise(restatePromise);
    }
  }

  private cancelTrackedPromises(
    trackedPromises: Array<InternalRestatePromise<unknown>>
  ): void {
    for (const restatePromise of trackedPromises) {
      if (!this.trackedPromises.has(restatePromise)) {
        continue;
      }
      restatePromise.tryCancel();
      this.stopTrackingPromise(restatePromise);
    }
  }

  private stopTrackingPromise(
    restatePromise: InternalRestatePromise<unknown>
  ): void {
    this.trackedPromises.delete(restatePromise);
    const waiter = this.progressWaiters.get(restatePromise);
    this.progressWaiters.delete(restatePromise);
    waiter?.resolve();
  }
}

const REPLAY_AWAIT_MISMATCH_ERROR_KIND =
  "uncompleted_do_progress_during_replay";
const ERROR_KIND_METADATA_KEY = "restate.error.kind";
const REPLAY_AWAITING_HANDLES_METADATA_KEY = "restate.error.awaiting_handles";
const CLOSED_AWAIT_ERROR_CODE = 598;
const CLOSED_AWAIT_ERROR_REGEX = /state machine was closed/i;

function isReplayAwaitMismatchError(error: unknown): boolean {
  if (!isWasmFailure(error)) {
    return false;
  }

  if (
    error.metadata.some(
      (entry) =>
        entry.key === ERROR_KIND_METADATA_KEY &&
        entry.value === REPLAY_AWAIT_MISMATCH_ERROR_KIND
    )
  ) {
    return true;
  }

  return error.message.includes("'do_progress' could not be replayed");
}

function isClosedAwaitError(error: unknown): boolean {
  const visited = new Set<unknown>();
  let current: unknown = error;

  while (
    current !== null &&
    typeof current === "object" &&
    !visited.has(current)
  ) {
    if (isClosedAwaitErrorShallow(current)) {
      return true;
    }
    visited.add(current);

    const cause = (current as { cause?: unknown }).cause;
    if (cause === undefined) {
      return false;
    }
    current = cause;
  }

  return isClosedAwaitErrorShallow(current);
}

function isClosedAwaitErrorShallow(error: unknown): boolean {
  if (isWasmFailure(error)) {
    return (
      error.code === CLOSED_AWAIT_ERROR_CODE &&
      CLOSED_AWAIT_ERROR_REGEX.test(error.message)
    );
  }
  if (typeof error !== "object" || error === null) {
    return false;
  }

  const candidate = error as Partial<{ code: unknown; message: unknown }>;
  const candidateCode =
    typeof candidate.code === "number"
      ? candidate.code
      : typeof candidate.code === "string"
        ? Number(candidate.code)
        : typeof candidate.code === "bigint"
          ? Number(candidate.code)
          : Number.NaN;

  return (
    Number.isInteger(candidateCode) &&
    candidateCode === CLOSED_AWAIT_ERROR_CODE &&
    typeof candidate.message === "string" &&
    CLOSED_AWAIT_ERROR_REGEX.test(candidate.message)
  );
}
function replayAwaitingHandlesFromError(
  error: unknown
): Set<number> | undefined {
  if (!isWasmFailure(error)) {
    return undefined;
  }

  const rawHandles = error.metadata.find(
    (entry) => entry.key === REPLAY_AWAITING_HANDLES_METADATA_KEY
  )?.value;
  if (rawHandles === undefined || rawHandles.length === 0) {
    return undefined;
  }

  const handles = new Set<number>();
  for (const token of rawHandles.split(",")) {
    const trimmed = token.trim();
    if (trimmed.length === 0) {
      return undefined;
    }

    const handle = Number(trimmed);
    if (!Number.isInteger(handle) || handle < 0 || handle > 0xffff_ffff) {
      return undefined;
    }
    handles.add(handle);
  }

  return handles.size > 0 ? handles : undefined;
}

function isWasmFailure(error: unknown): error is vm.WasmFailure {
  if (typeof error !== "object" || error === null) {
    return false;
  }

  const candidate = error as Partial<vm.WasmFailure>;
  return (
    typeof candidate.code === "number" &&
    typeof candidate.message === "string" &&
    Array.isArray(candidate.metadata)
  );
}
