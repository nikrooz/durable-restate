/*
 * Copyright (c) 2023-2026 - Restate Software, Inc., Restate GmbH
 *
 * This file is part of the Restate SDK for Node.js/TypeScript,
 * which is released under the MIT license.
 *
 * You can find a copy of the license in file LICENSE in the root
 * directory of this repository or package, or at
 * https://github.com/restatedev/sdk-typescript/blob/main/LICENSE
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

import { setTimeout as delay } from "node:timers/promises";
import { describe, expect, it, vi } from "vitest";
import type { Duration, RestatePromise } from "@restatedev/restate-sdk-core";
import type { InputPump, OutputPump } from "../src/io.js";
import type { RunClosuresTracker } from "../src/context_impl.js";
import type * as vm from "../src/endpoint/handlers/vm/sdk_shared_core_wasm_bindings.js";
import type { InternalRestatePromise } from "../src/promises.js";
import { PromisesExecutor, RESTATE_CTX_SYMBOL } from "../src/promises.js";
import { RetryableError, TerminalError } from "../src/types/errors.js";
import { CompletablePromise } from "../src/utils/completable_promise.js";

type Resolution =
  | { type: "resolve"; value: unknown }
  | { type: "reject"; reason: unknown };

type ProgressStep = {
  call: number;
  handles: number[];
  resolve: (handle: number, value: unknown) => void;
  reject: (handle: number, reason: unknown) => void;
};

class ScriptedCoreVm {
  private call = 0;
  private readonly pendingResolutions = new Map<number, Resolution>();
  readonly calls: number[][] = [];

  constructor(
    private readonly step: (progress: ProgressStep) => vm.WasmDoProgressResult
  ) {}

  do_progress(handles: Uint32Array): vm.WasmDoProgressResult {
    const asArray = Array.from(handles);
    this.call += 1;
    this.calls.push(asArray);
    return this.step({
      call: this.call,
      handles: asArray,
      resolve: (handle, value) => {
        this.pendingResolutions.set(handle, { type: "resolve", value });
      },
      reject: (handle, reason) => {
        this.pendingResolutions.set(handle, { type: "reject", reason });
      },
    });
  }

  takeResolution(handle: number): Resolution | undefined {
    const resolution = this.pendingResolutions.get(handle);
    if (resolution !== undefined) {
      this.pendingResolutions.delete(handle);
    }
    return resolution;
  }
}

class ManualPromise<T> implements InternalRestatePromise<T> {
  [RESTATE_CTX_SYMBOL] = {} as any;

  private readonly resultPromise = new CompletablePromise<T>();
  private pollingPromise?: Promise<void>;
  private completed = false;

  constructor(
    private readonly executor: PromisesExecutor,
    private readonly coreVm: ScriptedCoreVm,
    readonly handle: number
  ) {}

  then<TResult1 = T, TResult2 = never>(
    onfulfilled?:
      | ((value: T) => TResult1 | PromiseLike<TResult1>)
      | undefined
      | null,
    onrejected?:
      | ((reason: any) => TResult2 | PromiseLike<TResult2>)
      | undefined
      | null
  ): Promise<TResult1 | TResult2> {
    this.ensurePolling();
    return this.publicPromise().then(onfulfilled, onrejected);
  }

  catch<TResult = never>(
    onrejected?:
      | ((reason: any) => TResult | PromiseLike<TResult>)
      | undefined
      | null
  ): Promise<T | TResult> {
    this.ensurePolling();
    return this.publicPromise().catch(onrejected);
  }

  finally(onfinally?: (() => void) | undefined | null): Promise<T> {
    this.ensurePolling();
    return this.publicPromise().finally(onfinally);
  }

  orTimeout(_duration: number | Duration): RestatePromise<T> {
    return this;
  }

  map<U>(_mapper: (value?: T, failure?: any) => U): RestatePromise<U> {
    return this as unknown as RestatePromise<U>;
  }

  tryCancel(): void {
    if (!this.completed) {
      this.completed = true;
      this.resultPromise.reject(new Error("cancelled"));
    }
  }

  tryFail(error: unknown): void {
    if (!this.completed) {
      this.completed = true;
      this.resultPromise.reject(error);
    }
  }

  async tryComplete(): Promise<void> {
    if (this.completed) {
      return;
    }
    const resolution = this.coreVm.takeResolution(this.handle);
    if (resolution === undefined) {
      return;
    }

    this.completed = true;
    if (resolution.type === "resolve") {
      this.resultPromise.resolve(resolution.value as T);
    } else {
      this.resultPromise.reject(resolution.reason);
    }
  }

  uncompletedLeaves(): number[] {
    return this.completed ? [] : [this.handle];
  }

  publicPromise(): Promise<T> {
    return this.resultPromise.promise;
  }

  private ensurePolling(): void {
    if (this.pollingPromise !== undefined) {
      return;
    }
    const progressPromise = this.executor.doProgress(this).catch(() => {});
    this.pollingPromise = progressPromise.finally(() => {
      this.pollingPromise = undefined;
    });
  }

  readonly [Symbol.toStringTag] = "ManualPromise";
}

type Fixture = ReturnType<typeof createFixture>;

function createFixture(
  step: (progress: ProgressStep) => vm.WasmDoProgressResult
) {
  const coreVm = new ScriptedCoreVm(step);
  const inputPump = {
    awaitNextProgress: vi.fn(async () => {}),
  };
  const outputPump = {
    awaitNextProgress: vi.fn(async () => {}),
  };
  const runClosuresTracker = {
    awaitNextCompletedRun: vi.fn(async () => {}),
    executeRun: vi.fn(() => {}),
  };
  const errorCallback = vi.fn();

  const executor = new PromisesExecutor(
    coreVm as unknown as vm.WasmVM,
    inputPump as unknown as InputPump,
    outputPump as unknown as OutputPump,
    runClosuresTracker as unknown as RunClosuresTracker,
    errorCallback
  );

  return {
    coreVm,
    inputPump,
    outputPump,
    runClosuresTracker,
    errorCallback,
    promise<T>(handle: number) {
      return new ManualPromise<T>(executor, coreVm, handle);
    },
  };
}

function replayMismatch(
  label: string,
  awaitingHandles?: number[]
): vm.WasmFailure {
  const metadata: vm.WasmFailureMetadata[] = [
    {
      key: "restate.error.kind",
      value: "uncompleted_do_progress_during_replay",
    },
  ];
  if (awaitingHandles !== undefined) {
    metadata.push({
      key: "restate.error.awaiting_handles",
      value: awaitingHandles.join(","),
    });
  }

  return {
    code: 570,
    message: `${label}: 'do_progress' could not be replayed`,
    metadata,
  };
}

function expectHandles(actual: number[], expected: number[]) {
  const as = [...actual].sort((a, b) => a - b);
  const es = [...expected].sort((a, b) => a - b);
  expect(as).toStrictEqual(es);
}

async function expectRejectWithin(
  promise: Promise<unknown>,
  expectedReason: unknown
) {
  const outcome = await Promise.race([
    promise.then(
      (value) => ({ type: "resolved" as const, value }),
      (reason) => ({ type: "rejected" as const, reason })
    ),
    delay(200).then(() => ({ type: "timeout" as const })),
  ]);

  if (outcome.type === "timeout") {
    throw new Error("Expected promise rejection, but it stayed pending");
  }
  if (outcome.type === "resolved") {
    throw new Error(
      `Expected rejection, but resolved with ${String(outcome.value)}`
    );
  }

  expect(outcome.reason).toEqual(expectedReason);
}

async function toPromise<T>(promise: PromiseLike<T>): Promise<T> {
  return await promise;
}

describe("PromisesExecutor native combinator replay behavior", () => {
  it("fails only promises whose leaves intersect mismatch handles", async () => {
    const mismatch = replayMismatch("scoped mismatch", [1]);
    const fixture = createFixture((progress) => {
      switch (progress.call) {
        case 1:
          expectHandles(progress.handles, [1, 2, 3]);
          progress.resolve(2, "B");
          return "AnyCompleted";
        case 2:
          expectHandles(progress.handles, [1, 3]);
          throw mismatch;
        case 3:
          expectHandles(progress.handles, [3]);
          progress.resolve(3, "C");
          return "AnyCompleted";
        default:
          throw new Error(`unexpected call ${progress.call}`);
      }
    });

    const a = fixture.promise<string>(1);
    const b = fixture.promise<string>(2);
    const c = fixture.promise<string>(3);

    const cBranch = (async () => toPromise(c))();
    const winner = await Promise.race([toPromise(a), toPromise(b)]);
    expect(winner).toBe("B");

    await expectRejectWithin(a.publicPromise(), mismatch);
    await expect(cBranch).resolves.toBe("C");
    expect(fixture.errorCallback).not.toHaveBeenCalled();
    expect(fixture.coreVm.calls).toHaveLength(3);
    expectHandles(fixture.coreVm.calls[0]!, [1, 2, 3]);
    expectHandles(fixture.coreVm.calls[1]!, [1, 3]);
    expectHandles(fixture.coreVm.calls[2]!, [3]);
  });

  it("isolates detached loser mismatch for native Promise.race(toPromise)", async () => {
    const mismatch = replayMismatch("detached race loser");
    const fixture = createFixture((progress) => {
      switch (progress.call) {
        case 1:
          expectHandles(progress.handles, [1, 2]);
          progress.resolve(2, "B");
          return "AnyCompleted";
        case 2:
          expectHandles(progress.handles, [1]);
          throw mismatch;
        default:
          throw new Error(`unexpected call ${progress.call}`);
      }
    });

    const a = fixture.promise<string>(1);
    const b = fixture.promise<string>(2);

    const winner = await Promise.race([toPromise(a), toPromise(b)]);
    expect(winner).toBe("B");

    await expectRejectWithin(a.publicPromise(), mismatch);
    expect(fixture.errorCallback).not.toHaveBeenCalled();
    expect(fixture.coreVm.calls).toStrictEqual([[1, 2], [1]]);
  });

  it("isolates detached losers for native Promise.any(toPromise)", async () => {
    const mismatch = replayMismatch("detached any losers");
    const fixture = createFixture((progress) => {
      switch (progress.call) {
        case 1:
          expectHandles(progress.handles, [1, 2, 3]);
          progress.resolve(3, "C");
          return "AnyCompleted";
        case 2:
          expectHandles(progress.handles, [1, 2]);
          throw mismatch;
        default:
          throw new Error(`unexpected call ${progress.call}`);
      }
    });

    const a = fixture.promise<string>(1);
    const b = fixture.promise<string>(2);
    const c = fixture.promise<string>(3);

    const winner = await Promise.any([
      toPromise(a),
      toPromise(b),
      toPromise(c),
    ]);
    expect(winner).toBe("C");

    await expectRejectWithin(a.publicPromise(), mismatch);
    await expectRejectWithin(b.publicPromise(), mismatch);
    expect(fixture.errorCallback).not.toHaveBeenCalled();
    expect(fixture.coreVm.calls).toStrictEqual([
      [1, 2, 3],
      [1, 2],
    ]);
  });

  it("keeps Promise.all fast-fail error and isolates detached siblings", async () => {
    const terminal = new TerminalError("terminal failure");
    const mismatch = replayMismatch("detached all siblings");
    const fixture = createFixture((progress) => {
      switch (progress.call) {
        case 1:
          expectHandles(progress.handles, [1, 2, 3]);
          progress.reject(2, terminal);
          return "AnyCompleted";
        case 2:
          expectHandles(progress.handles, [1, 3]);
          throw mismatch;
        default:
          throw new Error(`unexpected call ${progress.call}`);
      }
    });

    const a = fixture.promise<string>(1);
    const b = fixture.promise<string>(2);
    const c = fixture.promise<string>(3);

    await expectRejectWithin(
      Promise.all([toPromise(a), toPromise(b), toPromise(c)]),
      terminal
    );

    await expectRejectWithin(a.publicPromise(), mismatch);
    await expectRejectWithin(c.publicPromise(), mismatch);
    expect(fixture.errorCallback).not.toHaveBeenCalled();
    expect(fixture.coreVm.calls).toStrictEqual([
      [1, 2, 3],
      [1, 3],
    ]);
  });

  it("handles nested Promise.allSettled inside native race", async () => {
    const mismatch = replayMismatch("detached nested allSettled");
    const fixture = createFixture((progress) => {
      switch (progress.call) {
        case 1:
          expectHandles(progress.handles, [10, 11, 12]);
          progress.resolve(10, "fast");
          return "AnyCompleted";
        case 2:
          expectHandles(progress.handles, [11, 12]);
          throw mismatch;
        default:
          throw new Error(`unexpected call ${progress.call}`);
      }
    });

    const fast = fixture.promise<string>(10);
    const a = fixture.promise<string>(11);
    const b = fixture.promise<string>(12);

    const inner = Promise.allSettled([toPromise(a), toPromise(b)]);
    const winner = await Promise.race([inner, toPromise(fast)]);
    expect(winner).toBe("fast");

    expect(await inner).toStrictEqual([
      { status: "rejected", reason: mismatch },
      { status: "rejected", reason: mismatch },
    ]);
    expect(fixture.errorCallback).not.toHaveBeenCalled();
    expect(fixture.coreVm.calls).toStrictEqual([
      [11, 12, 10],
      [11, 12],
    ]);
  });

  it("supports nested async branches with interleaved awaits and detached losers", async () => {
    const mismatch = replayMismatch("detached deep async branches");
    const fixture = createFixture((progress) => {
      switch (progress.call) {
        case 1:
          expectHandles(progress.handles, [101, 201, 301]);
          progress.resolve(301, "c1");
          return "AnyCompleted";
        case 2:
          expectHandles(progress.handles, [101, 201, 302]);
          progress.resolve(302, "c2");
          return "AnyCompleted";
        case 3:
          expectHandles(progress.handles, [101, 201]);
          throw mismatch;
        default:
          throw new Error(`unexpected call ${progress.call}`);
      }
    });

    const f1a = fixture.promise<string>(101);
    const f1b = fixture.promise<string>(102);
    const f2a = fixture.promise<string>(201);
    const f2b = fixture.promise<string>(202);
    const f3a = fixture.promise<string>(301);
    const f3b = fixture.promise<string>(302);

    const branch1 = (async () => {
      const first = await toPromise(f1a);
      const second = await toPromise(f1b);
      return `f1:${first}:${second}`;
    })();
    const branch2 = (async () => {
      const first = await toPromise(f2a);
      const second = await toPromise(f2b);
      return `f2:${first}:${second}`;
    })();
    const branch3 = (async () => {
      const first = await toPromise(f3a);
      const second = await toPromise(f3b);
      return `f3:${first}:${second}`;
    })();

    const winner = await Promise.any([branch1, branch2, branch3]);
    expect(winner).toBe("f3:c1:c2");

    await expectRejectWithin(branch1, mismatch);
    await expectRejectWithin(branch2, mismatch);
    expect(fixture.errorCallback).not.toHaveBeenCalled();
    expect(fixture.coreVm.calls).toStrictEqual([
      [101, 201, 301],
      [101, 201, 302],
      [101, 201],
    ]);
  });

  it("preserves retryable rejection semantics while isolating detached losers", async () => {
    const retryable = new RetryableError("retry later");
    const mismatch = replayMismatch("detached loser after retryable rejection");
    const fixture = createFixture((progress) => {
      switch (progress.call) {
        case 1:
          expectHandles(progress.handles, [1, 2]);
          progress.reject(1, retryable);
          return "AnyCompleted";
        case 2:
          expectHandles(progress.handles, [2]);
          throw mismatch;
        default:
          throw new Error(`unexpected call ${progress.call}`);
      }
    });

    const a = fixture.promise<string>(1);
    const b = fixture.promise<string>(2);

    await expectRejectWithin(
      Promise.race([toPromise(a), toPromise(b)]),
      retryable
    );
    await expectRejectWithin(b.publicPromise(), mismatch);
    expect(fixture.errorCallback).not.toHaveBeenCalled();
    expect(fixture.coreVm.calls).toStrictEqual([[1, 2], [2]]);
  });

  it("handles ReadFromInput progression and still isolates detached losers", async () => {
    const mismatch = replayMismatch("detached loser after input wait");
    const fixture = createFixture((progress) => {
      switch (progress.call) {
        case 1:
          expectHandles(progress.handles, [1, 2]);
          return "ReadFromInput";
        case 2:
          expectHandles(progress.handles, [1, 2]);
          progress.resolve(2, "B");
          return "AnyCompleted";
        case 3:
          expectHandles(progress.handles, [1]);
          throw mismatch;
        default:
          throw new Error(`unexpected call ${progress.call}`);
      }
    });

    const a = fixture.promise<string>(1);
    const b = fixture.promise<string>(2);

    const winner = await Promise.race([toPromise(a), toPromise(b)]);
    expect(winner).toBe("B");

    await expectRejectWithin(a.publicPromise(), mismatch);
    expect(fixture.inputPump.awaitNextProgress).toHaveBeenCalledTimes(1);
    expect(fixture.errorCallback).not.toHaveBeenCalled();
    expect(fixture.coreVm.calls).toStrictEqual([[1, 2], [1, 2], [1]]);
  });

  it("handles ExecuteRun/WaitingPendingRun and still isolates detached losers", async () => {
    const mismatch = replayMismatch("detached loser after run waits");
    const fixture = createFixture((progress) => {
      switch (progress.call) {
        case 1:
          expectHandles(progress.handles, [1, 2]);
          return { ExecuteRun: 99 };
        case 2:
          expectHandles(progress.handles, [1, 2]);
          return "WaitingPendingRun";
        case 3:
          expectHandles(progress.handles, [1, 2]);
          progress.resolve(2, "B");
          return "AnyCompleted";
        case 4:
          expectHandles(progress.handles, [1]);
          throw mismatch;
        default:
          throw new Error(`unexpected call ${progress.call}`);
      }
    });

    const a = fixture.promise<string>(1);
    const b = fixture.promise<string>(2);

    const winner = await Promise.race([toPromise(a), toPromise(b)]);
    expect(winner).toBe("B");

    await expectRejectWithin(a.publicPromise(), mismatch);
    expect(fixture.runClosuresTracker.executeRun).toHaveBeenCalledTimes(1);
    expect(fixture.runClosuresTracker.executeRun).toHaveBeenCalledWith(99);
    expect(
      fixture.runClosuresTracker.awaitNextCompletedRun
    ).toHaveBeenCalledTimes(1);
    expect(fixture.errorCallback).not.toHaveBeenCalled();
    expect(fixture.coreVm.calls).toStrictEqual([[1, 2], [1, 2], [1, 2], [1]]);
  });
});
