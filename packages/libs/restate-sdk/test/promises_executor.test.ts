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

import { describe, expect, it, vi } from "vitest";
import type * as vm from "../src/endpoint/handlers/vm/sdk_shared_core_wasm_bindings.js";
import type { InternalRestatePromise } from "../src/promises.js";
import { PromisesExecutor, RESTATE_CTX_SYMBOL } from "../src/promises.js";
import { CompletablePromise } from "../src/utils/completable_promise.js";
import type { InputPump, OutputPump } from "../src/io.js";
import type { RunClosuresTracker } from "../src/context_impl.js";
import type { Duration, RestatePromise } from "@restatedev/restate-sdk-core";

class ManualPromise implements InternalRestatePromise<string> {
  [RESTATE_CTX_SYMBOL] = {} as any;

  private readonly resultPromise = new CompletablePromise<string>();
  private completed = false;

  constructor(
    private readonly handle: number,
    private readonly value: string,
    private readonly completedHandles: Set<number>
  ) {}

  then<TResult1 = string, TResult2 = never>(
    onfulfilled?:
      | ((value: string) => TResult1 | PromiseLike<TResult1>)
      | undefined
      | null,
    onrejected?:
      | ((reason: any) => TResult2 | PromiseLike<TResult2>)
      | undefined
      | null
  ): Promise<TResult1 | TResult2> {
    return this.publicPromise().then(onfulfilled, onrejected);
  }

  catch<TResult = never>(
    onrejected?:
      | ((reason: any) => TResult | PromiseLike<TResult>)
      | undefined
      | null
  ): Promise<string | TResult> {
    return this.publicPromise().catch(onrejected);
  }

  finally(onfinally?: (() => void) | undefined | null): Promise<string> {
    return this.publicPromise().finally(onfinally);
  }

  orTimeout(_duration: number | Duration): RestatePromise<string> {
    return this;
  }

  map<U>(_mapper: (value?: string, failure?: any) => U): RestatePromise<U> {
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
    if (this.completedHandles.has(this.handle)) {
      this.completed = true;
      this.resultPromise.resolve(this.value);
    }
  }

  uncompletedLeaves(): number[] {
    return this.completed ? [] : [this.handle];
  }

  publicPromise(): Promise<string> {
    return this.resultPromise.promise;
  }

  readonly [Symbol.toStringTag] = "ManualPromise";
}

describe("PromisesExecutor", () => {
  it("fails only the detached loser when replay await mismatch happens after race winner", async () => {
    const completedHandles = new Set<number>();
    const replayMismatch: vm.WasmFailure = {
      code: 570,
      message:
        "Found a mismatch: 'do_progress' could not be replayed for this await point",
      metadata: [
        {
          key: "restate.error.kind",
          value: "uncompleted_do_progress_during_replay",
        },
      ],
    };

    const doProgressCalls: number[][] = [];
    const coreVm = {
      do_progress(handles: Uint32Array): vm.WasmDoProgressResult {
        const asArray = Array.from(handles);
        doProgressCalls.push(asArray);
        if (asArray.length === 2) {
          completedHandles.add(2);
          return "AnyCompleted";
        }
        throw replayMismatch;
      },
    } as unknown as vm.WasmVM;

    const executor = new PromisesExecutor(
      coreVm,
      {
        awaitNextProgress: async () => {},
      } as unknown as InputPump,
      {
        awaitNextProgress: async () => {},
      } as unknown as OutputPump,
      {
        awaitNextCompletedRun: async () => {},
        executeRun: () => {
          throw new Error("unexpected executeRun");
        },
      } as unknown as RunClosuresTracker,
      vi.fn()
    );

    const a = new ManualPromise(1, "A", completedHandles);
    const b = new ManualPromise(2, "B", completedHandles);

    const progressA = executor.doProgress(a);
    const progressB = executor.doProgress(b);

    const winner = await Promise.race([a.publicPromise(), b.publicPromise()]);
    expect(winner).toBe("B");

    await Promise.all([progressA, progressB]);
    await expect(a.publicPromise()).rejects.toEqual(replayMismatch);
    expect(doProgressCalls).toStrictEqual([[1, 2], [1]]);
  });
});
