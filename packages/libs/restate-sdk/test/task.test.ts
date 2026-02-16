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

import { describe, expect, it, vi } from "vitest";
import type { ContextImpl } from "../src/context_impl.js";
import {
  extractContext,
  RestateTaskPromise,
  RestatePendingPromise,
} from "../src/promises.js";
import {
  RestatePromise as RestatePromiseCombinators,
  type Context,
  type RestatePromise,
} from "../src/context.js";
import { taskBridge } from "../src/internal.js";

function fakeContext(): ContextImpl {
  return {
    promisesExecutor: {
      doProgress: vi.fn(async () => {}),
    },
    handleInvocationEndError: vi.fn(),
  } as unknown as ContextImpl;
}

function task<T>(
  ctx: ContextImpl,
  action: () => PromiseLike<T> | T
): RestatePromise<Awaited<T>> {
  return taskBridge(ctx as unknown as Context, action) as RestatePromise<
    Awaited<T>
  >;
}

describe("taskBridge", () => {
  it("wraps native promises so they can be combined", async () => {
    const ctx = fakeContext();

    const p1 = task(ctx, async () => {
      await Promise.resolve();
      return "a";
    });
    const p2 = task(ctx, async () => {
      await Promise.resolve();
      return "b";
    });

    await expect(RestatePromiseCombinators.all([p1, p2])).resolves.toEqual([
      "a",
      "b",
    ]);
    expect(extractContext(p1)).toBe(ctx);
    expect(extractContext(p2)).toBe(ctx);
  });

  it("passes through a restate promise from the same context", () => {
    const ctx = fakeContext();
    const inner = new RestateTaskPromise(ctx, Promise.resolve("ok"));

    const wrapped = task(ctx, () => inner);

    expect(wrapped).toBe(inner);
  });

  it("supports nested taskBridge calls", async () => {
    const ctx = fakeContext();

    const outer = task(ctx, async () => {
      const inner = task(ctx, async () => {
        await Promise.resolve();
        return 123;
      });
      return await inner;
    });

    await expect(outer).resolves.toBe(123);
  });

  it("returns a rejected restate promise when action throws synchronously", async () => {
    const ctx = fakeContext();

    const p = task(ctx, () => {
      throw new Error("boom");
    });

    await expect(p).rejects.toThrow("boom");
  });

  it("fails fast when action returns a restate promise from a different context", () => {
    const sourceCtx = fakeContext();
    const targetCtx = fakeContext();
    const foreignPromise = new RestateTaskPromise(targetCtx, Promise.resolve(1));

    const result = task(sourceCtx, () => foreignPromise);

    expect(sourceCtx.handleInvocationEndError).toHaveBeenCalledTimes(1);
    expect(result).toBeInstanceOf(RestatePendingPromise);
  });
});
