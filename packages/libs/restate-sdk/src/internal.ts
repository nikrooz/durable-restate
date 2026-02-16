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

import type { Context, RestatePromise } from "./context.js";
import type { ContextImpl } from "./context_impl.js";
import { createTaskBridgePromise } from "./context_impl.js";

/**
 * Internal action type used by task bridge helpers.
 */
export type TaskBridgeAction<T> = () => T;

/**
 * Internal bridge that wraps task-like actions into a combinable RestatePromise.
 *
 * This API is intentionally exposed from the `internal` entrypoint so external
 * wrappers can stay out of the core SDK public API surface.
 */
export function taskBridge<T>(
  context: Context,
  action: TaskBridgeAction<T>
): RestatePromise<Awaited<T>> {
  if (!isTaskBridgeContext(context)) {
    throw new TypeError(
      "Expected a Restate handler Context created by this SDK package."
    );
  }
  return createTaskBridgePromise(context, action);
}

function isTaskBridgeContext(value: unknown): value is ContextImpl {
  return (
    typeof value === "object" &&
    value !== null &&
    "promisesExecutor" in value &&
    "handleInvocationEndError" in value
  );
}
