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

// Internal bridge exports for downstream integrations that intentionally
// build wrappers outside this package.

export type { ContextImpl } from "./context_impl.js";
export {
  extractContext,
  RestateCombinatorPromise,
  RestatePendingPromise,
} from "./promises.js";
