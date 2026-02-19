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

import * as restate from "@restatedev/restate-sdk";
import * as clients from "@restatedev/restate-sdk-clients";
import { RestateTestEnvironment } from "@restatedev/restate-sdk-testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

type SlowInput = {
  value: string;
  ms: number;
};

const SLOW_LOSER_MS = 5_000;
const RETRY_MARKERS = new Set<string>();

const combinatorWorker = restate.service({
  name: "native-combinator-worker",
  handlers: {
    fast: async (_ctx: restate.Context, value: string) => value,
    slow: async (ctx: restate.Context, input: SlowInput) => {
      await ctx.sleep(input.ms);
      return input.value;
    },
    terminal: async (_ctx: restate.Context, message: string) => {
      throw new restate.TerminalError(message, { errorCode: 409 });
    },
    retryable: async (_ctx: restate.Context, message: string) => {
      throw new restate.RetryableError(message);
    },
  },
});

const nativeCombinatorRunner = restate.object({
  name: "native-combinator-runner",
  handlers: {
    race: async (ctx: restate.ObjectContext) => {
      const a = ctx
        .serviceClient(combinatorWorker)
        .slow({ value: "A", ms: SLOW_LOSER_MS });
      const b = ctx.serviceClient(combinatorWorker).fast("B");

      const winner = await Promise.race([toPromise(a), toPromise(b)]);
      await ctx.run(`race-branch-${winner}`, () => winner);
      failOnceOutsideJournal(`race:${ctx.key}`);

      return winner;
    },

    any: async (ctx: restate.ObjectContext) => {
      const a = ctx
        .serviceClient(combinatorWorker)
        .slow({ value: "A", ms: SLOW_LOSER_MS });
      const b = ctx
        .serviceClient(combinatorWorker)
        .slow({ value: "B", ms: SLOW_LOSER_MS });
      const c = ctx.serviceClient(combinatorWorker).fast("C");

      const winner = await Promise.any([toPromise(a), toPromise(b), toPromise(c)]);
      await ctx.run(`any-branch-${winner}`, () => winner);
      failOnceOutsideJournal(`any:${ctx.key}`);

      return winner;
    },

    allFastFail: async (ctx: restate.ObjectContext) => {
      const a = ctx
        .serviceClient(combinatorWorker)
        .slow({ value: "A", ms: SLOW_LOSER_MS });
      const b = ctx.serviceClient(combinatorWorker).terminal("boom");
      const c = ctx
        .serviceClient(combinatorWorker)
        .slow({ value: "C", ms: SLOW_LOSER_MS });

      let outcome = "unexpected";
      try {
        await Promise.all([toPromise(a), toPromise(b), toPromise(c)]);
      } catch (error) {
        if (!(error instanceof restate.TerminalError)) {
          throw error;
        }
        outcome = "terminal";
      }

      await ctx.run("all-fast-fail-outcome", () => outcome);
      failOnceOutsideJournal(`all-fast-fail:${ctx.key}`);

      return outcome;
    },

    nestedAllSettledRace: async (ctx: restate.ObjectContext) => {
      const fast = ctx.serviceClient(combinatorWorker).fast("fast");
      const a = ctx
        .serviceClient(combinatorWorker)
        .slow({ value: "A", ms: SLOW_LOSER_MS });
      const b = ctx
        .serviceClient(combinatorWorker)
        .slow({ value: "B", ms: SLOW_LOSER_MS });

      const inner = Promise.allSettled([toPromise(a), toPromise(b)]);
      const winner = await Promise.race([inner, toPromise(fast)]);

      if (winner !== "fast") {
        throw new Error("Expected fast winner");
      }

      await ctx.run("nested-all-settled-race", () => winner);
      failOnceOutsideJournal(`nested-all-settled-race:${ctx.key}`);

      return winner;
    },

    retryableRace: async (ctx: restate.ObjectContext) => {
      const a = ctx.serviceClient(combinatorWorker).retryable("retry-me");
      const b = ctx.serviceClient(combinatorWorker).slow({ value: "B", ms: 450 });

      const winner = await Promise.race([toPromise(a), toPromise(b)]);

      await ctx.run("retryable-race", () => winner);
      failOnceOutsideJournal(`retryable-race:${ctx.key}`);

      return winner;
    },

    stressManyNativeRaces: async (ctx: restate.ObjectContext) => {
      const races = Array.from({ length: 8 }, (_v, index) => {
        const slow = ctx
          .serviceClient(combinatorWorker)
          .slow({ value: `A${index}`, ms: SLOW_LOSER_MS });
        const fast = ctx.serviceClient(combinatorWorker).fast(`B${index}`);
        return Promise.race([toPromise(slow), toPromise(fast)]);
      });

      const winners = await Promise.all(races);
      const winnerCount = winners.length;

      await ctx.run("stress-many-races", () => winnerCount);
      failOnceOutsideJournal(`stress-many-races:${ctx.key}`);

      return winnerCount;
    },
  },
});

describe("Native Promise combinators E2E replay", () => {
  let restateTestEnvironment: RestateTestEnvironment;
  let ingress: clients.Ingress;

  beforeAll(async () => {
    restateTestEnvironment = await RestateTestEnvironment.start({
      services: [combinatorWorker, nativeCombinatorRunner],
    });
    ingress = clients.connect({ url: restateTestEnvironment.baseUrl() });
  }, 60_000);

  afterAll(async () => {
    if (restateTestEnvironment !== undefined) {
      await restateTestEnvironment.stop();
    }
  });

  it(
    "replays native Promise.race(toPromise) without journal mismatch",
    async () => {
      const runner = ingress.objectClient(nativeCombinatorRunner, "race");
      await expect(runner.race()).resolves.toBe("B");
    },
    60_000
  );

  it(
    "replays native Promise.any(toPromise) without journal mismatch",
    async () => {
      const runner = ingress.objectClient(nativeCombinatorRunner, "any");
      await expect(runner.any()).resolves.toBe("C");
    },
    60_000
  );

  it(
    "replays Promise.all fast-fail path without detached-loser mismatch",
    async () => {
      const runner = ingress.objectClient(nativeCombinatorRunner, "all");
      await expect(runner.allFastFail()).resolves.toBe("terminal");
    },
    60_000
  );

  it(
    "replays nested Promise.allSettled inside Promise.race",
    async () => {
      const runner = ingress.objectClient(nativeCombinatorRunner, "nested-all");
      await expect(runner.nestedAllSettledRace()).resolves.toBe("fast");
    },
    60_000
  );

  it(
    "replays race with a retrying loser invocation",
    async () => {
      const runner = ingress.objectClient(nativeCombinatorRunner, "retryable");
      await expect(runner.retryableRace()).resolves.toBe("B");
    },
    60_000
  );

  it(
    "replays many concurrent native races without mismatch",
    async () => {
      const runner = ingress.objectClient(nativeCombinatorRunner, "stress-races");
      await expect(runner.stressManyNativeRaces()).resolves.toBe(8);
    },
    60_000
  );
});

function failOnceOutsideJournal(marker: string): void {
  if (RETRY_MARKERS.has(marker)) {
    return;
  }
  RETRY_MARKERS.add(marker);
  throw new Error(`retry:${marker}`);
}

async function toPromise<T>(promise: PromiseLike<T>): Promise<T> {
  return await promise;
}
