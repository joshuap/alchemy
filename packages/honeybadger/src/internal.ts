import * as DeployRetry from "alchemy/DeployRetry";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";

/**
 * Retries an ingest call the way generated distilled operations retry: on
 * throttling, server and transport errors, honoring `Retry-After`, at most
 * `DeployRetry.MAX_RETRIES` times. The ingest module is hand-written, so the
 * `Retry` service `providers()` installs does not reach it.
 */
export const withIngestRetry = <A, E, R>(call: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.gen(function* () {
    const lastError = yield* Ref.make<unknown>(undefined);
    const policy = DeployRetry.policy(lastError);
    return yield* call.pipe(
      Effect.tapError((error) => Ref.set(lastError, error)),
      Effect.retry({
        while: policy.while ?? DeployRetry.isTransient,
        schedule: policy.schedule ?? Schedule.recurs(DeployRetry.MAX_RETRIES),
      }),
    );
  });
