import type { Credentials } from "@distilled.cloud/honeybadger/Credentials";
import * as Ingest from "@distilled.cloud/honeybadger/Ingest";
import { isResolved } from "alchemy/Diff";
import type { Diff } from "alchemy/Diff";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import { sha256 } from "alchemy/Util/sha256";
import * as Effect from "effect/Effect";
import type * as HttpClient from "effect/http/HttpClient";
import * as Redacted from "effect/Redacted";
import { withIngestRetry } from "./internal.ts";
import type { Providers } from "./Providers.ts";

export interface DeployProps {
  /** The project's API key, which Honeybadger takes deploys with. */
  apiKey: Redacted.Redacted;
  /**
   * The environment deployed to, for example `production`. A new
   * environment records a new deploy.
   */
  environment: string;
  /** The revision deployed. A new revision records a new deploy. */
  revision: string;
  /**
   * The repository the revision is in, linked from the deploy. Changing it
   * alone records nothing.
   */
  repository?: string;
  /** Who deployed. Changing it alone records nothing. */
  localUsername?: string;
  /**
   * Any output of the resources that must be deployed first, such as the
   * Worker's URL. It only orders the deploy record after them.
   */
  after?: unknown;
}

export interface DeployAttributes {
  /** The revision last recorded. */
  revision: string;
  /** The environment last recorded. */
  environment: string;
  /** SHA-256 of the API key last recorded with, so a key change records again. */
  apiKeyDigest: string;
}

export type Deploy = Resource<
  "Honeybadger.Deploy",
  DeployProps,
  DeployAttributes,
  never,
  Providers
>;

/**
 * Records a deploy in Honeybadger, once per revision, environment and
 * project. Pass the deployed Worker's outputs as `after` so the record is
 * made once it is live. It goes to the ingest host for the account's region
 * (`https://eu-api.honeybadger.io` for an EU account). Each attempt gives up
 * after 5 seconds, and throttling or server errors are retried.
 *
 * Honeybadger keeps its deploy history, so destroying the resource deletes
 * nothing. It doesn't de-duplicate deploys either: if a deploy is
 * interrupted after the record is made but before Alchemy saves its state,
 * the next deploy records the same revision again.
 *
 * ### Recording a deploy
 * **Example:** Record a deploy once the Worker is live
 * ```typescript
 * const project = yield* Honeybadger.Project("Errors", { name: "momo" });
 * const key = yield* Honeybadger.IngestionKey("Worker", {
 *   projectId: project.projectId,
 * });
 * const worker = yield* Cloudflare.Worker("Api", { main: "./src/worker.ts" });
 * const revision = yield* Config.String("GITHUB_SHA").pipe(Config.withDefault("dev"));
 * yield* Honeybadger.Deploy("Production", {
 *   apiKey: key.key,
 *   environment: "production",
 *   revision,
 *   after: worker.url,
 * });
 * ```
 *
 * @resource
 * @product Deployments
 */
export const Deploy = Resource<Deploy>("Honeybadger.Deploy");

export const DeployProvider = () =>
  Provider.effect(
    Deploy,
    Effect.gen(function* makeDeployProvider() {
      const services = yield* Effect.context<HttpClient.HttpClient | Credentials>();

      const recorded = Effect.fn(function* recorded(
        news: DeployProps,
        output: DeployAttributes | undefined,
      ) {
        const apiKeyDigest = yield* news.apiKey.pipe(Redacted.value, sha256);
        const same =
          output !== undefined &&
          output.revision === news.revision &&
          output.environment === news.environment &&
          output.apiKeyDigest === apiKeyDigest;
        return { apiKeyDigest, same };
      });

      return {
        delete: () => Effect.void,
        diff: ({ news, output }) =>
          isResolved(news)
            ? Effect.map(recorded(news, output), ({ same }): Diff => ({
                action: same ? "noop" : "update",
              }))
            : Effect.undefined,
        list: () => Effect.succeed([]),
        reconcile: Effect.fn(function* reconcile({ news, output }) {
          const { apiKeyDigest, same } = yield* recorded(news, output);
          if (same && output !== undefined) {
            return output;
          }
          yield* Ingest.createDeploy({
            api_key: news.apiKey,
            environment: news.environment,
            revision: news.revision,
            ...(news.repository === undefined ? {} : { repository: news.repository }),
            ...(news.localUsername === undefined ? {} : { local_username: news.localUsername }),
          }).pipe(Effect.timeout("5 seconds"), withIngestRetry, Effect.provideContext(services));
          return { apiKeyDigest, environment: news.environment, revision: news.revision };
        }),
      };
    }),
  );
