import * as Keys from "@distilled.cloud/honeybadger/ingestion_keys";
import * as Projects from "@distilled.cloud/honeybadger/projects";
import { havePropsChanged, isResolved } from "alchemy/Diff";
import type { Diff } from "alchemy/Diff";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import type { Providers } from "./Providers.ts";

export interface IngestionKeyProps {
  /**
   * The project the key belongs to, for example `project.projectId`.
   * Changing it replaces the key.
   */
  projectId: string;
  /** A human-readable name. Changing it relabels the key in place. */
  label?: string;
}

export interface IngestionKeyAttributes {
  /** Honeybadger's id for the key. */
  keyId: string;
  /** The project the key belongs to. */
  projectId: string;
  /** The value a client sends as its `apiKey`. */
  key: Redacted.Redacted;
}

export type IngestionKey = Resource<
  "Honeybadger.IngestionKey",
  IngestionKeyProps,
  IngestionKeyAttributes,
  never,
  Providers
>;

/**
 * A project's ingestion key: the value a Honeybadger client sends with its
 * errors and events. The key is kept `Redacted` in state. Honeybadger returns
 * it on every read, so a redeploy reads the same value back.
 *
 * Honeybadger refuses to delete a project's last key. Deleting the resource
 * then leaves that key in place and prints a note saying so: the key stays
 * active in that project until the project is deleted or the key is removed
 * in Honeybadger.
 *
 * A key that is gone from Honeybadger is created again with a new id and
 * value, so only `projectId` is stable across an update.
 *
 * :::caution
 * Changing `projectId` replaces the key: the new project gets a new key
 * with a new value, and apps still sending the old value stop reporting
 * once the old key is deleted.
 * :::
 *
 * Keys carry no name of their own, so a key made by an interrupted deploy is
 * not found again: the next deploy makes another.
 *
 * ### Creating an ingestion key
 * **Example:** A key for a Worker to report with
 * ```typescript
 * const project = yield* Honeybadger.Project("Errors", { name: "momo" });
 * const key = yield* Honeybadger.IngestionKey("Worker", {
 *   projectId: project.projectId,
 *   label: "worker",
 * });
 * ```
 *
 * @resource
 * @product Error Tracking
 */
export const IngestionKey = Resource<IngestionKey>("Honeybadger.IngestionKey");

const stables: Extract<keyof IngestionKeyAttributes, string>[] = ["projectId"];

const toAttributes = (key: Keys.IngestionKey): IngestionKeyAttributes => ({
  key: Redacted.isRedacted(key.key) ? key.key : Redacted.make(key.key),
  keyId: key.id,
  projectId: key.project_id,
});

const plan = (olds: IngestionKeyProps, news: IngestionKeyProps): Diff => {
  if (news.projectId !== olds.projectId) {
    return { action: "replace" };
  }
  return havePropsChanged(olds, news) ? { action: "update", stables } : { action: "noop", stables };
};

export const IngestionKeyProvider = () =>
  Provider.effect(
    IngestionKey,
    Effect.gen(function* makeIngestionKeyProvider() {
      const getKey = yield* Keys.getIngestionKey;
      const createKey = yield* Keys.createIngestionKey;
      const updateKey = yield* Keys.updateIngestionKey;
      const deleteKey = yield* Keys.deleteIngestionKey;
      const listKeys = yield* Keys.listIngestionKeys;
      const listProjects = yield* Projects.listProjects;

      const observe = (output: IngestionKeyAttributes) =>
        getKey({
          ingestion_key_id: output.keyId,
          project_id: output.projectId,
        }).pipe(
          Effect.map((response): Keys.IngestionKey | undefined => response.data),
          Effect.catchTag("NotFound", () => Effect.undefined),
        );

      const onlyKeyLeft = (output: IngestionKeyAttributes) =>
        listKeys.items({ project_id: output.projectId }).pipe(
          Stream.map((key) => key.id),
          Stream.runCollect,
          Effect.map((ids) => ids.length === 1 && ids[0] === output.keyId),
          Effect.catchTag("NotFound", () => Effect.succeed(false)),
        );

      return {
        delete: Effect.fn(function* remove({ output, session }) {
          yield* deleteKey({
            ingestion_key_id: output.keyId,
            project_id: output.projectId,
          }).pipe(
            Effect.catchTags({
              NotFound: () => Effect.void,
              UnprocessableEntity: (refused) =>
                Effect.flatMap(onlyKeyLeft(output), (last) =>
                  last
                    ? session.note(
                        `Ingestion key ${output.keyId} is project ${output.projectId}'s last key, so Honeybadger kept it; it stays active in that project until the project is deleted or the key is removed in Honeybadger.`,
                      )
                    : Effect.fail(refused),
                ),
            }),
          );
        }),
        diff: ({ news, olds }) => Effect.succeed(isResolved(news) ? plan(olds, news) : undefined),
        list: () =>
          listProjects.items({}).pipe(
            Stream.flatMap((project) =>
              listKeys
                .items({ project_id: project.id })
                .pipe(Stream.catchTag("NotFound", () => Stream.empty)),
            ),
            Stream.map(toAttributes),
            Stream.runCollect,
          ),
        read: ({ output }) =>
          output === undefined
            ? Effect.undefined
            : observe(output).pipe(
                Effect.map((live) => (live === undefined ? undefined : toAttributes(live))),
              ),
        reconcile: Effect.fn(function* reconcile({ news, output }) {
          const live =
            output === undefined || output.projectId !== news.projectId
              ? undefined
              : yield* observe(output);
          if (live === undefined) {
            const created = yield* createKey({
              project_id: news.projectId,
              ...(news.label === undefined ? {} : { label: news.label }),
            });
            return toAttributes(created.data);
          }
          if (news.label === undefined || news.label === live.label) {
            return toAttributes(live);
          }
          const updated = yield* updateKey({
            ingestion_key_id: live.id,
            label: news.label,
            project_id: live.project_id,
          });
          return toAttributes(updated.data);
        }),
        stables,
      };
    }),
  );
