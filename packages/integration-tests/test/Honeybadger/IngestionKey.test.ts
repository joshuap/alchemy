import * as Honeybadger from "@alchemy.run/honeybadger";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Keys from "@distilled.cloud/honeybadger/ingestion_keys";
import * as Provider from "alchemy/Provider";
import * as RemovalPolicy from "alchemy/RemovalPolicy";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import { getProject, logLevel } from "./support.ts";

const { test } = Test.make({ providers: Honeybadger.providers() });

const keyIds = (projectId: string) =>
  Keys.listIngestionKeys.items({ project_id: projectId }).pipe(
    Stream.map((key) => key.id),
    Stream.runCollect,
  );

const getLabel = (projectId: string, keyId: string) =>
  Keys.getIngestionKey({ project_id: projectId, ingestion_key_id: keyId }).pipe(
    Effect.map((response) => response.data.label),
  );

test.provider(
  "creates, relabels, lists and deletes a key",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const deployKey = (label: string | undefined) =>
        stack.deploy(
          Effect.gen(function* () {
            const project = yield* Honeybadger.Project("Errors", {}).pipe(RemovalPolicy.destroy());
            if (label === undefined) {
              return { project, key: undefined };
            }
            const key = yield* Honeybadger.IngestionKey("Key", {
              projectId: project.projectId,
              label,
            });
            return { project, key };
          }),
        );

      const first = yield* deployKey("first");
      const key = first.key!;
      const projectId = first.project.projectId;
      expect(key.projectId).toEqual(projectId);
      expect(Redacted.isRedacted(key.key)).toBe(true);
      expect(Redacted.value(key.key).startsWith("hbp_")).toBe(true);
      expect(yield* keyIds(projectId)).toContain(key.keyId);
      expect(yield* getLabel(projectId, key.keyId)).toEqual("first");

      const second = yield* deployKey("second");
      expect(second.key?.keyId).toEqual(key.keyId);
      const sameKey = Redacted.value(second.key!.key) === Redacted.value(key.key);
      expect(sameKey).toBe(true);
      expect(yield* getLabel(projectId, key.keyId)).toEqual("second");

      const provider = yield* Provider.findProvider(Honeybadger.IngestionKey);
      const listed = yield* provider.list();
      expect(listed.map((k) => k.keyId)).toContain(key.keyId);

      yield* deployKey(undefined);
      expect(yield* keyIds(projectId)).not.toContain(key.keyId);

      yield* stack.destroy();
      expect(yield* getProject(projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:ingestion-key", "live"],
    timeout: 120_000,
  },
);

test.provider(
  "keeps a project's last key when the resource is removed",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const deployKey = (withKey: boolean) =>
        stack.deploy(
          Effect.gen(function* () {
            const project = yield* Honeybadger.Project("Errors", {}).pipe(RemovalPolicy.destroy());
            const key = withKey
              ? yield* Honeybadger.IngestionKey("Key", { projectId: project.projectId })
              : undefined;
            return { project, key };
          }),
        );

      const { project, key } = yield* deployKey(true);
      const projectId = project.projectId;
      // Leave the resource's key as the project's only key.
      const others = (yield* keyIds(projectId)).filter((id) => id !== key!.keyId);
      for (const id of others) {
        yield* Keys.deleteIngestionKey({ project_id: projectId, ingestion_key_id: id });
      }
      expect(yield* keyIds(projectId)).toEqual([key!.keyId]);

      yield* deployKey(false);
      expect(yield* keyIds(projectId)).toEqual([key!.keyId]);

      yield* stack.destroy();
      expect(yield* getProject(projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:ingestion-key", "live"],
    timeout: 120_000,
  },
);

test.provider(
  "replaces the key when it moves to another project",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      // Both projects stay deployed in every step, so the old key's project
      // outlives the replacement.
      const deployKey = (on: "a" | "b") =>
        stack.deploy(
          Effect.gen(function* () {
            const a = yield* Honeybadger.Project("A", {}).pipe(RemovalPolicy.destroy());
            const b = yield* Honeybadger.Project("B", {}).pipe(RemovalPolicy.destroy());
            const key = yield* Honeybadger.IngestionKey("Key", {
              projectId: on === "a" ? a.projectId : b.projectId,
              label: "moved",
            });
            return { a, b, key };
          }),
        );

      const first = yield* deployKey("a");
      expect(first.key.projectId).toEqual(first.a.projectId);

      const moved = yield* deployKey("b");
      expect(moved.key.projectId).toEqual(moved.b.projectId);
      expect(moved.key.keyId).not.toEqual(first.key.keyId);
      expect(yield* keyIds(moved.b.projectId)).toContain(moved.key.keyId);
      expect(yield* keyIds(first.a.projectId)).not.toContain(first.key.keyId);

      yield* stack.destroy();
      expect(yield* getProject(first.a.projectId)).toBeUndefined();
      expect(yield* getProject(moved.b.projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:ingestion-key", "live"],
    timeout: 120_000,
  },
);
