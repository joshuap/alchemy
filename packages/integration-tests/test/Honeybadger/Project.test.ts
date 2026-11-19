import * as Honeybadger from "@alchemy.run/honeybadger";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Keys from "@distilled.cloud/honeybadger/ingestion_keys";
import * as Projects from "@distilled.cloud/honeybadger/projects";
import { adopt, OwnedBySomeoneElse } from "alchemy/AdoptPolicy";
import * as Provider from "alchemy/Provider";
import * as RemovalPolicy from "alchemy/RemovalPolicy";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { findOwnedError, getProject, logLevel } from "./support.ts";

const { test } = Test.make({ providers: Honeybadger.providers() });

test.provider(
  "creates, updates, lists and deletes a project",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const deployProject = (resolveErrorsOnDeploy: boolean) =>
        stack.deploy(
          Honeybadger.Project("Errors", {
            disablePublicLinks: true,
            language: "js",
            resolveErrorsOnDeploy,
          }).pipe(RemovalPolicy.destroy()),
        );

      const created = yield* deployProject(true);
      const first = yield* getProject(created.projectId);
      expect(first?.name).toEqual(created.name);
      expect(first?.language).toEqual("js");
      expect(first?.resolve_errors_on_deploy).toBe(true);
      expect(first?.disable_public_links).toBe(true);

      const updated = yield* deployProject(false);
      expect(updated.projectId).toEqual(created.projectId);
      const second = yield* getProject(created.projectId);
      expect(second?.resolve_errors_on_deploy).toBe(false);
      expect(second?.disable_public_links).toBe(true);

      const provider = yield* Provider.findProvider(Honeybadger.Project);
      const listed = yield* provider.list();
      expect(listed).toContainEqual(created);

      yield* stack.destroy();
      expect(yield* getProject(created.projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:project", "live"],
    timeout: 120_000,
  },
);

// Deletes a project this test created outside the stack, even when the
// test fails, so a failed run leaves nothing behind.
const deleteOwnFixture = (projectId: string) =>
  Projects.deleteProject({ project_id: projectId }).pipe(
    Effect.catchTag("NotFound", () => Effect.void),
    Effect.ignore,
  );

test.provider(
  "keeps the project in Honeybadger when it is removed",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const created = yield* stack.deploy(Honeybadger.Project("Kept", {}));
      yield* Effect.gen(function* () {
        yield* stack.destroy();
        expect(yield* getProject(created.projectId)).toBeDefined();
      }).pipe(Effect.ensuring(deleteOwnFixture(created.projectId)));
      expect(yield* getProject(created.projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:project", "live"],
    timeout: 120_000,
  },
);

test.provider(
  "refuses a project it doesn't own unless adopted",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const name = `alchemy-test-${stack.stage}-adopt`;
      const existing = yield* Projects.createProject({ name });
      yield* Effect.addFinalizer(() => deleteOwnFixture(existing.data.id));

      const deployProject = (adopted: boolean) => {
        const project = Honeybadger.Project("Existing", { name, language: "js" }).pipe(
          RemovalPolicy.destroy(),
        );
        return stack.deploy(adopted ? project.pipe(adopt(true)) : project);
      };

      const error = yield* deployProject(false).pipe(
        Effect.as(undefined),
        Effect.catchCause((cause) => Effect.succeed(findOwnedError(cause))),
      );
      expect(error).toBeInstanceOf(OwnedBySomeoneElse);

      const adopted = yield* deployProject(true);
      expect(adopted.projectId).toEqual(existing.data.id);
      expect((yield* getProject(existing.data.id))?.language).toEqual("js");

      yield* stack.destroy();
      expect(yield* getProject(existing.data.id)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:project", "live"],
    timeout: 120_000,
  },
);

test.provider(
  "looks up a project it doesn't manage and never deletes it",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const name = `alchemy-test-${stack.stage}-lookup`;
      const existing = yield* Projects.createProject({ name });
      yield* Effect.addFinalizer(() => deleteOwnFixture(existing.data.id));

      const program = Honeybadger.IngestionKey("OnLookedUp", {
        projectId: Honeybadger.Project.lookup({ name }).projectId,
        label: "lookup",
      });
      const key = yield* stack.deploy(program);
      expect(key.projectId).toEqual(existing.data.id);

      // A lookup resolves to the same project again.
      const plan = yield* stack.plan(program);
      expect(plan.resources.OnLookedUp?.action).toEqual("noop");

      // Destroying the stack removes the key, never the looked-up project.
      yield* stack.destroy();
      const keys = yield* Keys.listIngestionKeys.items({ project_id: existing.data.id }).pipe(
        Stream.map((k) => k.id),
        Stream.runCollect,
      );
      expect(keys).not.toContain(key.keyId);
      expect(yield* getProject(existing.data.id)).toBeDefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:project", "live"],
    timeout: 120_000,
  },
);
