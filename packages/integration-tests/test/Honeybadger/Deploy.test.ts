import * as Honeybadger from "@alchemy.run/honeybadger";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Deploys from "@distilled.cloud/honeybadger/deploys";
import * as RemovalPolicy from "alchemy/RemovalPolicy";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import { getProject, logLevel } from "./support.ts";

const { test } = Test.make({ providers: Honeybadger.providers() });

// Honeybadger accepts a deploy at once but lists it a moment later.
const waitForRevisions = (projectId: string, expected: ReadonlyArray<string>) =>
  Deploys.listDeploys.items({ project_id: projectId, environment: "test" }).pipe(
    Stream.map((deploy) => deploy.revision),
    Stream.runCollect,
    Effect.map((found) => [...found].toSorted()),
    Effect.repeat({
      schedule: Schedule.spaced("3 seconds"),
      until: (found) => JSON.stringify(found) === JSON.stringify(expected),
      times: 10,
    }),
  );

test.provider(
  "records one deploy per revision",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const deployRelease = (revision: string) =>
        stack.deploy(
          Effect.gen(function* () {
            const project = yield* Honeybadger.Project("Errors", {}).pipe(RemovalPolicy.destroy());
            const key = yield* Honeybadger.IngestionKey("Key", { projectId: project.projectId });
            const deploy = yield* Honeybadger.Deploy("Release", {
              apiKey: key.key,
              environment: "test",
              revision,
              repository: "https://github.com/alchemy-run/alchemy",
              localUsername: "alchemy-test",
            });
            return { project, deploy };
          }),
        );

      const first = yield* deployRelease("rev-1");
      const projectId = first.project.projectId;
      expect(first.deploy.revision).toEqual("rev-1");
      expect(yield* waitForRevisions(projectId, ["rev-1"])).toEqual(["rev-1"]);

      const unchanged = yield* deployRelease("rev-1");
      expect(unchanged.deploy).toEqual(first.deploy);

      yield* deployRelease("rev-2");
      expect(yield* waitForRevisions(projectId, ["rev-1", "rev-2"])).toEqual(["rev-1", "rev-2"]);

      yield* stack.destroy();
      expect(yield* getProject(projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:deploy", "live"],
    timeout: 120_000,
  },
);

// Every deploy of the project, as "environment:revision", sorted.
const recordedDeploys = (projectId: string) =>
  Deploys.listDeploys.items({ project_id: projectId }).pipe(
    Stream.map((deploy) => `${deploy.environment}:${deploy.revision}`),
    Stream.runCollect,
    Effect.map((found) => [...found].toSorted()),
  );

const waitForDeploys = (projectId: string, expected: ReadonlyArray<string>) =>
  recordedDeploys(projectId).pipe(
    Effect.repeat({
      schedule: Schedule.spaced("3 seconds"),
      until: (found) => JSON.stringify(found) === JSON.stringify(expected),
      times: 10,
    }),
  );

test.provider(
  "records the same revision again for a new environment or API key",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const deployRelease = (opts: {
        environment: string;
        key: "first" | "second";
        repository: string;
      }) =>
        stack.deploy(
          Effect.gen(function* () {
            const project = yield* Honeybadger.Project("Errors", {}).pipe(RemovalPolicy.destroy());
            const first = yield* Honeybadger.IngestionKey("First", {
              projectId: project.projectId,
            });
            const second = yield* Honeybadger.IngestionKey("Second", {
              projectId: project.projectId,
            });
            const deploy = yield* Honeybadger.Deploy("Release", {
              apiKey: opts.key === "first" ? first.key : second.key,
              environment: opts.environment,
              revision: "rev-1",
              repository: opts.repository,
            });
            return { project, deploy };
          }),
        );

      const repository = "https://github.com/alchemy-run/alchemy";
      const first = yield* deployRelease({ environment: "test", key: "first", repository });
      const projectId = first.project.projectId;
      expect(yield* waitForDeploys(projectId, ["test:rev-1"])).toEqual(["test:rev-1"]);

      // Only the repository changed: nothing new is recorded.
      const relinked = yield* deployRelease({
        environment: "test",
        key: "first",
        repository: "https://github.com/alchemy-run/other",
      });
      expect(relinked.deploy).toEqual(first.deploy);

      const staging = yield* deployRelease({ environment: "staging", key: "first", repository });
      expect(staging.deploy.environment).toEqual("staging");
      expect(yield* waitForDeploys(projectId, ["staging:rev-1", "test:rev-1"])).toEqual([
        "staging:rev-1",
        "test:rev-1",
      ]);

      const rekeyed = yield* deployRelease({ environment: "staging", key: "second", repository });
      expect(rekeyed.deploy.apiKeyDigest).not.toEqual(staging.deploy.apiKeyDigest);
      expect(
        yield* waitForDeploys(projectId, ["staging:rev-1", "staging:rev-1", "test:rev-1"]),
      ).toEqual(["staging:rev-1", "staging:rev-1", "test:rev-1"]);

      yield* stack.destroy();
      expect(yield* getProject(projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:deploy", "live"],
    timeout: 120_000,
  },
);
