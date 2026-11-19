import * as Honeybadger from "@alchemy.run/honeybadger";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as RemovalPolicy from "alchemy/RemovalPolicy";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { getProject, logLevel } from "./support.ts";

const { test } = Test.make({ providers: Honeybadger.providers() });

test.provider(
  "uploads a build's source maps once per revision",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const path = yield* Path.Path;
      const fixtures = path.join(import.meta.dirname, "fixtures");

      const deployMaps = (revision: string) =>
        stack.deploy(
          Effect.gen(function* () {
            const project = yield* Honeybadger.Project("Errors", {}).pipe(RemovalPolicy.destroy());
            const key = yield* Honeybadger.IngestionKey("Key", { projectId: project.projectId });
            const maps = yield* Honeybadger.SourceMaps("Web", {
              apiKey: key.key,
              revision,
              jsDirectory: path.join(fixtures, "web"),
              mapDirectory: path.join(fixtures, "web-maps"),
              baseUrl: "https://alchemy.example.com/",
            });
            return { project, maps };
          }),
        );

      const first = yield* deployMaps("rev-1");
      expect(first.maps.revision).toEqual("rev-1");
      expect(first.maps.files).toEqual(1);

      const again = yield* deployMaps("rev-1");
      expect(again.maps).toEqual(first.maps);

      const next = yield* deployMaps("rev-2");
      expect(next.maps.revision).toEqual("rev-2");
      expect(next.maps.digest).toEqual(first.maps.digest);

      yield* stack.destroy();
      expect(yield* getProject(first.project.projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:source-maps", "live"],
    timeout: 120_000,
  },
);

test.provider(
  "uploads the same revision again for a new key, new contents or a new URL",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const fixtures = path.join(import.meta.dirname, "fixtures");

      // A copy of the fixture build, so the test can change a map's contents.
      const build = yield* fs.makeTempDirectoryScoped({ prefix: "alchemy-hb-maps-" });
      yield* fs.copy(path.join(fixtures, "web"), path.join(build, "web"));
      yield* fs.copy(path.join(fixtures, "web-maps"), path.join(build, "web-maps"));
      const mapPath = path.join(build, "web-maps", "assets", "app.js.map");

      const deployMaps = (opts: { key: "first" | "second"; baseUrl: string }) =>
        stack.deploy(
          Effect.gen(function* () {
            const project = yield* Honeybadger.Project("Errors", {}).pipe(RemovalPolicy.destroy());
            const first = yield* Honeybadger.IngestionKey("First", {
              projectId: project.projectId,
            });
            const second = yield* Honeybadger.IngestionKey("Second", {
              projectId: project.projectId,
            });
            const maps = yield* Honeybadger.SourceMaps("Web", {
              apiKey: opts.key === "first" ? first.key : second.key,
              revision: "rev-1",
              jsDirectory: path.join(build, "web"),
              mapDirectory: path.join(build, "web-maps"),
              baseUrl: opts.baseUrl,
            });
            return { project, maps };
          }),
        );

      const baseUrl = "https://alchemy.example.com/";
      const first = yield* deployMaps({ key: "first", baseUrl });
      expect(first.maps.files).toEqual(1);

      const rekeyed = yield* deployMaps({ key: "second", baseUrl });
      expect(rekeyed.maps.apiKeyDigest).not.toEqual(first.maps.apiKeyDigest);
      expect(rekeyed.maps.digest).toEqual(first.maps.digest);

      const map = JSON.parse(yield* fs.readFileString(mapPath)) as { sourceRoot?: string };
      yield* fs.writeFileString(mapPath, JSON.stringify({ ...map, sourceRoot: "/changed/" }));
      const rebuilt = yield* deployMaps({ key: "second", baseUrl });
      expect(rebuilt.maps.digest).not.toEqual(rekeyed.maps.digest);

      const moved = yield* deployMaps({ key: "second", baseUrl: "https://cdn.example.com/" });
      expect(moved.maps.digest).not.toEqual(rebuilt.maps.digest);

      // Nothing changed: nothing is uploaded.
      expect(yield* deployMaps({ key: "second", baseUrl: "https://cdn.example.com/" })).toEqual(
        moved,
      );

      yield* stack.destroy();
      expect(yield* getProject(first.project.projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:source-maps", "live"],
    timeout: 120_000,
  },
);
