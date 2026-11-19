import * as Honeybadger from "@alchemy.run/honeybadger";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Sites from "@distilled.cloud/honeybadger/uptime_sites";
import { adopt, OwnedBySomeoneElse } from "alchemy/AdoptPolicy";
import * as Provider from "alchemy/Provider";
import * as RemovalPolicy from "alchemy/RemovalPolicy";
import * as Effect from "effect/Effect";
import { findOwnedError, getProject, logLevel } from "./support.ts";

const { test } = Test.make({ providers: Honeybadger.providers() });

const getSite = (projectId: string, siteId: string) =>
  Sites.getSite({ project_id: projectId, site_id: siteId }).pipe(
    Effect.map((response): Sites.Site | undefined => response.data),
    Effect.catchTag("NotFound", () => Effect.undefined),
  );

test.provider(
  "creates a JMESPath check, changes its frequency in place and deletes it",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const deploySite = (frequency: 5 | 15 | undefined) =>
        stack.deploy(
          Effect.gen(function* () {
            const project = yield* Honeybadger.Project("Errors", {}).pipe(RemovalPolicy.destroy());
            if (frequency === undefined) {
              return { project, site: undefined };
            }
            const site = yield* Honeybadger.Site("Health", {
              projectId: project.projectId,
              name: "alchemy health",
              url: "https://alchemy.run/",
              matchType: "jmespath",
              match: "ok == `true`",
              frequency,
              locations: ["Virginia", "Frankfurt"],
            });
            return { project, site };
          }),
        );

      const first = yield* deploySite(5);
      const project = first.project;
      const site = first.site!;
      expect(site.projectId).toEqual(project.projectId);
      const created = yield* getSite(project.projectId, site.siteId);
      expect(created?.match_type).toEqual("jmespath");
      expect(created?.match).toEqual("ok == `true`");
      expect(created?.frequency).toEqual(5);
      expect([...(created?.locations ?? [])].toSorted()).toEqual(["Frankfurt", "Virginia"]);

      const slower = yield* deploySite(15);
      expect(slower.site?.siteId).toEqual(site.siteId);
      const updated = yield* getSite(project.projectId, site.siteId);
      expect(updated?.frequency).toEqual(15);
      expect(updated?.match).toEqual("ok == `true`");

      const provider = yield* Provider.findProvider(Honeybadger.Site);
      const listed = yield* provider.list();
      expect(listed.map((s) => s.siteId)).toContain(site.siteId);

      yield* deploySite(undefined);
      expect(yield* getSite(project.projectId, site.siteId)).toBeUndefined();
      expect(yield* getProject(project.projectId)).toBeDefined();

      yield* stack.destroy();
      expect(yield* getProject(project.projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:site", "live"],
    timeout: 120_000,
  },
);

test.provider(
  "refuses a site it doesn't own unless adopted",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const deploy = (site: "none" | "owned" | "adopted") =>
        stack.deploy(
          Effect.gen(function* () {
            const project = yield* Honeybadger.Project("Errors", {}).pipe(RemovalPolicy.destroy());
            if (site === "none") {
              return { project, site: undefined };
            }
            const declared = Honeybadger.Site("Existing", {
              projectId: project.projectId,
              name: "alchemy existing",
              url: "https://alchemy.run/",
              matchType: "success",
              frequency: 15,
            });
            return {
              project,
              site: yield* site === "adopted" ? declared.pipe(adopt(true)) : declared,
            };
          }),
        );

      const { project } = yield* deploy("none");
      const existing = yield* Sites.createSite({
        project_id: project.projectId,
        name: "alchemy existing",
        url: "https://alchemy.run/",
        match_type: "success",
      });

      const error = yield* deploy("owned").pipe(
        Effect.as(undefined),
        Effect.catchCause((cause) => Effect.succeed(findOwnedError(cause))),
      );
      expect(error).toBeInstanceOf(OwnedBySomeoneElse);

      const adopted = yield* deploy("adopted");
      expect(adopted.site?.siteId).toEqual(existing.data.id);
      expect((yield* getSite(project.projectId, existing.data.id))?.frequency).toEqual(15);

      yield* stack.destroy();
      expect(yield* getProject(project.projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:site", "live"],
    timeout: 120_000,
  },
);

test.provider(
  "replaces the check when it moves to another project",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      // Both projects stay deployed in every step, so the old check's project
      // outlives the replacement.
      const deploySite = (on: "a" | "b") =>
        stack.deploy(
          Effect.gen(function* () {
            const a = yield* Honeybadger.Project("A", {}).pipe(RemovalPolicy.destroy());
            const b = yield* Honeybadger.Project("B", {}).pipe(RemovalPolicy.destroy());
            const site = yield* Honeybadger.Site("Health", {
              projectId: on === "a" ? a.projectId : b.projectId,
              name: "alchemy moved",
              url: "https://alchemy.run/",
              matchType: "success",
            });
            return { a, b, site };
          }),
        );

      const first = yield* deploySite("a");
      expect(first.site.projectId).toEqual(first.a.projectId);

      const moved = yield* deploySite("b");
      expect(moved.site.projectId).toEqual(moved.b.projectId);
      expect(moved.site.siteId).not.toEqual(first.site.siteId);
      expect((yield* getSite(moved.b.projectId, moved.site.siteId))?.name).toEqual("alchemy moved");
      expect(yield* getSite(first.a.projectId, first.site.siteId)).toBeUndefined();

      yield* stack.destroy();
      expect(yield* getProject(first.a.projectId)).toBeUndefined();
      expect(yield* getProject(moved.b.projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:site", "live"],
    timeout: 120_000,
  },
);
