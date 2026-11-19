import * as Honeybadger from "@alchemy.run/honeybadger";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Dashboards from "@distilled.cloud/honeybadger/dashboards";
import { adopt, OwnedBySomeoneElse } from "alchemy/AdoptPolicy";
import * as Provider from "alchemy/Provider";
import * as RemovalPolicy from "alchemy/RemovalPolicy";
import * as Effect from "effect/Effect";
import { findOwnedError, getProject, logLevel } from "./support.ts";

const { test } = Test.make({ providers: Honeybadger.providers() });

const widgets = (query: string) => [
  {
    id: "events",
    type: "insights_vis",
    grid: { h: 4, w: 6, x: 0, y: 0 },
    presentation: { title: "Events" },
    config: {
      query,
      streams: ["default"],
      vis: { view: "line", chart_config: { xField: "bin(1h)", yField: "count()" } },
    },
  },
  {
    id: "deploys",
    type: "deployments",
    grid: { h: 4, w: 6, x: 6, y: 0 },
    presentation: { title: "Deploys" },
  },
];

const getDashboard = (projectId: string, dashboardId: string) =>
  Dashboards.getDashboard({ project_id: projectId, dashboard_id: dashboardId }).pipe(
    Effect.map((response): Dashboards.Dashboard | undefined => response.data),
    Effect.catchTag("NotFound", () => Effect.undefined),
  );

test.provider(
  "creates a dashboard, changes a query in place and deletes it",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const deployDashboard = (query: string | undefined) =>
        stack.deploy(
          Effect.gen(function* () {
            const project = yield* Honeybadger.Project("Errors", {}).pipe(RemovalPolicy.destroy());
            if (query === undefined) {
              return { project, dashboard: undefined };
            }
            const dashboard = yield* Honeybadger.Dashboard("Overview", {
              projectId: project.projectId,
              title: "Alchemy test",
              defaultTs: "P7D",
              widgets: widgets(query),
            });
            return { project, dashboard };
          }),
        );

      const first = yield* deployDashboard("stats count() by bin(1h)");
      const project = first.project;
      const dashboard = first.dashboard!;
      expect(dashboard.projectId).toEqual(project.projectId);
      expect(dashboard.url).toContain(dashboard.dashboardId);
      const created = yield* getDashboard(project.projectId, dashboard.dashboardId);
      expect(created?.title).toEqual("Alchemy test");
      expect(created?.widgets.map((w) => w.id)).toEqual(["events", "deploys"]);
      expect(created?.widgets[0]?.config?.query).toEqual("stats count() by bin(1h)");

      const changed = yield* deployDashboard("stats count() by bin(1d)");
      expect(changed.dashboard?.dashboardId).toEqual(dashboard.dashboardId);
      const updated = yield* getDashboard(project.projectId, dashboard.dashboardId);
      expect(updated?.widgets.map((w) => w.id)).toEqual(["events", "deploys"]);
      expect(updated?.widgets[0]?.config?.query).toEqual("stats count() by bin(1d)");

      const provider = yield* Provider.findProvider(Honeybadger.Dashboard);
      const listed = yield* provider.list();
      expect(listed.map((d) => d.dashboardId)).toContain(dashboard.dashboardId);

      yield* deployDashboard(undefined);
      expect(yield* getDashboard(project.projectId, dashboard.dashboardId)).toBeUndefined();
      expect(yield* getProject(project.projectId)).toBeDefined();

      yield* stack.destroy();
      expect(yield* getProject(project.projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:dashboard", "live"],
    timeout: 120_000,
  },
);

test.provider(
  "refuses a dashboard it doesn't own unless adopted",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const deploy = (dashboard: "none" | "owned" | "adopted") =>
        stack.deploy(
          Effect.gen(function* () {
            const project = yield* Honeybadger.Project("Errors", {}).pipe(RemovalPolicy.destroy());
            if (dashboard === "none") {
              return { project, dashboard: undefined };
            }
            const declared = Honeybadger.Dashboard("Existing", {
              projectId: project.projectId,
              title: "Alchemy existing",
              widgets: widgets("stats count() by bin(1d)"),
            });
            return {
              project,
              dashboard: yield* dashboard === "adopted" ? declared.pipe(adopt(true)) : declared,
            };
          }),
        );

      const { project } = yield* deploy("none");
      const existing = yield* Dashboards.createDashboard({
        project_id: project.projectId,
        title: "Alchemy existing",
        widgets: widgets("stats count() by bin(1h)"),
      });

      const error = yield* deploy("owned").pipe(
        Effect.as(undefined),
        Effect.catchCause((cause) => Effect.succeed(findOwnedError(cause))),
      );
      expect(error).toBeInstanceOf(OwnedBySomeoneElse);

      const adopted = yield* deploy("adopted");
      expect(adopted.dashboard?.dashboardId).toEqual(existing.data.id);
      const live = yield* getDashboard(project.projectId, existing.data.id);
      expect(live?.widgets[0]?.config?.query).toEqual("stats count() by bin(1d)");

      yield* stack.destroy();
      expect(yield* getProject(project.projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:dashboard", "live"],
    timeout: 120_000,
  },
);

test.provider(
  "replaces the dashboard when it moves to another project",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      // Both projects stay deployed in every step, so the old dashboard's
      // project outlives the replacement.
      const deployDashboard = (on: "a" | "b") =>
        stack.deploy(
          Effect.gen(function* () {
            const a = yield* Honeybadger.Project("A", {}).pipe(RemovalPolicy.destroy());
            const b = yield* Honeybadger.Project("B", {}).pipe(RemovalPolicy.destroy());
            const dashboard = yield* Honeybadger.Dashboard("Overview", {
              projectId: on === "a" ? a.projectId : b.projectId,
              title: "Alchemy moved",
              widgets: widgets("stats count() by bin(1h)"),
            });
            return { a, b, dashboard };
          }),
        );

      const first = yield* deployDashboard("a");
      expect(first.dashboard.projectId).toEqual(first.a.projectId);

      const moved = yield* deployDashboard("b");
      expect(moved.dashboard.projectId).toEqual(moved.b.projectId);
      expect(moved.dashboard.dashboardId).not.toEqual(first.dashboard.dashboardId);
      const created = yield* getDashboard(moved.b.projectId, moved.dashboard.dashboardId);
      expect(created?.title).toEqual("Alchemy moved");
      expect(yield* getDashboard(first.a.projectId, first.dashboard.dashboardId)).toBeUndefined();

      yield* stack.destroy();
      expect(yield* getProject(first.a.projectId)).toBeUndefined();
      expect(yield* getProject(moved.b.projectId)).toBeUndefined();
    }).pipe(logLevel),
  {
    tags: ["provider:honeybadger", "provider:honeybadger:dashboard", "live"],
    timeout: 120_000,
  },
);
