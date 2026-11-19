import * as Dashboards from "@distilled.cloud/honeybadger/dashboards";
import * as Projects from "@distilled.cloud/honeybadger/projects";
import { unownedUnlessPrevious } from "alchemy/AdoptPolicy";
import { havePropsChanged, isResolved } from "alchemy/Diff";
import type { Diff } from "alchemy/Diff";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type { Providers } from "./Providers.ts";

/** A dashboard widget. Its `id` keeps it the same widget across updates. */
export type DashboardWidget = Dashboards.DashboardWidgetInput & { id: string };

export interface DashboardProps {
  /**
   * The project the dashboard belongs to, for example `project.projectId`.
   * Changing it replaces the dashboard.
   */
  projectId: string;
  /** The dashboard's title. Changing it renames the dashboard in place. */
  title: string;
  /** The widgets, in order. Each needs an `id` unique in the dashboard. */
  widgets: readonly DashboardWidget[];
  /**
   * Default time range: an ISO 8601 duration such as `P7D` or `PT3H`, a
   * keyword (`today`, `yesterday`, `week`, `month`) or a range such as
   * `2026-01-01/2026-01-31`. Honeybadger refuses `7d`. Left out, none is set.
   */
  defaultTs?: string;
}

export interface DashboardAttributes {
  /** Honeybadger's id for the dashboard. */
  dashboardId: string;
  /** The project the dashboard belongs to. */
  projectId: string;
  /** The dashboard's page in Honeybadger. */
  url: string;
}

export type Dashboard = Resource<
  "Honeybadger.Dashboard",
  DashboardProps,
  DashboardAttributes,
  never,
  Providers
>;

/**
 * A dashboard: a grid of widgets charting a project's queries,
 * errors, uptime, check-ins, deploys and alarms.
 *
 * Every update writes the whole dashboard (title, time range and widgets),
 * so edits made in Honeybadger's UI are replaced on the next deploy that
 * changes the props. A deploy also updates the dashboard when an
 * `insights_vis` widget read back has lost one of its `streams`: Honeybadger
 * drops a stream the project doesn't have yet, as happens right after the
 * project is created.
 *
 * :::caution
 * Changing `projectId` replaces the dashboard: it is deleted from the old
 * project and created again in the new one, with a new id and URL.
 * :::
 *
 * Dashboards carry no tags, so a dashboard with the same title in the
 * project that this stack has no state for is reported as someone else's,
 * and the deploy fails with `OwnedBySomeoneElse` unless adoption is allowed
 * (`--adopt`).
 *
 * ### Creating a dashboard
 * **Example:** An hourly error chart
 * ```typescript
 * const project = yield* Honeybadger.Project("Errors", { name: "momo" });
 * yield* Honeybadger.Dashboard("Overview", {
 *   projectId: project.projectId,
 *   title: "Momo",
 *   widgets: [
 *     {
 *       id: "errors",
 *       type: "insights_vis",
 *       config: { query: "stats count() by bin(1h)", streams: ["default"] },
 *     },
 *   ],
 * });
 * ```
 *
 * @resource
 * @product Dashboards
 */
export const Dashboard = Resource<Dashboard>("Honeybadger.Dashboard");

const stables: Extract<keyof DashboardAttributes, string>[] = ["projectId"];

const streamsOf = (widget: Dashboards.DashboardWidgetInput | undefined): readonly unknown[] => {
  const streams = widget?.config?.streams;
  return Array.isArray(streams) ? streams : [];
};

const streamsKept = (live: Dashboards.Dashboard, props: DashboardProps) =>
  props.widgets
    .filter((widget) => widget.type === "insights_vis")
    .every((widget) => {
      const read = streamsOf(live.widgets.find((w) => w.id === widget.id));
      return streamsOf(widget).every((stream) => read.includes(stream));
    });

const toAttributes = (dashboard: Dashboards.Dashboard): DashboardAttributes => ({
  dashboardId: dashboard.id,
  projectId: dashboard.project_id,
  url: dashboard.links.web,
});

const body = (props: DashboardProps) => ({
  default_ts: props.defaultTs ?? null,
  title: props.title,
  widgets: props.widgets.map((widget) => structuredClone(widget)),
});

export const DashboardProvider = () =>
  Provider.effect(
    Dashboard,
    Effect.gen(function* makeDashboardProvider() {
      const getDashboard = yield* Dashboards.getDashboard;
      const createDashboard = yield* Dashboards.createDashboard;
      const updateDashboard = yield* Dashboards.updateDashboard;
      const deleteDashboard = yield* Dashboards.deleteDashboard;
      const listDashboards = yield* Dashboards.listDashboards;
      const listProjects = yield* Projects.listProjects;

      const observe = (output: DashboardAttributes) =>
        getDashboard({
          dashboard_id: output.dashboardId,
          project_id: output.projectId,
        }).pipe(
          Effect.map((response): Dashboards.Dashboard | undefined => response.data),
          Effect.catchTag("NotFound", () => Effect.undefined),
        );

      const findByTitle = (projectId: string, title: string) =>
        listDashboards.items({ project_id: projectId }).pipe(
          Stream.filter((dashboard) => dashboard.title === title),
          Stream.runHead,
          Effect.map(Option.getOrUndefined),
          Effect.catchTag("NotFound", () => Effect.undefined),
        );

      const plan = Effect.fn(function* plan(
        olds: DashboardProps,
        news: DashboardProps,
        output: DashboardAttributes | undefined,
      ) {
        if (news.projectId !== olds.projectId) {
          return { action: "replace" } satisfies Diff;
        }
        if (output === undefined || havePropsChanged(olds, news)) {
          return { action: "update", stables } satisfies Diff;
        }
        const live = yield* observe(output);
        return {
          action: live !== undefined && streamsKept(live, news) ? "noop" : "update",
          stables,
        } satisfies Diff;
      });

      return {
        delete: Effect.fn(function* remove({ output }) {
          yield* deleteDashboard({
            dashboard_id: output.dashboardId,
            project_id: output.projectId,
          }).pipe(Effect.catchTag("NotFound", () => Effect.void));
        }),
        diff: ({ news, olds, output }) =>
          isResolved(news) ? plan(olds, news, output) : Effect.undefined,
        list: () =>
          listProjects.items({}).pipe(
            Stream.flatMap((project) =>
              listDashboards
                .items({ project_id: project.id })
                .pipe(Stream.catchTag("NotFound", () => Stream.empty)),
            ),
            Stream.map(toAttributes),
            Stream.runCollect,
          ),
        read: Effect.fn(function* read({ olds, output, previous }) {
          if (output !== undefined) {
            const live = yield* observe(output);
            return live === undefined ? undefined : toAttributes(live);
          }
          const found = yield* findByTitle(olds.projectId, olds.title);
          return found === undefined
            ? undefined
            : unownedUnlessPrevious(toAttributes(found), previous, "dashboardId");
        }),
        reconcile: Effect.fn(function* reconcile({ news, output }) {
          const live =
            output === undefined || output.projectId !== news.projectId
              ? undefined
              : yield* observe(output);
          if (live === undefined) {
            const created = yield* createDashboard({
              project_id: news.projectId,
              ...body(news),
            });
            return toAttributes(created.data);
          }
          const updated = yield* updateDashboard({
            dashboard_id: live.id,
            project_id: live.project_id,
            ...body(news),
          });
          return toAttributes(updated.data);
        }),
        stables,
      };
    }),
  );
