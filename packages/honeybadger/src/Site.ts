import * as Projects from "@distilled.cloud/honeybadger/projects";
import * as Sites from "@distilled.cloud/honeybadger/uptime_sites";
import { unownedUnlessPrevious } from "alchemy/AdoptPolicy";
import { havePropsChanged, isResolved } from "alchemy/Diff";
import type { Diff } from "alchemy/Diff";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type { Providers } from "./Providers.ts";

export interface SiteProps {
  /**
   * The project the site belongs to, for example `project.projectId`.
   * Changing it replaces the site.
   */
  projectId: string;
  /** Name shown in Honeybadger and on status pages. */
  name: string;
  /** The URL to check, with its `http://` or `https://` scheme. */
  url: string;
  /**
   * How a check decides the site is up: `success` for any 2xx, `exact` for
   * a status code equal to `match`, `include` or `exclude` for a body that
   * does or doesn't contain `match`, `jmespath` for a JMESPath expression
   * on the JSON body (numbers in backticks).
   */
  matchType: "success" | "exact" | "include" | "exclude" | "jmespath";
  /** What `matchType` compares against. Ignored with `success`. */
  match?: string;
  /** Minutes between checks. */
  frequency?: 1 | 2 | 5 | 15;
  /**
   * HTTP method each check sends.
   * @default "GET"
   */
  requestMethod?: Sites.SiteRequestMethod;
  /** Locations to check from. Empty means every location. */
  locations?: readonly string[];
  /** Consecutive failures before an outage is declared. */
  outageThreshold?: number;
  /**
   * Request timeout in seconds.
   * @default 30
   */
  timeout?: number;
  /** Fail the check when the TLS certificate does not validate. */
  validateSsl?: boolean;
}

export interface SiteAttributes {
  /** Honeybadger's id for the site. */
  siteId: string;
  /** The project the site belongs to. */
  projectId: string;
  /** The site's state when it was last deployed. */
  state: "pending" | "up" | "down";
}

export type Site = Resource<"Honeybadger.Site", SiteProps, SiteAttributes, never, Providers>;

/**
 * An uptime check: Honeybadger requests the URL on a schedule from several
 * locations and alerts the project when it goes down and when it recovers.
 *
 * Settings left out of the props are not managed. Every change except the
 * project happens in place.
 *
 * :::caution
 * Changing `projectId` replaces the site: the check in the old project is
 * deleted and a new one starts in the new project, with no outage history.
 * :::
 *
 * Sites carry no tags, so a site with the same name in the project that this
 * stack has no state for is reported as someone else's, and the deploy
 * fails with `OwnedBySomeoneElse` unless adoption is allowed (`--adopt`).
 *
 * ### Checking a health endpoint
 * **Example:** Check a JSON health endpoint every 5 minutes
 * ```typescript
 * const project = yield* Honeybadger.Project("Errors", { name: "momo" });
 * yield* Honeybadger.Site("Health", {
 *   projectId: project.projectId,
 *   name: "momo health",
 *   url: "https://momo.example.com/health",
 *   matchType: "jmespath",
 *   match: "ok == `true`",
 *   frequency: 5,
 * });
 * ```
 *
 * **Example:** Check that a page contains some text, from two locations
 * ```typescript
 * const project = yield* Honeybadger.Project("Errors", { name: "momo" });
 * yield* Honeybadger.Site("Home", {
 *   projectId: project.projectId,
 *   name: "momo home",
 *   url: "https://momo.example.com/",
 *   matchType: "include",
 *   match: "Welcome to Momo",
 *   locations: ["Virginia", "Frankfurt"],
 * });
 * ```
 *
 * @resource
 * @product Uptime
 */
export const Site = Resource<Site>("Honeybadger.Site");

type Settings = Omit<Sites.UpdateSiteRequest, "project_id" | "site_id">;

const DEFAULT_TIMEOUT = 30;

const desired = (props: SiteProps): Settings & { url: string } => ({
  match_type: props.matchType,
  name: props.name,
  url: props.url,
  ...(props.matchType === "success" || props.match === undefined ? {} : { match: props.match }),
  ...(props.frequency === undefined ? {} : { frequency: props.frequency }),
  ...(props.requestMethod === undefined ? {} : { request_method: props.requestMethod }),
  ...(props.locations === undefined ? {} : { locations: [...props.locations] }),
  ...(props.outageThreshold === undefined ? {} : { outage_threshold: props.outageThreshold }),
  ...(props.timeout === undefined ? {} : { timeout: props.timeout }),
  ...(props.validateSsl === undefined ? {} : { validate_ssl: props.validateSsl }),
});

const sameSet = (a: readonly string[], b: readonly string[]) => {
  const left = new Set(a);
  const right = new Set(b);
  return left.size === right.size && [...left].every((x) => right.has(x));
};

type Managed = keyof Settings & keyof Sites.Site;

const managed: readonly Managed[] = [
  "name",
  "url",
  "match_type",
  "match",
  "frequency",
  "request_method",
  "locations",
  "outage_threshold",
  "timeout",
  "validate_ssl",
];

const differs = (live: Sites.Site, want: Settings, key: Managed) => {
  const value = want[key];
  if (value === undefined) {
    return false;
  }
  if (key === "locations") {
    return !sameSet(live.locations, want.locations ?? []);
  }
  if (key === "timeout") {
    return value !== (live.timeout ?? DEFAULT_TIMEOUT);
  }
  if (key === "request_method") {
    return (want.request_method ?? "").toUpperCase() !== live.request_method.toUpperCase();
  }
  return value !== live[key];
};

const drift = (live: Sites.Site, want: Settings): Settings =>
  Object.fromEntries(
    managed.filter((key) => differs(live, want, key)).map((key) => [key, want[key]]),
  );

const stables: Extract<keyof SiteAttributes, string>[] = ["projectId"];

const plan = (olds: SiteProps, news: SiteProps): Diff => {
  if (news.projectId !== olds.projectId) {
    return { action: "replace" };
  }
  return havePropsChanged(olds, news) ? { action: "update", stables } : { action: "noop", stables };
};

const toAttributes = (site: Sites.Site): SiteAttributes => ({
  projectId: site.project_id,
  siteId: site.id,
  state: site.state,
});

export const SiteProvider = () =>
  Provider.effect(
    Site,
    Effect.gen(function* makeSiteProvider() {
      const getSite = yield* Sites.getSite;
      const createSite = yield* Sites.createSite;
      const updateSite = yield* Sites.updateSite;
      const deleteSite = yield* Sites.deleteSite;
      const listSites = yield* Sites.listSites;
      const listProjects = yield* Projects.listProjects;

      const observe = (output: SiteAttributes) =>
        getSite({ project_id: output.projectId, site_id: output.siteId }).pipe(
          Effect.map((response): Sites.Site | undefined => response.data),
          Effect.catchTag("NotFound", () => Effect.undefined),
        );

      const findByName = (projectId: string, name: string) =>
        listSites.items({ project_id: projectId }).pipe(
          Stream.filter((site) => site.name === name),
          Stream.runHead,
          Effect.map(Option.getOrUndefined),
          Effect.catchTag("NotFound", () => Effect.undefined),
        );

      return {
        delete: Effect.fn(function* remove({ output }) {
          yield* deleteSite({
            project_id: output.projectId,
            site_id: output.siteId,
          }).pipe(Effect.catchTag("NotFound", () => Effect.void));
        }),
        diff: ({ news, olds }) => Effect.succeed(isResolved(news) ? plan(olds, news) : undefined),
        list: () =>
          listProjects.items({}).pipe(
            Stream.flatMap((project) =>
              listSites
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
          const found = yield* findByName(olds.projectId, olds.name);
          return found === undefined
            ? undefined
            : unownedUnlessPrevious(toAttributes(found), previous, "siteId");
        }),
        reconcile: Effect.fn(function* reconcile({ news, output }) {
          const want = desired(news);
          const live =
            output === undefined || output.projectId !== news.projectId
              ? undefined
              : yield* observe(output);
          if (live === undefined) {
            const created = yield* createSite({
              project_id: news.projectId,
              ...want,
            });
            return toAttributes(created.data);
          }
          const changes = drift(live, want);
          if (Object.keys(changes).length === 0) {
            return toAttributes(live);
          }
          const updated = yield* updateSite({
            project_id: live.project_id,
            site_id: live.id,
            ...changes,
          });
          return toAttributes(updated.data);
        }),
        stables,
      };
    }),
  );
