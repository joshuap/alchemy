import * as Projects from "@distilled.cloud/honeybadger/projects";
import { unownedUnlessPrevious } from "alchemy/AdoptPolicy";
import { havePropsChanged, isResolved } from "alchemy/Diff";
import type { Diff } from "alchemy/Diff";
import { createPhysicalName } from "alchemy/PhysicalName";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type { Providers } from "./Providers.ts";

export interface ProjectProps {
  /**
   * The project's name. Defaults to a name made from the stack, the logical
   * id, the stage and a random suffix. Changing it renames the project in
   * place.
   */
  name?: string;
  /** Primary language, used to format backtraces (for example `js`). */
  language?: string;
  /** Resolve every open fault when a deploy is recorded. */
  resolveErrorsOnDeploy?: boolean;
  /** Disallow shareable public fault links. */
  disablePublicLinks?: boolean;
}

export interface ProjectAttributes {
  /** Honeybadger's id for the project. Other resources take it as `projectId`. */
  projectId: string;
  /** The project's name in Honeybadger. */
  name: string;
  /** The id of the Honeybadger account that owns the project. */
  accountId: string;
}

/**
 * Props for {@link Project.lookup}: find a project this stack doesn't manage
 * by its name.
 */
export interface ProjectLookup {
  /** The project's exact name in Honeybadger. */
  name: string;
}

export type Project = Resource<
  "Honeybadger.Project",
  ProjectProps,
  ProjectAttributes,
  never,
  Providers,
  ProjectLookup
>;

/**
 * A Honeybadger project: one application or service. Errors, logs and events,
 * uptime sites, check-ins and deploys belong to a project.
 *
 * Settings left out of the props are not managed: a deploy leaves them as
 * they are in Honeybadger.
 *
 * Honeybadger projects carry no tags, so a project with an explicit name that
 * this stack has no state for is reported as someone else's, and the deploy
 * fails with `OwnedBySomeoneElse` unless adoption is allowed (`--adopt`). A
 * generated name embeds this resource's instance id, so a project with that
 * name is this resource's own, left by an interrupted create.
 *
 * A project holds history that can't be recreated, so removing the resource
 * or destroying the stack keeps the project in Honeybadger and only forgets
 * it. Pipe the declaration through `RemovalPolicy.destroy()` to delete it.
 *
 * ### Creating a project
 * **Example:** A JavaScript project that resolves errors on deploy
 * ```typescript
 * const project = yield* Honeybadger.Project("Errors", {
 *   name: "momo",
 *   language: "js",
 *   resolveErrorsOnDeploy: true,
 * });
 * ```
 *
 * **Example:** Delete the project when the stack is destroyed
 * ```typescript
 * const project = yield* Honeybadger.Project("Preview", {
 *   language: "js",
 * }).pipe(RemovalPolicy.destroy());
 * ```
 *
 * ### Looking up an existing project
 * `Project.lookup` references a project Alchemy doesn't manage, such as one
 * created in Honeybadger's UI. It is read during plan and deploy, and never
 * created, updated or deleted.
 *
 * **Example:** Add an ingestion key to an existing project
 * ```typescript
 * const project = Honeybadger.Project.lookup({ name: "momo" });
 * const key = yield* Honeybadger.IngestionKey("Worker", {
 *   projectId: project.projectId,
 * });
 * ```
 *
 * @resource
 * @product Error Tracking
 */
export const Project = Resource<Project>("Honeybadger.Project", {
  defaultRemovalPolicy: "retain",
});

type Settings = Pick<
  Projects.UpdateProjectRequest,
  "disable_public_links" | "language" | "name" | "resolve_errors_on_deploy"
>;

const desired = (name: string, props: ProjectProps): Settings & { name: string } => ({
  name,
  ...(props.language === undefined ? {} : { language: props.language }),
  ...(props.resolveErrorsOnDeploy === undefined
    ? {}
    : { resolve_errors_on_deploy: props.resolveErrorsOnDeploy }),
  ...(props.disablePublicLinks === undefined
    ? {}
    : { disable_public_links: props.disablePublicLinks }),
});

const changed = (live: Projects.Project, want: Settings, key: keyof Settings): Settings =>
  want[key] === undefined || want[key] === live[key] ? {} : { [key]: want[key] };

const drift = (live: Projects.Project, want: Settings): Settings => ({
  ...changed(live, want, "name"),
  ...changed(live, want, "language"),
  ...changed(live, want, "resolve_errors_on_deploy"),
  ...changed(live, want, "disable_public_links"),
});

const plan = (olds: ProjectProps, news: ProjectProps): Diff => {
  if (!havePropsChanged(olds, news)) {
    return { action: "noop" };
  }
  return {
    action: "update",
    stables: news.name === olds.name ? ["accountId", "name"] : ["accountId"],
  };
};

const toAttributes = (project: Projects.Project): ProjectAttributes => ({
  accountId: project.account_id,
  name: project.name,
  projectId: project.id,
});

const physicalName = (id: string, instanceId: string, props: ProjectProps) =>
  props.name === undefined ? createPhysicalName({ id, instanceId }) : Effect.succeed(props.name);

export const ProjectProvider = () =>
  Provider.effect(
    Project,
    Effect.gen(function* makeProjectProvider() {
      const getProject = yield* Projects.getProject;
      const createProject = yield* Projects.createProject;
      const updateProject = yield* Projects.updateProject;
      const deleteProject = yield* Projects.deleteProject;
      const listProjects = yield* Projects.listProjects;

      const observe = (projectId: string) =>
        getProject({ project_id: projectId }).pipe(
          Effect.map((response): Projects.Project | undefined => response.data),
          Effect.catchTag("NotFound", () => Effect.undefined),
        );

      const findByName = (name: string) =>
        listProjects.items({ name }).pipe(
          Stream.filter((project) => project.name === name),
          Stream.runHead,
          Effect.map(Option.getOrUndefined),
        );

      return {
        delete: Effect.fn(function* remove({ output }) {
          yield* deleteProject({ project_id: output.projectId }).pipe(
            Effect.catchTag("NotFound", () => Effect.void),
          );
        }),
        diff: ({ news, olds }) => Effect.succeed(isResolved(news) ? plan(olds, news) : undefined),
        list: () => listProjects.items({}).pipe(Stream.map(toAttributes), Stream.runCollect),
        lookup: Effect.fn(function* lookup({ name }) {
          const found = yield* findByName(name);
          return found === undefined ? undefined : toAttributes(found);
        }),
        read: Effect.fn(function* read({ id, instanceId, olds, output, previous }) {
          if (output !== undefined) {
            const live = yield* observe(output.projectId);
            return live === undefined ? undefined : toAttributes(live);
          }
          const name = yield* physicalName(id, instanceId, olds);
          const found = yield* findByName(name);
          const attributes = found === undefined ? undefined : toAttributes(found);
          return attributes === undefined || olds.name === undefined
            ? attributes
            : unownedUnlessPrevious(attributes, previous, "projectId");
        }),
        reconcile: Effect.fn(function* reconcile({ id, instanceId, news, output }) {
          const name = yield* physicalName(id, instanceId, news);
          const want = desired(name, news);
          const live = output === undefined ? undefined : yield* observe(output.projectId);
          if (live === undefined) {
            const created = yield* createProject(want);
            return toAttributes(created.data);
          }
          const changes = drift(live, want);
          if (Object.keys(changes).length === 0) {
            return toAttributes(live);
          }
          const updated = yield* updateProject({
            project_id: live.id,
            ...changes,
          });
          return toAttributes(updated.data);
        }),
        stables: ["accountId"],
      };
    }),
  );
