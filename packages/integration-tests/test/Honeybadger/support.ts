import * as Projects from "@distilled.cloud/honeybadger/projects";
import { OwnedBySomeoneElse } from "alchemy/AdoptPolicy";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import { MinimumLogLevel } from "effect/References";

export const logLevel = Effect.provideService(
  MinimumLogLevel,
  process.env.DEBUG ? "Debug" : "Info",
);

/** The live project, or `undefined` once Honeybadger has deleted it. */
export const getProject = (projectId: string) =>
  Projects.getProject({ project_id: projectId }).pipe(
    Effect.map((response): Projects.Project | undefined => response.data),
    Effect.catchTag("NotFound", () => Effect.undefined),
  );

export const findOwnedError = (cause: Cause.Cause<unknown>): OwnedBySomeoneElse | undefined =>
  cause.reasons
    .map((reason) =>
      Cause.isFailReason(reason)
        ? reason.error
        : Cause.isDieReason(reason)
          ? reason.defect
          : undefined,
    )
    .find((value): value is OwnedBySomeoneElse => value instanceof OwnedBySomeoneElse);
