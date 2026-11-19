import { Retry as HoneybadgerRetry } from "@distilled.cloud/honeybadger/Retry";
import { CredentialsStoreLive } from "alchemy/Auth/Credentials";
import { ProfileStoreLive } from "alchemy/Auth/Profile";
import * as DeployRetry from "alchemy/DeployRetry";
import * as Provider from "alchemy/Provider";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import { HoneybadgerAuth } from "./AuthProvider.ts";
import * as Credentials from "./Credentials.ts";
import { Dashboard, DashboardProvider } from "./Dashboard.ts";
import { Deploy, DeployProvider } from "./Deploy.ts";
import { IngestionKey, IngestionKeyProvider } from "./IngestionKey.ts";
import { Project, ProjectProvider } from "./Project.ts";
import { Site, SiteProvider } from "./Site.ts";
import { SourceMaps, SourceMapsProvider } from "./SourceMaps.ts";

export class Providers extends Provider.ProviderCollection<Providers>()("Honeybadger") {}

export type ProviderRequirements = Layer.Services<ReturnType<typeof providers>>;

/**
 * Honeybadger providers and credentials. Wires up Project, IngestionKey,
 * Site, Dashboard, SourceMaps and Deploy resources, and registers the
 * Honeybadger AuthProvider so `alchemy profile edit` can configure it.
 * Source maps are read with the stack's `FileSystem` and `Path`.
 */
export const providers = () =>
  Layer.effect(
    Providers,
    Provider.collection([Dashboard, Deploy, IngestionKey, Project, Site, SourceMaps]),
  ).pipe(
    Layer.provide(
      Layer.mergeAll(
        DashboardProvider(),
        DeployProvider(),
        IngestionKeyProvider(),
        ProjectProvider(),
        SiteProvider(),
        SourceMapsProvider(),
      ),
    ),
    Layer.provideMerge(Layer.succeed(HoneybadgerRetry, DeployRetry.policy)),
    Layer.provideMerge(Credentials.fromAuthProvider()),
    Layer.provideMerge(FetchHttpClient.layer),
    Layer.provideMerge(HoneybadgerAuth),
    Layer.provideMerge(ProfileStoreLive),
    Layer.provideMerge(CredentialsStoreLive),
    Layer.orDie,
  );
