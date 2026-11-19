// @effect-diagnostics unstableApiUsage:off

import type { Credentials } from "@distilled.cloud/honeybadger/Credentials";
import * as Ingest from "@distilled.cloud/honeybadger/Ingest";
import { isResolved } from "alchemy/Diff";
import type { Diff } from "alchemy/Diff";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import { initialCwd } from "alchemy/Util/Node";
import { sha256, sha256Object } from "alchemy/Util/sha256";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import type * as HttpClient from "effect/http/HttpClient";
import * as Path from "effect/Path";
import * as Redacted from "effect/Redacted";
import { withIngestRetry } from "./internal.ts";
import type { Providers } from "./Providers.ts";

export interface SourceMapsProps {
  /** The project's API key, which Honeybadger takes source maps with. */
  apiKey: Redacted.Redacted;
  /** The revision the minified files belong to, as the client reports it. */
  revision: string;
  /**
   * Folder holding the minified files, for example a `Command.Build`'s
   * `outdir`. A relative path is read from the working directory the deploy
   * started in.
   */
  jsDirectory: string;
  /** Folder holding the `.js.map` files, laid out like `jsDirectory`. */
  mapDirectory: string;
  /**
   * The URL `jsDirectory` is served from. A file's URL is this followed by
   * its path inside `jsDirectory`.
   */
  baseUrl: string;
}

export interface SourceMapsAttributes {
  /** The revision last uploaded. */
  revision: string;
  /** Digest of every file's URL, minified code and source map. */
  digest: string;
  /** SHA-256 of the API key last uploaded with, so a key change uploads again. */
  apiKeyDigest: string;
  /** How many source maps were uploaded. */
  files: number;
}

/** One source map and the minified file it belongs to. */
export interface SourceMapFile {
  /** The URL the minified file is served from: `baseUrl` plus its path. */
  readonly minifiedUrl: string;
  /** Where the minified `.js` file is on disk. */
  readonly jsPath: string;
  /** Where its `.js.map` file is on disk. */
  readonly mapPath: string;
}

/** A source map whose minified file is not where it should be. */
export class MissingMinifiedFile extends Data.TaggedError("MissingMinifiedFile")<{
  /** Names the missing file and the source map that needs it. */
  readonly message: string;
  /** Where the minified file was expected. */
  readonly path: string;
}> {}

/** No `.js.map` files were found, so there is nothing to upload. */
export class NoSourceMaps extends Data.TaggedError("NoSourceMaps")<{
  /** Names the folder that was searched. */
  readonly message: string;
  /** The `mapDirectory` that held no source maps. */
  readonly path: string;
}> {}

const MAP_SUFFIX = ".map";

const encodePath = (relative: string) => relative.split("/").map(encodeURIComponent).join("/");

const withSlash = (url: string) => (url.endsWith("/") ? url : `${url}/`);

/**
 * Pairs every `x.js.map` under `mapDirectory` with `x.js` at the same
 * relative path under `jsDirectory`. Fails naming the first minified file
 * that is missing, or with `NoSourceMaps` when `mapDirectory` holds none.
 */
export const fromDirectories = Effect.fn("SourceMaps.fromDirectories")(
  function* fromDirectories(input: {
    readonly jsDirectory: string;
    readonly mapDirectory: string;
    readonly baseUrl: string;
  }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const jsDirectory = path.resolve(initialCwd, input.jsDirectory);
    const mapDirectory = path.resolve(initialCwd, input.mapDirectory);
    const names = yield* fs.readDirectory(mapDirectory, { recursive: true });
    const maps = names
      .map((name) => name.split(path.sep).join("/"))
      .filter((name) => name.endsWith(`.js${MAP_SUFFIX}`))
      .toSorted();
    if (maps.length === 0) {
      return yield* new NoSourceMaps({
        message: `No .js.map files under ${mapDirectory}`,
        path: mapDirectory,
      });
    }
    const files: SourceMapFile[] = [];
    for (const map of maps) {
      const relative = map.slice(0, -MAP_SUFFIX.length);
      const jsPath = path.join(jsDirectory, relative);
      if (!(yield* fs.exists(jsPath))) {
        return yield* new MissingMinifiedFile({
          message: `No minified file at ${jsPath} for ${path.join(mapDirectory, map)}`,
          path: jsPath,
        });
      }
      files.push({
        jsPath,
        mapPath: path.join(mapDirectory, map),
        minifiedUrl: `${withSlash(input.baseUrl)}${encodePath(relative)}`,
      });
    }
    return files;
  },
);

export type SourceMaps = Resource<
  "Honeybadger.SourceMaps",
  SourceMapsProps,
  SourceMapsAttributes,
  never,
  Providers
>;

/**
 * Uploads a build's source maps to Honeybadger so its backtraces point at
 * the original source. Every `.js.map` under `mapDirectory` is sent with
 * the `.js` beside it under `jsDirectory`.
 *
 * It uploads again when the revision, the API key, or any file's URL or
 * content changes, and makes no request otherwise. Every plan reads and
 * hashes the files to tell. Uploads go to the ingest host for the
 * account's region (`https://eu-api.honeybadger.io` for an EU account),
 * four at a time. Each attempt gives up after 60 seconds, and throttling or
 * server errors are retried. Honeybadger takes at
 * most 80 MB per file. Honeybadger keeps the maps it was sent, so
 * destroying the resource deletes nothing. Order it before the
 * Worker goes live by making the Worker depend on it.
 *
 * ### Uploading source maps
 * **Example:** Upload a Vite build's maps before the Worker goes live
 * ```typescript
 * const project = yield* Honeybadger.Project("Errors", { name: "momo" });
 * const key = yield* Honeybadger.IngestionKey("Web", {
 *   projectId: project.projectId,
 * });
 * const revision = yield* Config.String("GITHUB_SHA").pipe(Config.withDefault("dev"));
 * const maps = yield* Honeybadger.SourceMaps("Web", {
 *   apiKey: key.key,
 *   revision,
 *   jsDirectory: "./dist/assets",
 *   mapDirectory: "./dist/assets",
 *   baseUrl: "https://momo.example.com/assets/",
 * });
 * yield* Cloudflare.Worker("Web", {
 *   main: "./src/worker.ts",
 *   env: { HONEYBADGER_API_KEY: key.key, HONEYBADGER_REVISION: maps.revision },
 * });
 * ```
 *
 * @resource
 * @product Source Maps
 */
export const SourceMaps = Object.assign(Resource<SourceMaps>("Honeybadger.SourceMaps"), {
  fromDirectories,
});

interface Loaded {
  readonly file: SourceMapFile;
  readonly minifiedFile: Uint8Array;
  readonly sourceMap: Uint8Array;
}

const load = (props: SourceMapsProps) =>
  Effect.gen(function* loadFiles() {
    const fs = yield* FileSystem.FileSystem;
    const files = yield* fromDirectories(props);
    return yield* Effect.forEach(files, (file) =>
      Effect.all({
        file: Effect.succeed(file),
        minifiedFile: fs.readFile(file.jsPath),
        sourceMap: fs.readFile(file.mapPath),
      }),
    );
  });

const digestOf = (loaded: readonly Loaded[]) =>
  Effect.forEach(loaded, (entry) =>
    Effect.all({
      js: sha256(entry.minifiedFile),
      map: sha256(entry.sourceMap),
      url: Effect.succeed(entry.file.minifiedUrl),
    }),
  ).pipe(Effect.flatMap((entries) => sha256Object({ entries })));

const unchanged = (
  output: SourceMapsAttributes | undefined,
  revision: string,
  digest: string,
  apiKeyDigest: string,
) =>
  output !== undefined &&
  output.revision === revision &&
  output.digest === digest &&
  output.apiKeyDigest === apiKeyDigest;

export const SourceMapsProvider = () =>
  Provider.effect(
    SourceMaps,
    Effect.gen(function* makeSourceMapsProvider() {
      const services = yield* Effect.context<
        FileSystem.FileSystem | Path.Path | HttpClient.HttpClient | Credentials
      >();

      const current = (props: SourceMapsProps) =>
        Effect.all({
          apiKeyDigest: props.apiKey.pipe(Redacted.value, sha256),
          loaded: load(props),
        }).pipe(
          Effect.flatMap(({ apiKeyDigest, loaded }) =>
            Effect.map(digestOf(loaded), (digest) => ({ apiKeyDigest, digest, loaded })),
          ),
          Effect.provideContext(services),
        );

      return {
        delete: () => Effect.void,
        diff: ({ news, output }) =>
          isResolved(news)
            ? current(news).pipe(
                Effect.map(({ apiKeyDigest, digest }): Diff =>
                  unchanged(output, news.revision, digest, apiKeyDigest)
                    ? { action: "noop" }
                    : { action: "update" },
                ),
                // A file that can't be read plans an update; reconcile then
                // reads again and fails with the real error.
                Effect.orElseSucceed((): Diff => ({ action: "update" })),
              )
            : Effect.undefined,
        list: () => Effect.succeed([]),
        reconcile: Effect.fn(function* reconcile({ news, output }) {
          const { apiKeyDigest, digest, loaded } = yield* current(news);
          if (unchanged(output, news.revision, digest, apiKeyDigest) && output !== undefined) {
            return output;
          }
          yield* Effect.forEach(
            loaded,
            (entry) =>
              Ingest.uploadSourceMap({
                api_key: news.apiKey,
                minified_file: entry.minifiedFile,
                minified_url: entry.file.minifiedUrl,
                revision: news.revision,
                source_map: entry.sourceMap,
              }).pipe(Effect.timeout("60 seconds"), withIngestRetry),
            { concurrency: 4, discard: true },
          ).pipe(Effect.provideContext(services));
          return { apiKeyDigest, digest, files: loaded.length, revision: news.revision };
        }),
      };
    }),
  );
