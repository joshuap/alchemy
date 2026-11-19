import { DEFAULT_API_BASE_URL, ingestUrlFor } from "@distilled.cloud/honeybadger/Credentials";
import { AuthError } from "alchemy/Auth/AuthProvider";
import { getEnv, getEnvRedacted } from "alchemy/Auth/Env";
import {
  makeStoredAuthProvider,
  storedSecret,
  storedValueText,
  type StoredAuthConfig,
} from "alchemy/Auth/StoredAuthProvider";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";

export const HONEYBADGER_AUTH_PROVIDER_NAME = "Honeybadger";

export type HoneybadgerAuthConfig = StoredAuthConfig;

/**
 * Resolved Honeybadger credentials: an API v3 token (a user API token from
 * User settings, or an account API token from Account settings) and the API
 * root and ingest root for the account's region.
 */
export interface HoneybadgerResolvedCredentials {
  type: "apiToken";
  apiToken: Redacted.Redacted<string>;
  apiBaseUrl: string;
  /** Where deploys and source maps go; derived from `apiBaseUrl` unless set. */
  ingestUrl: string;
  source: { type: HoneybadgerAuthConfig["method"] | "env"; details?: string };
}

const readEnvironment = Effect.gen(function* () {
  const apiToken = yield* getEnvRedacted("HONEYBADGER_API_TOKEN");
  if (!apiToken) {
    return yield* AuthError.make({
      message: "Honeybadger CI credentials not found. Set HONEYBADGER_API_TOKEN.",
    });
  }
  // An unset CI variable often arrives as "", which must mean "not set".
  const apiBaseUrl = (yield* getEnv("HONEYBADGER_API_URL")) || DEFAULT_API_BASE_URL;
  const ingestUrl = (yield* getEnv("HONEYBADGER_INGEST_URL")) || ingestUrlFor(apiBaseUrl);
  return {
    type: "apiToken" as const,
    apiToken,
    apiBaseUrl,
    ingestUrl,
    source: { type: "env" as const },
  };
});

const honeybadgerAuth = makeStoredAuthProvider<HoneybadgerResolvedCredentials>({
  provider: HONEYBADGER_AUTH_PROVIDER_NAME,
  fields: [
    {
      name: "token",
      label: "Honeybadger API token (user or account, hbu_ or hba_)",
      secret: true,
    },
    {
      name: "apiBaseUrl",
      label: "Honeybadger API base URL (https://eu-app.honeybadger.io/v3 for EU accounts)",
      optional: true,
      placeholder: DEFAULT_API_BASE_URL,
    },
    {
      name: "ingestUrl",
      label:
        "Honeybadger ingest URL for deploys and source maps (derived from the API URL if empty)",
      optional: true,
    },
  ],
  toResolved: (values, source) => {
    const apiBaseUrl = storedValueText(values.apiBaseUrl) ?? DEFAULT_API_BASE_URL;
    return {
      type: "apiToken",
      apiToken: storedSecret(values.token) ?? Redacted.make(""),
      apiBaseUrl,
      ingestUrl: storedValueText(values.ingestUrl) ?? ingestUrlFor(apiBaseUrl),
      source: { type: source },
    };
  },
  readEnvironment,
  environment: [
    {
      name: "HONEYBADGER_API_TOKEN",
      required: true,
      secret: true,
      description:
        "API v3 token: a user API token from User settings, or an account API token from Account settings.",
    },
    {
      name: "HONEYBADGER_API_URL",
      required: false,
      description: "API base URL; https://eu-app.honeybadger.io/v3 for EU accounts.",
    },
    {
      name: "HONEYBADGER_INGEST_URL",
      required: false,
      description:
        "Where deploys and source maps go. Derived from HONEYBADGER_API_URL's region when unset (https://eu-api.honeybadger.io for EU accounts).",
    },
  ],
});

/**
 * Layer that registers the Honeybadger {@link AuthProvider} into the
 * {@link AuthProviders} registry when built. Include this in the Honeybadger
 * `providers()` layer so the alchemy CLI can discover it.
 */
export const HoneybadgerAuth = honeybadgerAuth.layer;

/** Schema of Honeybadger's inline static-token values. */
export const HoneybadgerStoredCredentials = honeybadgerAuth.storedSchema;
export type HoneybadgerStoredCredentials = typeof HoneybadgerStoredCredentials.Type;
