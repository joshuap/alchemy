import { ConfigError } from "@distilled.cloud/core/errors";
import { Credentials } from "@distilled.cloud/honeybadger/Credentials";
import { lazyCredentials } from "alchemy/Auth/Resolve";
import * as Layer from "effect/Layer";
import {
  HONEYBADGER_AUTH_PROVIDER_NAME,
  type HoneybadgerResolvedCredentials,
} from "./AuthProvider.ts";

export {
  Credentials,
  CredentialsFromEnv,
  DEFAULT_API_BASE_URL,
  DEFAULT_INGEST_URL,
  EU_API_BASE_URL,
  EU_INGEST_URL,
  ingestUrlFor,
} from "@distilled.cloud/honeybadger/Credentials";

/**
 * Build a `Credentials` layer that resolves Honeybadger credentials via the
 * Alchemy AuthProvider: `HONEYBADGER_API_TOKEN` (plus `HONEYBADGER_API_URL`
 * and `HONEYBADGER_INGEST_URL`) when set, otherwise the selected profile.
 */
export const fromAuthProvider = () =>
  Layer.effect(
    Credentials,
    lazyCredentials<HoneybadgerResolvedCredentials, ConfigError>()(
      HONEYBADGER_AUTH_PROVIDER_NAME,
      (creds) => ({
        token: creds.apiToken,
        apiBaseUrl: creds.apiBaseUrl,
        ingestUrl: creds.ingestUrl,
      }),
    ),
  );
