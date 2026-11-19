# @alchemy.run/honeybadger

[Honeybadger](https://www.honeybadger.io) for [Alchemy](https://alchemy.run). Declare a project, its ingestion key, uptime checks and dashboards next to the app that reports to it, upload source maps before a release goes live, and record the deploy.

```sh
npm install alchemy @alchemy.run/honeybadger effect
```

A project with a key for the Worker, an uptime check on its health endpoint, and a dashboard:

```typescript
// alchemy.run.ts
import * as Alchemy from "alchemy";
import * as Honeybadger from "@alchemy.run/honeybadger";
import * as Effect from "effect/Effect";

export default Alchemy.Stack(
  "Monitoring",
  {
    providers: Honeybadger.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const project = yield* Honeybadger.Project("App", { name: "my-app", language: "js" });
    const key = yield* Honeybadger.IngestionKey("Worker", {
      projectId: project.projectId,
      label: "worker",
    });

    yield* Honeybadger.Site("Health", {
      projectId: project.projectId,
      name: "my-app health",
      url: "https://my-app.example.com/api/health",
      matchType: "success",
      frequency: 5,
    });

    yield* Honeybadger.Dashboard("Overview", {
      projectId: project.projectId,
      title: "My app",
      defaultTs: "P1D",
      widgets: [
        {
          id: "requests",
          type: "insights_vis",
          config: { query: "stats count() by bin(1h)", streams: ["default"] },
        },
      ],
    });

    return { apiKey: key.key };
  }),
);
```

Pass `key.key` to the app as its Honeybadger API key. For a release, add `Honeybadger.SourceMaps` (make the Worker depend on it, so the maps are in before the code is live) and `Honeybadger.Deploy` after the Worker.

## Credentials

The provider uses a Honeybadger API token: a user API token from User settings, or an account API token from Account settings for CI. Give it the scopes for the resources your stack manages. Store it in a profile with `alchemy profile edit --add Honeybadger`, or set `HONEYBADGER_API_TOKEN`. EU accounts set the API URL to `https://eu-app.honeybadger.io/v3`, in the profile or as `HONEYBADGER_API_URL` next to the token; deploys and source maps then go to `https://eu-api.honeybadger.io`. `HONEYBADGER_INGEST_URL` (or the profile's ingest URL) sends them somewhere else.

Projects are kept in Honeybadger when they are removed from a stack, because their error history can't be recreated. Pipe a project through `RemovalPolicy.destroy()` to delete it with the stack.

## Docs

- [Honeybadger overview](https://alchemy.run/honeybadger)
- [Setup](https://alchemy.run/honeybadger/setup)
- [Accounts, tokens and regions](https://alchemy.run/honeybadger/accounts-tokens-and-regions)
- [API reference](https://alchemy.run/providers)
