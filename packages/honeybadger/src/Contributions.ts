import * as Contributions from "alchemy/Contributions";

export default Contributions.make({
  name: "honeybadger",
  auth: Contributions.lazy(
    () => import("./AuthProvider.ts"),
    (module) => module.HoneybadgerAuth,
  ),
});
