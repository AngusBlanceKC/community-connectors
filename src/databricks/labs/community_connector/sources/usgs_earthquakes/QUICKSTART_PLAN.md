# USGS Earthquakes Quickstart Plan

## Goal

Make this connector easy to try from a clean checkout: build and deploy the
connector to the Databricks workspace, create or select its Unity Catalog
connection, start ingestion, and confirm the earthquake table in the UI.

Temporary workspace destination for this demo:

`https://adb-3990188733873176.16.azuredatabricks.net/ml/automl/create?problemType=classification&o=3990188733873176`

Keep this destination hard-coded in the quickstart for now, as requested.

## Small steps and checkpoints

### 1. Confirm the deployment path

- Inspect the connector package, repo deployment tooling, and any existing DAB
  examples. This checkout does not currently contain a root `databricks.yml`,
  so determine the smallest correct bundle setup rather than inventing commands.
- The repo does include `tools/community_connector`, whose `create_pipeline`
  command creates a Lakeflow pipeline, can upload local source files or wheel
  packages, and prints a workspace pipeline URL. Check whether this should be
  invoked by a small DAB wrapper or whether DAB can define the required
  connector/pipeline resources directly.
- The current Databricks deployment guide describes two parts for a custom
  connector: a merged single-file source artifact and dependency wheels in a
  Unity Catalog volume. It describes adding the custom connector tile via the
  UI or the `community-connector publish` CLI, then creating the ingestion
  pipeline in the UI. Verify how to combine this with the requested DAB flow;
  do not claim `bundle deploy` alone publishes the source tile unless verified.
- This checkout's `tools/community_connector` currently has no `publish`
  command, so resolve whether to add that functionality or document its UI
  equivalent before writing commands into the quickstart.
- The installed Databricks CLI's `pipelines init` template provides a DAB
  pipeline plus a job that refreshes it. Use this as the bundle structure. The
  connection is a separate prerequisite (the bundle resource list does not
  expose a connection resource). The repo CLI currently submits the legacy
  `COMMUNITY` type, which this workspace rejected; the workspace accepted
  `GENERIC_LAKEFLOW_CONNECT` through the Databricks CLI API.
- Confirm the CLI version and selected workspace profile.
- **Checkpoint:** `databricks bundle validate -t dev --profile DEFAULT`
  succeeds. `databricks current-user me --profile DEFAULT` confirms the
  authenticated workspace identity.

### 2. Add the minimal deployable bundle

- Add only the bundle files and resource definitions needed to build/deploy this
  source connector and expose it to Lakeflow.
- Keep names, catalog/schema, and other user-specific values configurable where
  the framework permits; keep the demo workspace destination hard-coded.
- **Checkpoint:** bundle validation and deployment succeeded. The workspace
  contains job `373617954619619`, pipeline
  `c86145c7-8320-4a54-9197-fcdb3c30edf0`, and the DAB-created demo schema.

### 3. Make the connector selectable in the UI

- Document the Unity Catalog connection step. USGS is public, so no API key is
  needed; describe `base_url` only as an optional override.
- Direct readers to the requested Databricks page and explain the UI path for
  selecting the deployed community connector and creating its ingestion
  pipeline.
- **Checkpoint:** connection `usgs_earthquakes_demo` is active with type
  `GENERIC_LAKEFLOW_CONNECT`; pipeline and job are visible in the workspace.
  The refresh job completed successfully.

### 4. Confirm the output table

- Run the pipeline against USGS and confirm the `earthquakes` streaming table
  appears in the chosen Unity Catalog destination.
- **Checkpoint:** `databricks tables get
  raw_angus.dev_angus_blance_usgs_earthquakes_demo.earthquakes --profile
  DEFAULT` reports `STREAMING_TABLE` with 20 columns; the refresh job result is
  `SUCCESS`.

### 5. Write the README quickstart

- Put a concise quickstart at the top of `README.md`: prerequisites, exact
  build/deploy commands, workspace link, UI setup, expected table, and links to
  detailed options and USGS API documentation.
- Retain the existing connector overview and options below the quickstart.
- **Checkpoint:** top of the source README now has sign-in, preview enablement,
  connection setup, bundle validate/deploy/run commands, pipeline/table links,
  and the requested AutoML URL. Commands and resource names match the live
  workspace deployment.

## Progress

- [x] Inspected the USGS connector and confirmed there is no root DAB bundle in
  this checkout yet.
- [x] Inspected the repo's `community-connector` deployment tooling.
- [x] Confirmed the `DEFAULT` profile and inventoried workspace resources.
- [x] Confirmed there is no USGS connection, pipeline, or job yet; `raw_angus`
  is available and `usgs_earthquakes_demo` is an unused schema name.
- [x] Completed checkpoints 2–5 in order; the table and successful job are
  present in the demo workspace.

## Current checkpoint

The installed Databricks CLI reports version `1.7.0`. Workspace preview
`generic_lfc` was enabled because connection creation initially failed with a
feature-gate error. The bundle was deployed and its refresh job ran
successfully. The live table is
`raw_angus.dev_angus_blance_usgs_earthquakes_demo.earthquakes` (streaming table,
20 columns); its pipeline is `c86145c7-8320-4a54-9197-fcdb3c30edf0` and its
refresh job is `373617954619619`. The repo CLI's `create_connection` still
submits `COMMUNITY`, which the workspace rejects; the README uses the current
accepted `GENERIC_LAKEFLOW_CONNECT` API payload. The unrelated existing
`stungevents-connector-poc` pipeline was left untouched.

Official deployment references reviewed:

- [Build a custom connector for Lakeflow Connect](https://learn.microsoft.com/en-us/azure/databricks/ingestion/custom-connectors)
- [Declarative Automation Bundles resources](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/resources)
