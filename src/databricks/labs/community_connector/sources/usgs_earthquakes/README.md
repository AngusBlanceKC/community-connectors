# USGS Earthquakes Connector

## Quickstart: deploy the demo to Databricks

This bundle deploys a Lakeflow Spark Declarative Pipeline and a job that
refreshes the USGS earthquake table. After the first successful run, open the
pipeline in the workspace and find the streaming table in Unity Catalog.

### 1. Sign in and enable Community Connectors

The workspace preview **Lakeflow Connect Community Connectors** must be
enabled by a workspace admin. Sign in with the Databricks CLI, then check the
setting:

```bash
databricks auth login --host https://adb-3990188733873176.16.azuredatabricks.net --profile DEFAULT
databricks api get /api/2.1/settings/generic_lfc --profile DEFAULT
```

If `effective_boolean_val.value` is `false`, enable it:

```bash
databricks api patch /api/2.1/settings/generic_lfc --profile DEFAULT \
  --json '{"name":"generic_lfc","boolean_val":{"value":true}}'
```

### 2. Create the Unity Catalog connection

The USGS API is public, so no credentials are needed. Create a connection
named `usgs_earthquakes_demo`:

```bash
databricks api post /api/2.1/unity-catalog/connections --profile DEFAULT \
  --json '{"name":"usgs_earthquakes_demo","connection_type":"GENERIC_LAKEFLOW_CONNECT","options":{"sourceName":"usgs_earthquakes"}}'
```

### 3. Validate, deploy, and run the bundle

From the repository root:

```bash
databricks bundle validate -t dev --profile DEFAULT
databricks bundle deploy -t dev --profile DEFAULT
databricks bundle run usgs_earthquakes_refresh -t dev --profile DEFAULT
```

The bundle is configured for the demo workspace at
`https://adb-3990188733873176.16.azuredatabricks.net`. The default catalog is
`raw_angus`. Development mode prefixes the schema with your Databricks user;
the demo deployment created
`raw_angus.dev_angus_blance_usgs_earthquakes_demo.earthquakes`.

### 4. View the pipeline and table

Open the [USGS Earthquakes pipeline](https://adb-3990188733873176.16.azuredatabricks.net/pipelines/c86145c7-8320-4a54-9197-fcdb3c30edf0)
in **Workflows & Pipelines**. The output is the streaming table
`earthquakes` in your deployment schema. You can also open it from **Catalog**
under `raw_angus`.

To continue into the classification demo, open [AutoML classification](https://adb-3990188733873176.16.azuredatabricks.net/ml/automl/create?problemType=classification&o=3990188733873176)
and select the `earthquakes` table.

The connector uses a seven-day first-run lookback for this demo. See the
connector overview below for table variants and configurable source options.

This connector ingests public earthquake event data from the USGS FDSN Event
Web Service.

The connector exposes one production-style table and four demo variants:

- `earthquakes`: CDC, the default/recommended mode
- `earthquakes_snapshot`: snapshot ingestion
- `earthquakes_cdc`: CDC ingestion
- `earthquakes_cdc_with_deletes`: CDC with a delete flow; USGS does not
  publish deleted-event tombstones, so the delete flow returns no rows
- `earthquakes_append`: append-only ingestion

It reads the USGS GeoJSON response, flattens each Feature into a row, and uses
the event `updated` timestamp as the incremental cursor. On the first run, it
queries recent events using `starttime`. On later runs, it queries events using
USGS `updatedafter` so newly published and revised events are ingested.

Useful options:

- `lookback_days`: first-run window, default `1`
- `limit`: maximum events per request, default `20000`
- `min_magnitude`: minimum earthquake magnitude, default `0.0`
- `event_type`: USGS event type, for example `earthquake`
- `min_latitude`, `max_latitude`, `min_longitude`, `max_longitude`: bounding box

USGS documentation:

- https://earthquake.usgs.gov/fdsnws/event/1/
- https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php
