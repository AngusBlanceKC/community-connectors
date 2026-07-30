# USGS Earthquakes Connector

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
