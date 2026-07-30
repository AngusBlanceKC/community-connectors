"""USGS Earthquake Catalog connector.

The connector reads the public USGS FDSN Event Web Service as GeoJSON and
flattens earthquake features into a Lakeflow table. Incremental reads use the
USGS ``updatedafter`` parameter so revised earthquake events are picked up as
CDC-style updates.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import requests
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.interface import LakeflowConnect

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
EARTHQUAKES_TABLE = "earthquakes"
EARTHQUAKES_SNAPSHOT_TABLE = "earthquakes_snapshot"
EARTHQUAKES_CDC_TABLE = "earthquakes_cdc"
EARTHQUAKES_CDC_WITH_DELETES_TABLE = "earthquakes_cdc_with_deletes"
EARTHQUAKES_APPEND_TABLE = "earthquakes_append"
TABLE_INGESTION_TYPES = {
    EARTHQUAKES_TABLE: "cdc",
    EARTHQUAKES_SNAPSHOT_TABLE: "snapshot",
    EARTHQUAKES_CDC_TABLE: "cdc",
    EARTHQUAKES_CDC_WITH_DELETES_TABLE: "cdc_with_deletes",
    EARTHQUAKES_APPEND_TABLE: "append",
}
DEFAULT_LOOKBACK_DAYS = 1
DEFAULT_MIN_MAGNITUDE = 0.0
DEFAULT_LIMIT = 20000
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}

EARTHQUAKES_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("mag", DoubleType(), nullable=True),
        StructField("place", StringType(), nullable=True),
        StructField("event_time", TimestampType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=False),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("depth", DoubleType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("url", StringType(), nullable=True),
        StructField("detail", StringType(), nullable=True),
        StructField("tsunami", LongType(), nullable=True),
        StructField("sig", LongType(), nullable=True),
        StructField("net", StringType(), nullable=True),
        StructField("code", StringType(), nullable=True),
        StructField("ids", StringType(), nullable=True),
        StructField("sources", StringType(), nullable=True),
        StructField("types", StringType(), nullable=True),
        StructField("mag_type", StringType(), nullable=True),
    ]
)


class UsgsEarthquakesLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for public USGS earthquake events."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._base_url = options.get("base_url", BASE_URL)
        self._session = requests.Session()

    def list_tables(self) -> list[str]:
        """Return supported tables, including demo variants by ingestion type."""
        return list(TABLE_INGESTION_TYPES)

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """Return the fixed USGS earthquake schema."""
        self._validate_table(table_name)
        return EARTHQUAKES_SCHEMA

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """Return metadata keyed by USGS event id and updated timestamp."""
        self._validate_table(table_name)
        return {
            "primary_keys": ["id"],
            "cursor_field": "updated_at",
            "ingestion_type": TABLE_INGESTION_TYPES[table_name],
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read events updated since the previous offset."""
        self._validate_table(table_name)
        params = self._build_query_params(start_offset, table_options)
        payload = self._request_geojson(params)
        records = [_feature_to_record(feature) for feature in payload.get("features", [])]
        records = [record for record in records if record["updated_at"] is not None]
        if start_offset and start_offset.get("updated_after"):
            start_cursor = start_offset["updated_after"]
            records = [
                record for record in records if record["updated_at"] > start_cursor
            ]
        records.sort(key=lambda record: record["updated_at"])

        if not records:
            return iter([]), start_offset or {}

        return iter(records), {"updated_after": records[-1]["updated_at"]}

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Return deleted records for the delete-flow demo table.

        USGS does not expose a deleted-earthquakes feed, so this table
        demonstrates the framework's ``cdc_with_deletes`` path while producing
        no delete tombstones.
        """
        del table_options
        self._validate_table(table_name)
        if table_name != EARTHQUAKES_CDC_WITH_DELETES_TABLE:
            raise ValueError(f"Table '{table_name}' does not support delete sync")
        return iter([]), start_offset or {}

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_INGESTION_TYPES:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {self.list_tables()}"
            )

    def _build_query_params(
        self, start_offset: dict | None, table_options: dict[str, str]
    ) -> dict[str, str]:
        params = {
            "format": "geojson",
            "limit": str(table_options.get("limit", DEFAULT_LIMIT)),
            "minmagnitude": str(
                table_options.get("min_magnitude", DEFAULT_MIN_MAGNITUDE)
            ),
        }

        updated_after = (start_offset or {}).get("updated_after")
        if updated_after:
            params["updatedafter"] = updated_after
        else:
            lookback_days = int(table_options.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
            params["starttime"] = _days_ago_iso(lookback_days)

        for option_name, param_name in {
            "event_type": "eventtype",
            "min_latitude": "minlatitude",
            "max_latitude": "maxlatitude",
            "min_longitude": "minlongitude",
            "max_longitude": "maxlongitude",
        }.items():
            if option_name in table_options:
                params[param_name] = str(table_options[option_name])

        return params

    def _request_geojson(self, params: dict[str, str]) -> dict[str, Any]:
        response = None
        for _attempt in range(3):
            response = self._session.get(self._base_url, params=params, timeout=30)
            if response.status_code not in RETRIABLE_STATUS_CODES:
                break
        response.raise_for_status()
        return response.json()


def _feature_to_record(feature: dict[str, Any]) -> dict[str, Any]:
    properties = feature.get("properties") or {}
    geometry = feature.get("geometry") or {}
    coordinates = geometry.get("coordinates") or []

    return {
        "id": feature.get("id"),
        "mag": properties.get("mag"),
        "place": properties.get("place"),
        "event_time": _epoch_ms_to_iso(properties.get("time")),
        "updated_at": _epoch_ms_to_iso(properties.get("updated")),
        "longitude": _coordinate(coordinates, 0),
        "latitude": _coordinate(coordinates, 1),
        "depth": _coordinate(coordinates, 2),
        "status": properties.get("status"),
        "type": properties.get("type"),
        "url": properties.get("url"),
        "detail": properties.get("detail"),
        "tsunami": properties.get("tsunami"),
        "sig": properties.get("sig"),
        "net": properties.get("net"),
        "code": properties.get("code"),
        "ids": properties.get("ids"),
        "sources": properties.get("sources"),
        "types": properties.get("types"),
        "mag_type": properties.get("magType"),
    }


def _coordinate(coordinates: list[Any], index: int) -> float | None:
    if len(coordinates) <= index or coordinates[index] is None:
        return None
    return float(coordinates[index])


def _epoch_ms_to_iso(value: Any) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc).isoformat()


def _days_ago_iso(days: int) -> str:
    now = datetime.now(timezone.utc)
    start = now.replace(microsecond=0) - timedelta(days=days)
    return start.isoformat().replace("+00:00", "Z")
