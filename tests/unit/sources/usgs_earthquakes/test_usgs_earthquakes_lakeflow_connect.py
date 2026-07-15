from unittest.mock import Mock

import pytest
from pyspark.sql.types import DoubleType, StringType, TimestampType

from databricks.labs.community_connector.sources.usgs_earthquakes.usgs_earthquakes import (
    EARTHQUAKES_TABLE,
    UsgsEarthquakesLakeflowConnect,
)


def _feature(event_id, updated_ms, time_ms=1784112000000):
    return {
        "type": "Feature",
        "id": event_id,
        "properties": {
            "mag": 2.5,
            "place": "10 km W of Testville",
            "time": time_ms,
            "updated": updated_ms,
            "status": "reviewed",
            "type": "earthquake",
            "url": f"https://earthquake.usgs.gov/earthquakes/eventpage/{event_id}",
            "detail": f"https://earthquake.usgs.gov/fdsnws/event/1/query?eventid={event_id}",
            "tsunami": 0,
            "sig": 96,
            "net": "us",
            "code": event_id,
            "ids": f",{event_id},",
            "sources": ",us,",
            "types": ",origin,phase-data,",
            "magType": "ml",
        },
        "geometry": {"type": "Point", "coordinates": [-122.1, 37.2, 8.3]},
    }


def _response(features):
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"type": "FeatureCollection", "features": features}
    response.raise_for_status.return_value = None
    return response


def test_reads_earthquakes_and_returns_updated_offset():
    connector = UsgsEarthquakesLakeflowConnect({})
    connector._session.get = Mock(  # pylint: disable=protected-access
        return_value=_response(
            [
                _feature("us2", 1784112900000),
                _feature("us1", 1784112300000),
            ]
        )
    )

    records, offset = connector.read_table(EARTHQUAKES_TABLE, None, {"lookback_days": "2"})

    rows = list(records)
    assert [row["id"] for row in rows] == ["us1", "us2"]
    assert rows[0]["longitude"] == -122.1
    assert rows[0]["latitude"] == 37.2
    assert rows[0]["depth"] == 8.3
    assert offset == {"updated_after": "2026-07-15T10:55:00+00:00"}

    params = connector._session.get.call_args.kwargs["params"]  # pylint: disable=protected-access
    assert params["format"] == "geojson"
    assert "starttime" in params


def test_resume_uses_usgs_updatedafter_parameter():
    connector = UsgsEarthquakesLakeflowConnect({})
    connector._session.get = Mock(return_value=_response([]))  # pylint: disable=protected-access

    start_offset = {"updated_after": "2026-07-15T08:15:00+00:00"}
    records, offset = connector.read_table(
        EARTHQUAKES_TABLE,
        start_offset,
        {"min_magnitude": "4.5", "event_type": "earthquake"},
    )

    assert list(records) == []
    assert offset == start_offset
    params = connector._session.get.call_args.kwargs["params"]  # pylint: disable=protected-access
    assert params["updatedafter"] == "2026-07-15T08:15:00+00:00"
    assert params["minmagnitude"] == "4.5"
    assert params["eventtype"] == "earthquake"
    assert "starttime" not in params


def test_schema_and_metadata():
    connector = UsgsEarthquakesLakeflowConnect({})

    schema = connector.get_table_schema(EARTHQUAKES_TABLE, {})
    metadata = connector.read_table_metadata(EARTHQUAKES_TABLE, {})

    assert schema["id"].dataType == StringType()
    assert schema["mag"].dataType == DoubleType()
    assert schema["updated_at"].dataType == TimestampType()
    assert metadata == {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    }


def test_rejects_unknown_table():
    connector = UsgsEarthquakesLakeflowConnect({})

    with pytest.raises(ValueError, match="not supported"):
        connector.read_table("other", None, {})
