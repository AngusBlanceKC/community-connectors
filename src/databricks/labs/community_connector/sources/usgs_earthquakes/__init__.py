"""USGS earthquakes connector."""

from databricks.labs.community_connector.sources.usgs_earthquakes.usgs_earthquakes import (
    UsgsEarthquakesLakeflowConnect,
)

__all__ = ["UsgsEarthquakesLakeflowConnect"]
