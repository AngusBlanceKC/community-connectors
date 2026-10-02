"""Lakeflow pipeline entry point for the USGS Earthquakes demo bundle."""

from databricks.labs.community_connector import register
from databricks.labs.community_connector.pipeline import ingest


spark.conf.set(
    "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true"
)

connection_name = spark.conf.get(
    "usgs.connection_name", "usgs_earthquakes_demo"
)
pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {
            "table": {
                "source_table": "earthquakes",
                "table_configuration": {"lookback_days": "7"},
            }
        }
    ],
}

register(spark, "usgs_earthquakes")
ingest(spark, pipeline_spec)
