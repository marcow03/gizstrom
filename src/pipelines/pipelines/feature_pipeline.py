from feast import FeatureStore, FeatureView, Field, FileSource
from feast.types import Float64
from pipelines.utils import BasePipeline


class FeaturePipeline(BasePipeline):
    def __init__(self, config: dict):
        super().__init__(config)

    def run(self):
        self._feast_apply()

    def _feast_apply(self):
        # repo_config = RepoConfig(
        #     project="gizstrom",
        #     registry=self.config["feast_registry_destination"],
        #     provider="local",
        #     online_store={
        #         "type": "redis",
        #         "connection_string": f"{self.config['feast_redis_host']}:{self.config['feast_redis_port']},password={self.config['feast_redis_password']}",
        #     },
        #     offline_store={
        #         "type": "file",
        #     },
        #     entity_key_serialization_version=3,
        # )
        fs = FeatureStore(fs_yaml_file="config/feature_store.yaml")

        weather_schema = [
            Field(name="temperature_2m_mean", dtype=Float64),
            Field(name="temperature_2m_max", dtype=Float64),
            Field(name="temperature_2m_min", dtype=Float64),
            Field(name="daylight_duration", dtype=Float64),
            Field(name="sunshine_duration", dtype=Float64),
            Field(name="rain_sum", dtype=Float64),
            Field(name="snowfall_sum", dtype=Float64),
            Field(name="shortwave_radiation_sum", dtype=Float64),
        ]

        weather_historical_fv = FeatureView(
            name="weather_historical",
            entities=[],
            schema=weather_schema,
            source=FileSource(
                path="s3://data/source/weather_data.parquet",
                timestamp_field="time",
            ),
        )

        weather_forecast_fv = FeatureView(
            name="weather_forecast",
            entities=[],
            schema=weather_schema,
            source=FileSource(
                path="s3://data/source/forecast_data.parquet",
                timestamp_field="time",
            ),
        )

        fs.apply([weather_historical_fv, weather_forecast_fv])
