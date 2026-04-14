from datetime import datetime, timedelta

import pandas as pd
from feast import Entity, FeatureStore, FeatureView, Field, FileSource, ValueType
from feast.types import Float64
from pipelines.utils import BasePipeline


class FeaturePipeline(BasePipeline):
    def __init__(self, config: dict):
        super().__init__(config)

    def run(self):
        self._feast_apply()
        self._get_feature_values_test()

    def _get_feature_values_test(self):
        fs = FeatureStore(fs_yaml_file="config/feature_store.yaml")
        feature_refs = [
            "weather_historical:temperature_2m_mean",
            "weather_historical:temperature_2m_max",
            "weather_historical:temperature_2m_min",
            "weather_historical:daylight_duration",
            "weather_historical:sunshine_duration",
            "weather_historical:rain_sum",
            "weather_historical:snowfall_sum",
            "weather_historical:shortwave_radiation_sum",
            "power_generation:power_generation_kwh",
        ]
        timestamps = [
            pd.Timestamp(dt, unit="ms", tz="UTC").round("ms")
            for dt in pd.date_range(
                start=datetime.now() - timedelta(days=100),
                end=datetime.now(),
                periods=100,
            )
        ]
        feature_vector = fs.get_historical_features(
            features=feature_refs,
            entity_df=pd.DataFrame(
                {
                    "location": ["walenstadt-dummy"] * len(timestamps),
                    "event_timestamp": timestamps,
                }
            ),
        ).to_df()
        print(feature_vector)

    def _feast_apply(self):
        fs = FeatureStore(fs_yaml_file="config/feature_store.yaml")

        location = Entity(name="location", value_type=ValueType.STRING)

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

        power_generation_schema = [
            Field(name="power_generation_kwh", dtype=Float64),
        ]

        weather_historical_fv = FeatureView(
            name="weather_historical",
            entities=[location],
            schema=weather_schema,
            source=FileSource(
                path="s3://data/source/weather_data.parquet",
                timestamp_field="time",
            ),
        )

        weather_forecast_fv = FeatureView(
            name="weather_forecast",
            entities=[location],
            schema=weather_schema,
            source=FileSource(
                path="s3://data/source/forecast_data.parquet",
                timestamp_field="time",
            ),
        )

        power_generation_fv = FeatureView(
            name="power_generation",
            entities=[location],
            schema=power_generation_schema,
            source=FileSource(
                path="s3://data/source/power_generation.parquet",
                timestamp_field="time",
            ),
        )

        fs.apply(
            [location, weather_historical_fv, weather_forecast_fv, power_generation_fv]
        )
