from datetime import datetime, timedelta

import pandas as pd
from feast import Entity, FeatureStore, FeatureView, Field, FileSource, ValueType
from feast.types import Float64
from pipelines.utils import BasePipeline, OpenMeteoClient, S3Client

# Feast requires an entity to work properly...
# For simplicity, we will use a dummy entity since we only have one location in our dataset.
DUMMY_ENTITY = "walenstadt-dummy"
TIMEZONE = "Europe/Zurich"
OPENMETEO_PARAMS = {
    "latitude": 47.1241,  # Walenstadt, Switzerland
    "longitude": 9.3119,
    "daily": ",".join(
        [
            "temperature_2m_mean",
            "temperature_2m_max",
            "temperature_2m_min",
            "daylight_duration",
            "sunshine_duration",
            "rain_sum",
            "snowfall_sum",
            "shortwave_radiation_sum",
        ]
    ),
    "timezone": "Europe/Berlin",  # Europe/Zurich is not supported by OpenMeteo
}


class FeaturePipeline(BasePipeline):
    def __init__(self, config: dict):
        super().__init__(config)
        self.s3 = S3Client(config)
        self.om = OpenMeteoClient()

    def run(self):
        self.log.info("Starting feature pipeline")

        self.log.info("Collecting and processing data")
        self._fetch_clean_and_save_power_generation_data()
        dates = self._try_get_date_range_for_historical_data()
        # Only fetch historical weather data for the date range of the power generation data
        if dates is not None:
            start_date, end_date = dates
            self.log.info(f"Power generation data from {start_date} to {end_date}")
            self._fetch_and_save_historical_weather_data(start_date, end_date)
        self._fetch_and_save_forecast_weather_data()

        self.log.info("Updating feature definitions")
        self._feast_apply()
        self._get_feature_values_test()

    def _try_get_date_range_for_historical_data(
        self,
    ) -> tuple[pd.Timestamp, pd.Timestamp] | None:
        generation_data = self.s3.load_parquet(
            bucket=self.config["data_bucket"],
            object_key="source/power_generation.parquet",
        )

        if generation_data is None:
            self.log.error("Power generation data not found in S3")
            return None

        start_date = generation_data["time"].min()
        end_date = generation_data["time"].max()

        return start_date, end_date

    def _fetch_clean_and_save_power_generation_data(self):
        power_gen_data = self.s3.load_csv(
            bucket=self.config["data_bucket"],
            object_key="uploads/power_generation/power_generation.csv",
        )

        if power_gen_data is None:
            self.log.error("Power generation data not found in S3")
            return

        # drop second row (contains units)
        power_gen_data = power_gen_data.drop(index=0).reset_index(drop=True)
        power_gen_data = power_gen_data.rename(
            columns={
                "Datum und Uhrzeit": "time",
                "Gesamtanlage": "power_generation_kwh",
            }
        )
        power_gen_data = power_gen_data[["time", "power_generation_kwh"]]
        power_gen_data["time"] = (
            pd.to_datetime(power_gen_data["time"], dayfirst=True)
            .dt.tz_localize(TIMEZONE)
            .dt.tz_convert("UTC")
        )
        power_gen_data["power_generation_kwh"] = power_gen_data[
            "power_generation_kwh"
        ].astype(float)
        power_gen_data["location"] = DUMMY_ENTITY
        self.log.info(f"Cleaned power generation data with {len(power_gen_data)} rows")

        self.s3.save(
            content=power_gen_data.to_parquet(index=False),
            bucket=self.config["data_bucket"],
            object_key="source/power_generation.parquet",
        )
        self.log.info("Saved power generation data to S3")

    def _fetch_and_save_historical_weather_data(
        self, start_date: pd.Timestamp, end_date: pd.Timestamp
    ):
        range_params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }
        params = {**OPENMETEO_PARAMS, **range_params}
        weather_data = self.om.fetch_historical_weather_data(params)

        if weather_data is None:
            self.log.error("Failed to fetch historical weather data")
            return

        weather_data = pd.DataFrame(weather_data["daily"])
        weather_data["id"] = (
            weather_data["time"].astype("datetime64[ns]").astype("int64") // 10**9
        )
        weather_data["time"] = (
            pd.to_datetime(weather_data["time"])
            .dt.tz_localize(TIMEZONE)
            .dt.tz_convert("UTC")
        )
        weather_data["location"] = DUMMY_ENTITY
        self.log.info(f"Fetched {len(weather_data)} rows of weather data")

        self.s3.save(
            content=weather_data.to_parquet(index=False),
            bucket=self.config["data_bucket"],
            object_key="source/weather_data.parquet",
        )
        self.log.info("Saved weather data to S3")

    def _fetch_and_save_forecast_weather_data(self):
        params = OPENMETEO_PARAMS
        forecast_data = self.om.fetch_forecast_weather_data(params)

        if forecast_data is None:
            self.log.error("Failed to fetch forecast weather data")
            return

        forecast_data = pd.DataFrame(forecast_data["daily"])
        forecast_data["id"] = (
            forecast_data["time"].astype("datetime64[ns]").astype("int64") // 10**9
        )
        forecast_data["time"] = (
            pd.to_datetime(forecast_data["time"])
            .dt.tz_localize(TIMEZONE)
            .dt.tz_convert("UTC")
        )
        forecast_data["location"] = DUMMY_ENTITY
        self.log.info(f"Fetched {len(forecast_data)} rows of forecast data")

        self.s3.save(
            content=forecast_data.to_parquet(index=False),
            bucket=self.config["data_bucket"],
            object_key="source/forecast_data.parquet",
        )
        self.log.info("Saved forecast data to S3")

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
