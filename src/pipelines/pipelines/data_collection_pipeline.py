import pandas as pd
from pipelines.utils import BasePipeline, OpenMeteoClient, S3Client


class DataCollectionPipeline(BasePipeline):
    def __init__(self, config: dict):
        super().__init__(config)
        self.s3 = S3Client(config)
        self.om = OpenMeteoClient()

    def run(self):
        self.log.info("Starting feature pipeline")

        generation_data = self.s3.load_parquet(
            bucket=self.config["data_bucket"],
            object_key="uploads/power_generation/power_generation.parquet",
        )
        if generation_data is not None:
            start_date, end_date = self._get_date_range_for_historical_data(
                generation_data
            )
            self.log.info(f"Power generation data from {start_date} to {end_date}")

            self._fetch_and_save_historical_weather_data(start_date, end_date)

        self._fetch_and_save_forecast_weather_data()

    def _get_date_range_for_historical_data(
        self, generation_data: pd.DataFrame
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        start_date = generation_data["date"].min()
        end_date = generation_data["date"].max()

        return start_date, end_date

    def _fetch_and_save_historical_weather_data(
        self, start_date: pd.Timestamp, end_date: pd.Timestamp
    ):
        params = {
            "latitude": 47.1241,
            "longitude": 9.3119,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "daily": "temperature_2m_mean,temperature_2m_max,temperature_2m_min,daylight_duration,sunshine_duration,rain_sum,snowfall_sum,shortwave_radiation_sum",
            "timezone": "Europe/Berlin",
        }
        weather_data = self.om.fetch_historical_weather_data(params)

        if weather_data is not None:
            weather_data = pd.DataFrame(weather_data["daily"])
            weather_data["id"] = (
                weather_data["time"].astype("datetime64[ns]").astype("int64") // 10**9
            )
            weather_data["time"] = pd.to_datetime(weather_data["time"]).dt.tz_localize(
                "Europe/Zurich"
            )
            self.log.info(f"Fetched {len(weather_data)} rows of weather data")
            self.s3.save(
                content=weather_data.to_parquet(index=False),
                bucket=self.config["data_bucket"],
                object_key="source/weather_data.parquet",
            )
            self.log.info("Saved weather data to S3")
        else:
            self.log.error("Failed to fetch weather data")

    def _fetch_and_save_forecast_weather_data(self):
        params = {
            "latitude": 47.1241,
            "longitude": 9.3119,
            "daily": "temperature_2m_mean,temperature_2m_max,temperature_2m_min,daylight_duration,sunshine_duration,rain_sum,snowfall_sum,shortwave_radiation_sum",
            "timezone": "Europe/Berlin",
        }
        forecast_data = self.om.fetch_forecast_weather_data(params)

        if forecast_data is not None:
            forecast_data = pd.DataFrame(forecast_data["daily"])
            forecast_data["id"] = (
                forecast_data["time"].astype("datetime64[ns]").astype("int64") // 10**9
            )
            forecast_data["time"] = pd.to_datetime(
                forecast_data["time"]
            ).dt.tz_localize("Europe/Zurich")
            self.log.info(f"Fetched {len(forecast_data)} rows of forecast data")
            self.s3.save(
                content=forecast_data.to_parquet(index=False),
                bucket=self.config["data_bucket"],
                object_key="source/forecast_data.parquet",
            )
            self.log.info("Saved forecast data to S3")
        else:
            self.log.error("Failed to fetch forecast data")
