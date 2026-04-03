import requests
from datetime import datetime
import pandas as pd

from pipelines.utils import BasePipeline, S3Client, OpenMeteoClient

class DataCollectionPipeline(BasePipeline):
    def __init__(self, config: dict):
        super().__init__(config)
        self.s3 = S3Client(config)
        self.om = OpenMeteoClient()

    def run(self):
        self.log.info("Starting feature pipeline")

        # TODO: Handle case where there is no generation data
        generation_data = self.s3.load_csv(
            bucket=self.config["data_bucket"],
            object_key="uploads/power_generation/power_generation.csv"
        )
        # convert date ([dd.MM.yyyy]) to ISO format
        generation_data["date"] = pd.to_datetime(generation_data["date"], format='%d.%m.%Y').dt.tz_localize('Europe/Zurich')
        start_date = generation_data["date"].min()
        end_date = generation_data["date"].max()
        self.log.info(f"Power generation data from {start_date} to {end_date}")

        params = {
            "latitude": 47.1241,
            "longitude": 9.3119,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "daily": "temperature_2m_mean,temperature_2m_max,temperature_2m_min,daylight_duration,sunshine_duration,rain_sum,snowfall_sum,shortwave_radiation_sum",
            "timezone": "Europe/Berlin"
        }
        weather_data = self.om.fetch_historical_weather_data(params)

        if weather_data is not None:
            weather_data = pd.DataFrame(weather_data["daily"])
            self.log.info(f"Fetched {len(weather_data)} rows of weather data")
            self.s3.save_parquet(
                df=weather_data,
                bucket=self.config["data_bucket"],
                object_key="source/weather_data.parquet"
            )
            self.log.info("Saved weather data to S3")
        else:
            self.log.error("Failed to fetch weather data")

        params = {
            "latitude": 47.1241,
            "longitude": 9.3119,
            "daily": "temperature_2m_mean,temperature_2m_max,temperature_2m_min,daylight_duration,sunshine_duration,rain_sum,snowfall_sum,shortwave_radiation_sum",
            "timezone": "Europe/Berlin"
        }
        forecast_data = self.om.fetch_forecast_weather_data(params)
        if forecast_data is not None:
            forecast_data = pd.DataFrame(forecast_data["daily"])
            self.log.info(f"Fetched {len(forecast_data)} rows of forecast data")
            self.s3.save_parquet(
                df=forecast_data,
                bucket=self.config["data_bucket"],
                object_key="source/forecast_data.parquet"
            )
            self.log.info("Saved forecast data to S3")
        else:
            self.log.error("Failed to fetch forecast data")