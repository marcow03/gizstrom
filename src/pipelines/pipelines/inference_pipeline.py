from datetime import datetime, timedelta
from typing import Any

import mlflow
import mlflow.sklearn
import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException
from pipelines.utils import BasePipeline, get_feast_feature_store

DUMMY_ENTITY = "walenstadt-dummy"
REGISTERED_MODEL_NAME = "power_generation_forecasting_model"
CHAMPION_ALIAS = "champion"
MODEL_RELOAD_INTERVAL_SECONDS = 6 * 60 * 60  # 6 hours


class InferencePipeline(BasePipeline):
    def __init__(self, config: dict):
        super().__init__(config)
        self._mlflow_client = mlflow.MlflowClient()
        mlflow.set_tracking_uri(self.config["mlflow_tracking_uri"])
        self._model = self._load_champion_model()
        self._last_model_load_time = datetime.now()
        self._fs = get_feast_feature_store()
        self._feature_refs = [
            "weather_forecast:temperature_2m_mean",
            "weather_forecast:temperature_2m_max",
            "weather_forecast:temperature_2m_min",
            "weather_forecast:daylight_duration",
            "weather_forecast:sunshine_duration",
            "weather_forecast:rain_sum",
            "weather_forecast:snowfall_sum",
            "weather_forecast:shortwave_radiation_sum",
        ]

    def run(self):
        self.log.info("Starting inference pipeline")

        app = FastAPI()

        @app.get("/predict")
        def predict():
            features = self._fetch_features()
            if features is None or features.empty:
                raise HTTPException(
                    status_code=503, detail="No features available for inference"
                )

            if (
                self._model is None
                or (datetime.now() - self._last_model_load_time).total_seconds()
                > MODEL_RELOAD_INTERVAL_SECONDS
            ):
                self.log.info("Reloading champion model")
                self._model = self._load_champion_model()
                self._last_model_load_time = datetime.now()

            if self._model is None:
                raise HTTPException(
                    status_code=503, detail="No model available for inference"
                )

            X_pred = features[
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
            ]
            prediction = self._model.predict(X_pred)

            return {
                "features": features.to_dict(orient="list"),
                "pred_power_generation_kwh": prediction.tolist(),
            }

        uvicorn.run(app, host="0.0.0.0", port=8001)

    def _load_champion_model(self) -> Any | None:
        try:
            model_version = self._mlflow_client.get_model_version_by_alias(
                name=REGISTERED_MODEL_NAME, alias=CHAMPION_ALIAS
            )
            model_uri = f"models:/{REGISTERED_MODEL_NAME}/{model_version.version}"
            model = mlflow.sklearn.load_model(model_uri)
            return model
        except Exception as e:
            self.log.error(f"Failed to load champion model: {e}")
            return None

    def _fetch_features(self) -> pd.DataFrame | None:
        try:
            timestamps = [
                pd.Timestamp(dt, unit="ms", tz="UTC").round("ms")
                for dt in pd.date_range(
                    start=datetime.now(),
                    end=datetime.now() + timedelta(days=7),
                    periods=7,
                )
            ]
            # Fetch features from the feature store
            features = (
                self._fs.get_historical_features(
                    features=self._feature_refs,
                    entity_df=pd.DataFrame(
                        {
                            "location": ["walenstadt-dummy"] * len(timestamps),
                            "event_timestamp": timestamps,
                        }
                    ),
                )
                .to_df()
                .dropna()
            )
            return features
        except Exception as e:
            self.log.error(f"Failed to fetch features: {e}")
            return None
