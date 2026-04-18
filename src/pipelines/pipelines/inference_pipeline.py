from dataclasses import dataclass
from datetime import datetime
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


@dataclass
class InferenceRequest:
    timestamps: list[datetime]


class InferencePipeline(BasePipeline):
    def __init__(self, config: dict):
        super().__init__(config)
        self._mlflow_client = mlflow.MlflowClient()
        mlflow.set_tracking_uri(self.config["mlflow_tracking_uri"])

    def run(self):
        model = self._load_champion_model()
        fs = get_feast_feature_store()
        feature_refs = [
            "weather_forecast:temperature_2m_mean",
            "weather_forecast:temperature_2m_max",
            "weather_forecast:temperature_2m_min",
            "weather_forecast:daylight_duration",
            "weather_forecast:sunshine_duration",
            "weather_forecast:rain_sum",
            "weather_forecast:snowfall_sum",
            "weather_forecast:shortwave_radiation_sum",
        ]
        app = FastAPI()

        @app.post("/predict")
        def predict(request: InferenceRequest):
            timestamps = request.timestamps
            if not timestamps:
                raise HTTPException(
                    status_code=400,
                    detail="No valid dates provided for inference. Please provide a list of ISO formatted date strings with the 'dates' key in the request body.",
                )
            for ts in timestamps:
                if ts < pd.Timestamp.now(tz="UTC") or ts > pd.Timestamp.now(
                    tz="UTC"
                ) + pd.Timedelta(days=7):
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid date {ts}. Only future dates are allowed for inference.",
                    )

            features = (
                fs.get_historical_features(
                    features=feature_refs,
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

            if model is None:
                raise HTTPException(
                    status_code=503, detail="No model available for inference"
                )

            prediction = model.predict(
                features[
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
            )

            return {
                "features": features.to_dict(),
                "pred_power_generation_kwh": prediction.tolist(),
            }

        uvicorn.run(app, host="0.0.0.0", port=8000)

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
