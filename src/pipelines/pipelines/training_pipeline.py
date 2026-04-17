from datetime import datetime, timedelta

import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow import MlflowClient
from pipelines.utils import BasePipeline, get_feast_feature_store
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score,
)
from sklearn.model_selection import ParameterGrid, train_test_split

EXPERIMENT_NAME = "power_generation_forecasting"
REGISTERED_MODEL_NAME = "power_generation_forecasting_model"
CHAMPION_ALIAS = "champion"
TEST_METRIC_NAME = "test_mse"
N_TRAIN_SAMPLES = 365


class TrainingPipeline(BasePipeline):
    def __init__(self, config: dict):
        super().__init__(config)
        self._mlflow_client = MlflowClient()
        mlflow.set_tracking_uri(self.config["mlflow_tracking_uri"])
        mlflow.set_experiment(EXPERIMENT_NAME)

    def run(self):
        self.log.info("Starting training pipeline")

        features = self._get_train_features(N_TRAIN_SAMPLES)
        if features.empty:
            self.log.error("No training data available. Aborting training.")
            return
        if len(features) < N_TRAIN_SAMPLES:
            self.log.warning(
                f"Only {len(features)} training samples available, "
                f"which is less than the requested {N_TRAIN_SAMPLES}"
            )

        self._train_model(features)

    def _get_train_features(self, n_samples: int) -> pd.DataFrame:
        fs = get_feast_feature_store()
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
                start=datetime.now() - timedelta(days=n_samples),
                end=datetime.now(),
                periods=n_samples,
            )
        ]

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

        return features

    def _train_model(self, features: pd.DataFrame):
        X = features.drop(
            columns=["power_generation_kwh", "event_timestamp", "location"]
        )
        y = features["power_generation_kwh"]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.4, shuffle=False
        )
        X_test, X_val, y_test, y_val = train_test_split(
            X_test, y_test, test_size=0.5, shuffle=False
        )

        param_grid = ParameterGrid(
            {
                "n_estimators": [100, 200, 300, 400],
                "max_depth": [None, 8, 16, 32],
                "min_samples_split": [2, 5, 10],
                "min_samples_leaf": [1, 2, 4],
            }
        )

        best_model = None
        best_params = None
        best_val_mse = float("inf")

        with mlflow.start_run(run_name="rf_hparam_tuning_and_publish"):
            mlflow.log_param("split_strategy", "random_60_20_20")
            mlflow.log_param("n_samples_used", len(features))
            mlflow.log_param("n_trials", len(param_grid))

            for i, params in enumerate(param_grid):
                with mlflow.start_run(
                    run_name=f"trial_{i}",
                    nested=True,
                ):
                    model = RandomForestRegressor(**params)
                    model.fit(X_train, y_train)

                    val_pred = model.predict(X_val)
                    val_mse = mean_squared_error(y_val, val_pred)
                    val_mae = mean_absolute_error(y_val, val_pred)
                    val_r2 = r2_score(y_val, val_pred)

                    mlflow.log_params(params)
                    mlflow.log_metric("val_mse", val_mse)
                    mlflow.log_metric("val_mae", val_mae)
                    mlflow.log_metric("val_r2", val_r2)

                    if val_mse < best_val_mse:
                        best_val_mse = val_mse
                        best_params = params
                        best_model = model

            test_pred = best_model.predict(X_test)

            metrics = {
                "best_val_mse": best_val_mse,
                "test_mse": mean_absolute_error(y_test, test_pred),
                "test_mae": mean_absolute_error(y_test, test_pred),
                "test_mape": mean_absolute_percentage_error(y_test, test_pred),
                "test_r2": r2_score(y_test, test_pred),
            }

            mlflow.log_params({f"best_{k}": v for k, v in best_params.items()})
            mlflow.log_metrics(metrics)

            model_info = mlflow.sklearn.log_model(
                sk_model=best_model,
                name="model",
                registered_model_name=REGISTERED_MODEL_NAME,
                input_example=X_test.head(5),
            )

            model_version = int(model_info.registered_model_version)
            became_champion = self._maybe_promote_to_champion(
                model_name=REGISTERED_MODEL_NAME,
                candidate_version=model_version,
                candidate_test_metric=metrics[TEST_METRIC_NAME],
            )

            mlflow.set_tag("registered_model_name", REGISTERED_MODEL_NAME)
            mlflow.set_tag("registered_model_version", str(model_version))
            mlflow.set_tag("champion_promoted", str(became_champion).lower())

            self.log.info(
                f"Best params: {best_params} | val_mse={best_val_mse:.4f} "
                f"| test_mse={metrics['test_mse']:.4f} | model_version={model_version} "
                f"| champion_promoted={became_champion}"
            )

    def _maybe_promote_to_champion(
        self,
        model_name: str,
        candidate_version: int,
        candidate_test_metric: float,
    ) -> bool:
        try:
            champion_mv = self._mlflow_client.get_model_version_by_alias(
                name=model_name,
                alias=CHAMPION_ALIAS,
            )
            champion_run = self._mlflow_client.get_run(champion_mv.run_id)
            champion_test_metric = champion_run.data.metrics.get(TEST_METRIC_NAME)
        except Exception:
            champion_mv = None
            champion_test_metric = None

        should_promote = (
            champion_mv is None
            or champion_test_metric is None
            or candidate_test_metric < champion_test_metric
        )

        if should_promote:
            self._mlflow_client.set_registered_model_alias(
                name=model_name,
                alias=CHAMPION_ALIAS,
                version=str(candidate_version),
            )

        return should_promote
