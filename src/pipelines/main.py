import argparse

from pipelines.feature_pipeline import FeaturePipeline
from pipelines.inference_pipeline import BatchInferencePipeline
from pipelines.training_pipeline import TrainingPipeline
from pipelines.utils import get_logger, load_config


def main():
    log = get_logger("main")
    config = load_config()

    parser = argparse.ArgumentParser(description="Gizstrom Pipelines")
    parser.add_argument(
        "--pipeline",
        type=str,
        choices=["feature", "training", "batch-inference", "data-collection"],
        required=True,
        help="The pipeline to run",
    )

    args = parser.parse_args()
    log.info(f"Running pipeline: {args.pipeline}")

    match args.pipeline:
        case "training":
            p = TrainingPipeline(config)
        case "feature":
            p = FeaturePipeline(config)
        case "batch-inference":
            p = BatchInferencePipeline(config)

    p.run()


if __name__ == "__main__":
    main()
