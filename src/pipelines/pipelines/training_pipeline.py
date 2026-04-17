from pipelines.utils import BasePipeline  # noqa: F401


class TrainingPipeline(BasePipeline):
    def __init__(self, config: dict):
        super().__init__(config)

    def run(self):
        pass
