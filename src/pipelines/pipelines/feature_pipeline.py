from pipelines.utils import BasePipeline

class FeaturePipeline(BasePipeline):
    def __init__(self, config: dict):
        super().__init__(config)

    def run(self):
        pass