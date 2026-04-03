from pipelines.utils import BasePipeline

class BatchInferencePipeline(BasePipeline):
    def __init__(self, config: dict):
        super().__init__(config)

    def run(self):
        pass