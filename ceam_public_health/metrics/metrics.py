class Metrics:
    """This class declares a value pipeline that allows other components to store summary metrics."""
    def setup(self, builder):
        self.metrics = builder.value('metrics')
        self.metrics.source = lambda index: {}

