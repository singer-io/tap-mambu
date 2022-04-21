import math
import time


class PerformanceMetrics:
    _metrics_start_time = time.monotonic()
    _metrics = dict(generator=list(),
                    processor=list(),
                    generator_wait=list(),
                    processor_wait=list())
    _generator_batch_size = 500

    def __init__(self, metric_name):
        self.start_time = None
        if metric_name not in self._metrics:
            raise ValueError("One argument must be True, but only one!")
        self._metric_name = metric_name

    def __enter__(self):
        self.start_time = time.monotonic()

    def __exit__(self, exc_type, exc_val, exc_tb):
        metric = (self.start_time, time.monotonic())
        self._metrics[self._metric_name].append(metric)

    @classmethod
    def reset_metrics(cls):
        cls._metrics_start_time = time.monotonic()
        cls._metrics = dict(generator=list(),
                            processor=list(),
                            generator_wait=list(),
                            processor_wait=list())

    @classmethod
    def set_generator_batch_size(cls, batch_size):
        if any(cls._metrics.values()):
            raise RuntimeError("You cannot change batch size after measuring metrics!")
        cls._generator_batch_size = batch_size

    @property
    def generator_batch_size(self):
        return self._generator_batch_size

    @staticmethod
    def get_sum(metric):
        if not metric:
            return 0
        return sum([record[1] - record[0] for record in metric])

    @staticmethod
    def get_avg_with_98th(metric):
        if not metric:
            return 0, 0
        values_total = sorted([record[1] - record[0] for record in metric], reverse=True)
        values_98th = values_total[:math.ceil(len(values_total) * 2 / 100)]

        average = sum(values_total) / len(values_total)
        average_98th = sum(values_98th) / len(values_98th)

        return average, average_98th

    @classmethod
    def get_statistics(cls):
        extraction_duration = time.monotonic() - cls._metrics_start_time

        generator_avg, generator_avg_98th = cls.get_avg_with_98th(cls._metrics["generator"])
        processor_avg, processor_avg_98th = cls.get_avg_with_98th(cls._metrics["processor"])
        generator_wait = cls.get_sum(cls._metrics["generator_wait"])
        processor_wait = cls.get_sum(cls._metrics["processor_wait"])

        return dict(generator=generator_avg / cls._generator_batch_size,
                    generator_98th=generator_avg_98th / cls._generator_batch_size,
                    processor=processor_avg, processor_98th=processor_avg_98th,
                    generator_wait=generator_wait,
                    processor_wait=processor_wait,
                    records=len(cls._metrics["processor"]) // extraction_duration,
                    extraction=extraction_duration)
