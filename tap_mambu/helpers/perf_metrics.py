import math
import time
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D


class PerformanceMetrics:
    _metrics_start_time = time.monotonic()
    _generator_metrics = list()
    _processor_metrics = list()
    _generator_batch_size = 500

    def __init__(self, generator=False, processor=False):
        self.start_time = None
        if generator == processor:
            raise ValueError("Either generator or processor argument must be True, but not both!")
        self.is_generator_metric = generator
        self.is_processor_metric = processor

    def __enter__(self):
        self.start_time = time.monotonic()

    def __exit__(self, exc_type, exc_val, exc_tb):
        metric = (self.start_time, time.monotonic())
        if self.is_generator_metric:
            self._generator_metrics.append(metric)
        elif self.is_processor_metric:
            self._processor_metrics.append(metric)

    @classmethod
    def reset_metrics(cls):
        cls._metrics_start_time = time.monotonic()
        cls._generator_metrics = list()
        cls._processor_metrics = list()

    @classmethod
    def set_generator_batch_size(cls, batch_size):
        if cls._generator_metrics or cls._processor_metrics:
            raise RuntimeError("You cannot change batch size after measuring metrics!")
        cls._generator_batch_size = batch_size

    @property
    def generator_batch_size(self):
        return self._generator_batch_size

    @classmethod
    def show_graph(cls):
        all_timestamps = [(*generator_time, "r", "Generator") for generator_time in cls._generator_metrics] + \
                         [(*processor_time, "b", "Processor") for processor_time in cls._processor_metrics]
        counter = 0
        total_time = 0
        for timestamp in sorted(all_timestamps, key=lambda ts: ts[0]):
            start_time = round(timestamp[0] - cls._metrics_start_time, 1)
            end_time = round(timestamp[1] - cls._metrics_start_time, 1)
            plt.plot([start_time, end_time],
                     [counter, counter], color=timestamp[2], label=timestamp[3], linestyle="-")
            counter += 1
            if end_time > total_time:
                total_time = end_time
        plt.title(f"Total execution time: {total_time}s")
        plt.ylabel("Timestamp")
        plt.legend([Line2D([0], [0], color="r", lw=4), Line2D([0], [0], color="b", lw=4)], ["Generator", "Processor"])

        plt.show()

    @classmethod
    def get_statistics(cls):
        extraction_duration = time.monotonic() - cls._metrics_start_time
        generator_durations = sorted([record[1] - record[0] for record in cls._generator_metrics], reverse=True)
        processor_durations = sorted([record[1] - record[0] for record in cls._processor_metrics], reverse=True)

        generator_durations_98th = generator_durations[:math.ceil(len(generator_durations) * 2 / 100)]
        processor_durations_98th = processor_durations[:math.ceil(len(processor_durations) * 2 / 100)]

        generator_avg = sum(generator_durations) / len(generator_durations) / cls._generator_batch_size
        generator_avg_98th = sum(generator_durations_98th) / len(generator_durations_98th) / cls._generator_batch_size

        processor_avg = sum(processor_durations) / len(processor_durations)
        processor_avg_98th = sum(processor_durations_98th) / len(processor_durations_98th)

        return dict(generator=generator_avg, generator_98th=generator_avg_98th,
                    processor=processor_avg, processor_98th=processor_avg_98th,
                    records=len(processor_durations)//extraction_duration,
                    extraction=extraction_duration)
