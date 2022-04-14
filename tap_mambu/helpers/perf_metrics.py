import time
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D


class PerformanceMetrics:
    metrics_start_time = time.monotonic()
    generator_metrics = list()
    processor_metrics = list()
    generator_batch_size = 500

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
            self.generator_metrics.append(metric)
        elif self.is_processor_metric:
            self.processor_metrics.append(metric)

    @classmethod
    def set_generator_batch_size(cls, batch_size):
        cls.generator_batch_size = batch_size

    @classmethod
    def show_graph(cls):

        all_timestamps = [(*generator_time, "r", "Generator") for generator_time in cls.generator_metrics] + \
                         [(*processor_time, "b", "Processor") for processor_time in cls.processor_metrics]
        counter = 0
        total_time = 0
        for timestamp in sorted(all_timestamps, key=lambda ts: ts[0]):
            start_time = round(timestamp[0] - cls.metrics_start_time, 1)
            end_time = round(timestamp[1] - cls.metrics_start_time, 1)
            plt.plot([start_time, end_time],
                     [counter, counter], color=timestamp[2], label=timestamp[3], linestyle="-")
            counter += 1
            if end_time > total_time:
                total_time = end_time
        plt.title(f"Total execution time: {total_time}s")
        plt.ylabel("Timestamp")
        plt.legend([Line2D([0], [0], color="r", lw=4), Line2D([0], [0], color="b", lw=4)], ["Generator", "Processor"])

        generator_times = [timestamp[1] - timestamp[0]
                           for timestamp in sorted(all_timestamps, key=lambda ts: ts[0])
                           if timestamp[2] == 'r']
        processor_times = [timestamp[1] - timestamp[0]
                           for timestamp in sorted(all_timestamps, key=lambda ts: ts[0])
                           if timestamp[2] == 'b']
        print(f"Average Generator Time: {sum(generator_times) / len(generator_times)}s")
        print(f"Total Processor Time: {sum(processor_times)}")
        print(f"Average Processor Time: {sum(processor_times) / len(processor_times)}s")

        plt.show()

    @classmethod
    def get_statistics(cls):
        generator_durations = sorted([record[1]-record[0] for record in cls.generator_metrics], reverse=True)
        processor_durations = sorted([record[1]-record[0] for record in cls.processor_metrics], reverse=True)

        generator_durations_98th = generator_durations[:len(generator_durations) * 2 // 100]
        processor_durations_98th = processor_durations[:len(processor_durations) * 2 // 100]

        generator_avg = sum(generator_durations) / len(generator_durations) / cls.generator_batch_size
        generator_avg_98th = sum(generator_durations_98th) / len(generator_durations_98th) / cls.generator_batch_size

        processor_avg = sum(processor_durations) / len(processor_durations)
        processor_avg_98th = sum(processor_durations_98th) / len(processor_durations_98th)

        return dict(generator=generator_avg, generator_98th=generator_avg_98th,
                    processor=processor_avg, processor_98th=processor_avg_98th)
