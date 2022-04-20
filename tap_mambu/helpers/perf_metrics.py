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

    @classmethod
    def show_thread_graph(cls):
        import matplotlib.pyplot as plt
        from matplotlib.lines import Line2D

        all_timestamps = [(*generator_time, "r", "Generator") for generator_time in cls._metrics["generator"]] + \
                         [(*processor_time, "b", "Processor") for processor_time in cls._metrics["processor"]]
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
    def show_request_duration_graph(cls):
        import matplotlib.pyplot as plt
        from matplotlib.lines import Line2D

        data_points = [record[1] - record[0] for record in cls._metrics["generator"]]
        x = list(range(len(data_points)))
        plt.bar(x, data_points)
        plt.show()

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
