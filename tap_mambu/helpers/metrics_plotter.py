# pragma pylint: disable=protected-access
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

from .perf_metrics import PerformanceMetrics as PerfMetrics


# noinspection PyProtectedMember
def show_thread_graph():
    all_timestamps = [(*generator_time, "r", "Generator") for generator_time in PerfMetrics._metrics["generator"]] + \
                     [(*processor_time, "b", "Processor") for processor_time in PerfMetrics._metrics["processor"]]
    counter = 0
    total_time = 0
    for timestamp in sorted(all_timestamps, key=lambda ts: ts[0]):
        start_time = round(timestamp[0] - PerfMetrics._metrics_start_time, 1)
        end_time = round(timestamp[1] - PerfMetrics._metrics_start_time, 1)
        plt.plot([start_time, end_time],
                 [counter, counter], color=timestamp[2], label=timestamp[3], linestyle="-")
        counter += 1
        if end_time > total_time:
            total_time = end_time
    plt.title(f"Total execution time: {total_time}s")
    plt.ylabel("Timestamp")
    plt.legend([Line2D([0], [0], color="r", lw=4), Line2D([0], [0], color="b", lw=4)], ["Generator", "Processor"])

    plt.show()


# noinspection PyProtectedMember
def show_request_duration_graph():
    data_points = [record[1] - record[0] for record in PerfMetrics._metrics["generator"]]
    x = list(range(len(data_points)))
    plt.bar(x, data_points)
    plt.show()