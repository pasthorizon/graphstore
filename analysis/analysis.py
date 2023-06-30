#!/usr/bin/env python
import math
import subprocess
import pandas as pd
import numpy as np
from enum import Enum
import matplotlib.pyplot as plt
from matplotlib import rcParams

rcParams['figure.figsize'] = 10, 8

REMOTE_USER = "fuchs"
REMOTE_URL = "scyper15.in.tum.de"
REMOTE_PATH = "/home/fuchs/graph-two-results.csv"
LOCAL_PATH = "/home/per/graph-two-results.csv"

TEMPLATE_PATH = "analysis/html"

FIGURE_PATH = "/home/per/workspace/live-graph-two/analysis/figures"

pd.options.display.precision = 2


class DataStructure(Enum):
    CSR = "csr"


def merge_datasets():
    if input("Merge datasets?") == "y":
        data = pd.read_csv("/home/per/graph-two-results.csv", delimiter=";")
        data1 = pd.read_csv("/home/per/graph-two-results-merge.csv", delimiter=";")

        concat_data = pd.concat((data, data1), )

        concat_data.to_csv("/home/per/graph-two-results.csv", sep=";", index=False)


def get_differences_in_percent(numbers):
    differences = []

    for i in range(1, len(numbers)):
        differences.append(float((numbers[i] - numbers[0])) / float(numbers[0]) * 100)

    return differences


def get_report_file(remote_user, remote_url, remote_path, local_path):
    if input("Get report?") == "y":
        file_location = "%s@%s:%s" % (remote_user, remote_url, remote_path)
        result = subprocess.run(["scp", file_location, local_path])
        if result.returncode != 0:
            print("Could update report file from %s" % file_location)


# Drops the first three repetition as warmup.
def filter_out_warmup(data):
    # if input("Use cold runs? ") == "y":
    #     return data[data["repetition"] == 0]
    # else:
    return data[data["repetition"] > 2]


def get_last_executions(data):
    executions = int(input("Until execution? (-1 for all, -2 for last)"))
    if executions == -1:
        return data
    elif executions == -2:
        max_execution_id = data["execution_id"].max()
        return data[data["execution_id"] == max_execution_id]
    else:
        return data[data["execution_id"] >= executions]


def rewrite_dataset(data):
    data["dataset"] = data["dataset"].apply(lambda ds: ds.split("/")[-2])
    return data


def generate_report():
  data = pd.read_csv(LOCAL_PATH, delimiter=";")
  # data = filter_out_warmup(data)
  # data = get_last_executions(data)
  data = rewrite_dataset(data)

  data["data_structure"] = data["data_structure"].map(lambda s: s.replace("csrMalloc", "csr"))
  data["r"] = data["runtime"]

  data = data.drop("runtime", axis=1)
  data = data.drop("storage", axis=1)
  data = data.drop("Unnamed: 8", axis=1)
  data = data.drop("repetition", axis=1)
  data = data.drop("timestamp", axis=1)
  data = data.drop("execution_id", axis=1)

  # data.query("data_structure != 'mallocAL(0,1)'", inplace=True)

  datasets = set(data["dataset"])
  experiments = set(data["experiment"])

  data["pivot_index"] = data.groupby("data_structure")["data_structure"].cumcount()

  data = data.set_index(["experiment", "dataset", "pivot_index", "data_structure"]).stack().unstack([3, 4])
  data.index = data.index.droplevel(2)
  # print(data)
  for e in experiments:
      for d in datasets:
          filtered_data = data.query("experiment == '%s' & dataset == '%s'" % (e, d))

          median =  filtered_data.median().sort_values()
          median = median.dropna()
          print(median)
          subplot = filtered_data[median.index].boxplot()

          difference_total = (float(median[-1]) - float(median[0])) / float(median[0]) * 100
          difference_min = (float(median[1]) - float(median[0])) / float(median[0]) * 100
          if math.isnan(difference_total):
              difference_in_percent = 0
          if math.isnan(difference_min):
              difference_min = 0


          file_title = "%s, %s" % (e, d)
          title = "%s, %s, Difference total / min: %i%% / %i%%" % (e, d, difference_total, difference_min)
          subplot.set_title(title)
          subplot.set_xlabel("data structures")
          subplot.set_ylabel("runtime [microseconds]")

          subplot.set_ylim((0.0, subplot.get_ylim()[1]))

          differences = get_differences_in_percent(median)

          def modify_x_tick_labels(tl):
              if tl[0] >= 1:
                tl[1].set_text("%s (%.1f%%)" % (tl[1].get_text(), differences[tl[0] - 1]))

          list(map(modify_x_tick_labels, enumerate(subplot.get_xticklabels())))
          subplot.set_xticklabels(subplot.get_xticklabels(), rotation=90)


          plt.tight_layout()
          plt.savefig(FIGURE_PATH + "/" + file_title + ".png")

          plt.clf()



  # pivot = data.pivot_table(index=["experiment", "dataset"],
  #                          columns=["data_structure"],
  #                          values=["runtime", "storage"])
  #
  # for ds in pivot.columns.levels[1]:
  #     if ds == "csr()":
  #         continue
  #     pivot["ratios", ds] = pivot["runtime", ds] / pivot["runtime", "csr()"]
  # pivot = pivot.drop(columns="runtime")
  #

  # print(pivot)
  return data


get_report_file(REMOTE_USER, REMOTE_URL, REMOTE_PATH, LOCAL_PATH)
merge_datasets()

global data
data = generate_report()