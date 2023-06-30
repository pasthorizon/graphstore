#!/usr/bin/env python

import subprocess
import pandas as pd
import numpy as np
from enum import Enum
import matplotlib.pyplot as plt

LOCAL_PATH = "/home/per/workspace/live-graph-two/analysis/data/csr-vs-al.csv"

FIGURE_PATH = "/home/per/workspace/live-graph-two/analysis/figures"

pd.options.display.precision = 2


class DataStructure(Enum):
    CSR = "csr"


def rewrite_dataset(data):
    data["dataset"] = data["dataset"].apply(lambda ds: ds.split("/")[-2])
    return data


label_names = {
    "(csr(), r)": "CSR",
    "(csrAL(0,0), r)": "degree in index",
    "(csrAL(32,0), r)": "csr < 32",
    "(csrAL(64,0), r)": "csr < 64",
    "(csrAL(128,0), r)": "csr < 128",
    "(mallocAL(0), r)": "eliminate second pointer",
    "(vectorAL(1), r)": "vector",
    "(vectorAL(0), r)": "vector sorted"
}


def rename_label(l):
  if l.get_text() in label_names:
    l.set_text(label_names[l.get_text()])
  return l


def generate_report():
    data = pd.read_csv(LOCAL_PATH, delimiter=";")
    data = rewrite_dataset(data)

    data["data_structure"] = data["data_structure"].map(lambda s: s.replace("csrMalloc", "csr"))
    data["r"] = data["runtime"]
    data["r"] = data["r"] / 1000


    data = data.drop("runtime", axis=1)
    data = data.drop("storage", axis=1)
    data = data.drop("Unnamed: 8", axis=1)
    data = data.drop("repetition", axis=1)
    data = data.drop("timestamp", axis=1)
    data = data.drop("execution_id", axis=1)

    datasets = set(data["dataset"])
    experiments = set(data["experiment"])

    data["pivot_index"] = data.groupby("data_structure")["data_structure"].cumcount()

    data = data.set_index(["experiment", "dataset", "pivot_index", "data_structure"]).stack().unstack([3, 4])
    data.index = data.index.droplevel(2)

    data = data.drop(("csrAL(0,1)", "r"), axis=1)
    # data = data.drop(("csrAL(10000000,1)", "r"), axis=1)
    # data = data.drop(("csrAL(10000000,0)", "r"), axis=1)
    data = data.drop(("mallocAL(1)", "r"), axis=1)

    data = data[[
                 ("csr()", "r"),
                 ("csrAL(128,0)", "r"),
                 ("csrAL(64,0)", "r"),
                 ("csrAL(32,0)", "r"),
                 ("csrAL(0,0)", "r"),
                 ("mallocAL(0)", "r"),
                 ("vectorAL(0)", "r"),
                 ("vectorAL(1)", "r"),
                 ]]

    for e in experiments:
        for d in datasets:
            subplot = data.query("experiment == '%s' & dataset == '%s'" % (e, d)).boxplot()

            title = "%s, %s" % (e, d)
            subplot.set_title(title)
            subplot.set_xlabel("data structure")
            subplot.set_ylabel("runtime [ms]")

            subplot.set_ylim((0.0, subplot.get_ylim()[1]))

            x_tick_labels = subplot.get_xticklabels()
            x_tick_labels = list(map(rename_label, x_tick_labels))
            subplot.set_xticklabels(x_tick_labels, rotation=45)

            plt.tight_layout()
            plt.savefig(FIGURE_PATH + "/" + title + ".png")

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

global data
data = generate_report()