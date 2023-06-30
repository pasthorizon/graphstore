#!/usr/bin/env python

import subprocess
import pandas as pd
import numpy as np
from enum import Enum
import matplotlib.pyplot as plt

REMOTE_USER = "fuchs"
REMOTE_URL = "scyper15.in.tum.de"
REMOTE_PATH = "/home/fuchs/graph-two-results.csv"
LOCAL_PATH = "/home/per/workspace/live-graph-two/analysis/data/interface-access-vs-direct-access.csv"


pd.options.display.precision = 2


def get_report_file(remote_user, remote_url, remote_path, local_path):
    if input("Get report?") == "y":
        file_location = "%s@%s:%s" % (remote_user, remote_url, remote_path)
        result = subprocess.run(["scp", file_location, local_path])
        if result.returncode != 0:
            print("Could update report file from %s" % file_location)


# Drops the first three repetition as warmup.
def filter_out_warmup(data):
    if input("Use cold runs? ") == "y":
        return data[data["repetition"] == 0]
    else:
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
    data = filter_out_warmup(data)
    # data = get_last_executions(data)
    data = rewrite_dataset(data)


    data["data_structure"] = data.apply(lambda r: "csr" if r["execution_id"] == 1 else "csr-direct", axis=1)

    data = data.drop("storage", axis=1)
    data = data.drop("Unnamed: 8", axis=1)
    data = data.drop("repetition", axis=1)
    data = data.drop("timestamp", axis=1)
    data = data.drop("execution_id", axis=1)


    pivot = data.pivot_table(index=["experiment", "dataset"],
                             columns=["data_structure"],
                             values=["runtime"])

    for ds in pivot.columns.levels[1]:
        if ds == "csr-direct":
            continue
        pivot["ratios", ds] = pivot["runtime", ds] / pivot["runtime", "csr-direct"]

    print(pivot)
    return data


# get_report_file(REMOTE_USER, REMOTE_URL, REMOTE_PATH, LOCAL_PATH)

global data
data = generate_report()