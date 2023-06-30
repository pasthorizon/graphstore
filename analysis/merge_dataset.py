import pandas as pd

MERGE_1_PATH = "/home/per/graph-two-results.csv"
MERGE_2_PATH = "/home/per/graph-two-results-merge.csv"
OUTPUT_PATH = "/home/per/graph-two-results.csv"

data = pd.read_csv(MERGE_1_PATH, delimiter=";")
data1 = pd.read_csv(MERGE_2_PATH, delimiter=";")

concat_data = pd.concat((data, data1), )

concat_data.to_csv(OUTPUT_PATH, sep=";", index=False)