import pandas as pd

FILTER_PATH = "/home/per/graph-two-results-filter.csv"
OUTPUT_PATH = "/home/per/graph-two-results.csv"

data = pd.read_csv(FILTER_PATH, delimiter=";")

data = data.query("experiment != '2-neighbour'")
data = data.query("data_structure != 'csrMallocAL(0,512)'")
data = data.query("data_structure != 'csrMallocAL(1,512)'")
data = data.query("data_structure != 'csrMallocAL(0,32)'")
data = data.query("data_structure != 'csrMallocAL(1,32)'")

data.to_csv(OUTPUT_PATH, sep=";", index=False)