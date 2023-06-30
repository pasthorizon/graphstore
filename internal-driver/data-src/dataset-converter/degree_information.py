import struct

import matplotlib.pyplot as plt
import sys


def read_degree_information(file_path):
    SIZE_T = "N"
    VERTEX_ID_T = "I"

    degrees = []

    with open(file_path, "rb") as f:
        size = struct.unpack(SIZE_T, f.read(struct.calcsize(SIZE_T)))[0]

        for n in range(size):
            _ = f.read(struct.calcsize(VERTEX_ID_T))
            degrees.append(struct.unpack(SIZE_T, f.read(struct.calcsize(SIZE_T)))[0])

    return degrees


def generate_histogram(values, graph_name):
    bins = range(0, max(values), 30)
    plt.hist(values, bins, histtype="step", density=1, cumulative=1)

    plt.xscale("log")
    # plt.yscale("log")

    plt.xlabel("degree (log)")
    plt.ylabel("CDF")

    plt.title(graph_name + " degree distribution")

    plt.tight_layout()
    # plt.show()


    plt.savefig("./" + graph_name + "_degree_distribution.png")

    plt.clf()

    # Log,log histogram
    plt.hist(values, bins, histtype="step")

    plt.xscale("log")
    plt.yscale("log")

    plt.xlabel("degree (log)")
    plt.ylabel("# number of occurences")

    plt.title(graph_name + " degree distribution")

    plt.tight_layout()
    # plt.show()


    plt.savefig("./" + graph_name + "_degree_distribution_log_log.png")



def generate_characteristics_file(values, graph_name):
    with open("./" + graph_name + ".degree_characteristics", "w") as f:
        m = max(values)
        print("Max degree: ", m, file=f)
        zero = values.count(0) / len(values)
        print("0 degree vertices", zero, file=f)
        below_10 = len(list(filter(lambda x: x < 10, values))) / len(values)
        print("Below 10 degree vertices", below_10, file=f)
        below_20 = len(list(filter(lambda x: x < 20, values))) / len(values)
        print("Below 20 degree vertices", below_20, file=f)
        below_100 = len(list(filter(lambda x: x < 100, values))) / len(values)
        print("Below 100 degree vertices", below_100, file=f)


file_path = sys.argv[1]
graph_name = sys.argv[2]

degrees = read_degree_information(file_path)
generate_histogram(degrees, graph_name)
generate_characteristics_file(degrees, graph_name)