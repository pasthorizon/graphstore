#!python3

import sys
import struct

seperator = " "

input_path = sys.argv[1]
output_path = sys.argv[2]
value_output_type = sys.argv[3]

VALUE_TYPE_DOUBLE = 0
VALUE_TYPE_INT = 1
VALUE_TYPE_LONG = 3

value_type = -1

if value_output_type == "uint":
    value_output_type = "I"
    value_type = VALUE_TYPE_INT
elif value_output_type == "ulong":
    value_output_type = "q"
    value_type = VALUE_TYPE_LONG
elif value_output_type == "double":
    value_output_type = "d"
    value_type = VALUE_TYPE_DOUBLE
else:
    exit(2)

output_format_string = "q" + value_output_type

def detect_value_type(value):
    try:
        int(value)
        return VALUE_TYPE_INT
    except ValueError:
        return VALUE_TYPE_DOUBLE


lines = 0
with open(input_path) as i:
    line = i.readline()
    while line:
        lines += 1
        line = i.readline()




with open(input_path) as i:
    with open(output_path, "bw") as o:
        o.write(struct.pack("q", lines))
        line = i.readline()
        while line:
            [vertex, value] = line.split(seperator)

            bin = None
            if value_type == VALUE_TYPE_INT:
                v = int(value)
                if 4294967295 < v:
                    v = 4294967295
                bin = struct.pack(output_format_string, int(vertex), v)
            elif value_type == VALUE_TYPE_DOUBLE:
                bin = struct.pack(output_format_string, int(vertex), float(value))
            elif value_type == VALUE_TYPE_LONG:
                bin = struct.pack(output_format_string, int(vertex), int(value))
            o.write(bin)

            line = i.readline()
