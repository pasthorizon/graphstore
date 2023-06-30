//
// Created by per on 21.07.20.
//

#include "Options.h"

#include <getopt.h>
#include <iostream>

#include "utils/utils.h"

Options parseOptions(int argc, char **argv) {
  Options o;
  int c;
  optind = 1; // Reset optind for multiple runs

  while (1) {
    int option_index = 0;
    static struct option long_options[] = {
            {"temporal",required_argument, 0, 't'},  // If the dataset contains temporal edges, the argument gives the position of the creation timestamp.
                // If a temporal network is given and parsed as such, the edge insertion dataset are the last x% sorted by creation time.
            {"densify", no_argument, 0, 'e'},        // If the dataset has no dense id or none-numeric ids and needs to be translated into dense numeric ids.
            {"insert_percentage", required_argument, 0, 'i'}, // The percentage of edges to be choosen for insertion.
            {"delete_percentage", required_argument, 0, 'd'}, // The percentage of edges to be choosen for deletion.
            {"make_undirected", no_argument, 0, 'u'}, // Creates edge list which include only edge with src < dst and base dataset which have edges in both directions
            {"weighted", no_argument, 0, 'w'}, // Creates edge list which include only edge with src < dst and base dataset which have edges in both directions
    };

    c = getopt_long(argc, argv,"",
                    long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
      case 'i':
        o.insert_percentage = stof(optarg);
        break;
      case 'd':
        o.deletion_percentage = stof(optarg);
        break;
      case 'e':
        o.densify = true;
        break;
      case 'w':
        o.input_format = WEIGHTED_EDGELIST_TEXT;
        break;
      case 't':
        o.input_format = TEMPORAL_EDGELIST_TEXT;
        o.temporal_value_position = stoi(optarg);
        break;
      case 'u':
        o.make_undirected = true;
        o.base_file_name = "undirected_" + o.base_file_name;
        break;
      case '?':
        printf("No help provided read src.\n");
        break;
      default:
        printf("?? getopt returned character code 0%o ??\n", c);
    }
  }

  if (optind < argc) {
    o.input_path = argv[optind];
  }

  optind++;
  if (optind < argc) {
    o.output_path = argv[optind];
  }

  o.validate();

  return o;
}

void Options::validate() {
  if (input_path.empty()) {
    printf("No input path provided.\n");
    exit(BAD_CONF);
  }

  if (output_path.empty()) {
    printf("No output path provided.\n");
    exit(BAD_CONF);
  }
  cout << input_path << endl;
  if(!file_exists(input_path)) {
    cout << "Input file does not exist." << endl;
    exit(BAD_CONF);
  }

  if(!file_exists(output_path)) {
    cout << "Output path does not exist." << endl;
    exit(BAD_CONF);
  }

  if (input_format == TEMPORAL_EDGELIST_TEXT) {
    if (temporal_value_position == 0 || temporal_value_position == 1 || temporal_value_position == numeric_limits<std::size_t>::max()) {
      printf("Invalid temporal value position.");
      exit(BAD_CONF);
    }
  }
}

