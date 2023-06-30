//
// Created by per on 02.09.20.
//

#ifndef LIVE_GRAPH_TWO_REPORTER_H
#define LIVE_GRAPH_TWO_REPORTER_H

#include <fstream>
#include <ctime>
#include <unordered_map>
#include <fstream>
#include <iostream>

#include "Configuration.h"
#include "Configuration.h"

/**
 * Reports experimental results to a CSR file.
 *
 * Each line in the file contains, the ID of the execution, timestamp of the execution
 * start, experiment name, data structure name and dataset name.
 *
 * All times are given as unix timestamp or in microseconds.
 *
 * The file is called graph-two-results.csv and is located in the home directory of the executing
 * user.
 */
class Reporter {
public:
    Reporter(Config& config) {
      string file_path = get_home_dir();
      if (config.release) {
        file_path += + "/graph-two-results.csv";
      } else {
        file_path += + "/graph-two-results-debug.csv";
      }

      if (!file_exists(file_path)) {
        cout << "Starting new reporting file at " + file_path << endl;
        file.open(file_path, fstream::in | fstream::out | fstream::app);

        if (!file.good()) {
          throw ConfigurationError("Could not create report file: " + file_path);
        }

        write_standard_header();
      } else {
        cout << "Reporting to file at " + file_path << endl;
        file.open(file_path, fstream::in | fstream::out | fstream::app);
      }

      execution_id = get_highest_execution_id() + 1;

      header = get_header();
    }

    ~Reporter() {
      file.close();
    }

    void add_repetition(Experiments experiment, int repetition,
            unordered_map<string, ulong> metrics);

    void add_repetition(Experiments experiment, int repetition,
                        ulong runtime);

    void set_dataset(Dataset dataset);
    void set_data_structure(DataStructures ds, const vector<string>& parameters);

private:
    const vector<string> standard_header {"execution_id", "timestamp", "repetition", "dataset",
                                          "experiment", "data_structure",
                                          "runtime", "storage"};
    const char separator = ';';

    fstream file;

    vector<string> header;

    ulong execution_id;

    Dataset dataset;
    DataStructures data_structure;

    ulong get_highest_execution_id();

    vector<string> get_header();

    void write_standard_header();

    vector<string> data_structure_parameters;
};


#endif //LIVE_GRAPH_TWO_REPORTER_H
