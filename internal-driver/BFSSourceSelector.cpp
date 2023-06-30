//
// Created by per on 09.10.20.
//

#include "BFSSourceSelector.h"

#include "algorithms/Algorithms.h"


const string BFSSourceSelector::SOURCE_FOLDER =  "/home/fuchs/graph-two-bfs-sources/";

vertex_id_t BFSSourceSelector::get_source() {
  const string file_path = SOURCE_FOLDER + dataset.get_name() + ".source";
  if (!file_exists(file_path)) {
    vertex_id_t source = find_source();

    string s_source = to_string(source);

    ofstream f(file_path);

    if (f.bad()) {
      throw ConfigurationError("Cannot create source file for BFS configuration.");
    }

    f << s_source << endl;
    f.close();
    return source;
  } else {
    ifstream f(file_path);

    string s_source;
    getline(f, s_source);

    f.close();

    return stoi(s_source);
  }
}

vertex_id_t BFSSourceSelector::find_source() {
  cout << "Finding source for bfs in graph " << dataset.get_name() << endl;
  uint target_vertices = ds.vertex_count() * TARGET_PERCENTAGE;

  vertex_id_t max_vertex =0;
  size_t max_traversed = 0;
  vector<pair<vertex_id_t, uint>> distances;
  for (vertex_id_t v = 0; v < ds.vertex_count(); v++) {
    distances = Algorithms::bfs(driver, ds, v);
    auto traversed_vertices = Algorithms::traversed_vertices(ds, distances);

    if (max_traversed < traversed_vertices) {
      max_vertex = v;
      max_traversed = traversed_vertices;
    }

    cout << "Traversed " << traversed_vertices << endl;
    cout << "Percentage " << (float) traversed_vertices / (float) ds.vertex_count() << endl;
    if (target_vertices < traversed_vertices) {
      cout << "Found source " << v << endl;
      return v;
    }
  }
  cout << "Couldn't find a source from which a BFS traverses " << TARGET_PERCENTAGE << "% of the graph." << endl;
  cout << "Highest was " << max_traversed / ds.vertex_count() << "% at vertex " << max_vertex << endl;
  throw ConfigurationError("No BFS source available");
}
