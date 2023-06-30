/**
 * Copyright (C) 2019 Dean De Leo, email: dleo[at]cwi.nl
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef LIVE_GRAPH_TWO_CDLP_H
#define LIVE_GRAPH_TWO_CDLP_H

#include <cstdint>
#include <vector>

#include "data-structure/TopologyInterface.h"
#include "internal-driver/Driver.h"

using namespace std;

class CDLP {

public:
    static vector<pair<vertex_id_t, vertex_id_t>> cdlp(Driver& driver, TopologyInterface& ds, uint64_t max_iterations, bool run_on_raw_neighbourhoud);

    static vector<vertex_id_t> teseo_cdlp(TopologyInterface& ds, uint64_t max_iterations);
};


#endif //LIVE_GRAPH_TWO_CDLP_H
