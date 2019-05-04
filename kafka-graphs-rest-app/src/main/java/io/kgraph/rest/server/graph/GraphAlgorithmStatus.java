/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kgraph.rest.server.graph;

import java.util.Map;
import java.util.stream.Collectors;

import io.kgraph.GraphAlgorithmState;
import io.kgraph.GraphAlgorithmState.State;
import io.kgraph.pregel.PregelComputation;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
class GraphAlgorithmStatus {

    public GraphAlgorithmStatus(GraphAlgorithmState<?> state) {
        this.state = state.state();
        this.superstep = state.superstep();
        this.runningTime = state.runningTime();
        this.aggregates = state.aggregates().entrySet().stream()
            .filter(e -> e.getKey().equals(PregelComputation.LAST_WRITTEN_OFFSETS))
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
    }

    private State state;
    private int superstep;
    private long runningTime;
    private Map<String, String> aggregates;
}
