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

package io.kgraph.library;

import java.util.HashMap;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.ValueMapper;

import io.kgraph.GraphSerialized;
import io.kgraph.pregel.ComputeFunction;
import io.kgraph.utils.KryoSerde;

public enum GraphAlgorithmType {
    bfs,
    lcc,
    lp,
    mssp,
    pagerank,
    sssp,
    wcc;

    public static ComputeFunction<?, ?, ?, ?> computeFunction(GraphAlgorithmType type) {
        switch (type) {
            case bfs:
                return new BreadthFirstSearch<>();
            case lcc:
                return new LocalClusteringCoefficient();
            case lp:
                return new LabelPropagation<>();
            case mssp:
                return new MultipleSourceShortestPaths();
            case pagerank:
                return new PageRank<>();
            case sssp:
                return new SingleSourceShortestPaths();
            case wcc:
                return new ConnectedComponents<>();
            default:
                throw new IllegalArgumentException("Unsupported algorithm type");
        }
    }

    public static GraphSerialized<?, ?, ?> graphSerialized(GraphAlgorithmType type, boolean useDouble) {
        switch (type) {
            case bfs:
                return useDouble
                    ? GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Double())
                    : GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long());
            case lcc:
                return GraphSerialized.with(Serdes.Long(), Serdes.Double(), Serdes.Double());
            case lp:
                return useDouble
                    ? GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Double())
                    : GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long());
            case mssp:
                return GraphSerialized.with(Serdes.Long(), new KryoSerde<>(), Serdes.Double());
            case pagerank:
                return GraphSerialized.with(Serdes.Long(), new KryoSerde<>(), Serdes.Double());
            case sssp:
                return GraphSerialized.with(Serdes.Long(), Serdes.Double(), Serdes.Double());
            case wcc:
                return useDouble
                    ? GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Double())
                    : GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long());
            default:
                throw new IllegalArgumentException("Unsupported algorithm type");
        }
    }

    public static Object initialVertexValue(GraphAlgorithmType type) {
        switch (type) {
            case bfs:
                return BreadthFirstSearch.UNVISITED;
            case lcc:
                return 1.0;
            case lp:
                return null;
            case mssp:
                return new HashMap<>();
            case pagerank:
                return Double.POSITIVE_INFINITY;
            case sssp:
                return Double.POSITIVE_INFINITY;
            case wcc:
                return null;
            default:
                throw new IllegalArgumentException("Unsupported algorithm type");
        }
    }
}
