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
import org.jblas.FloatMatrix;

import io.kgraph.GraphSerialized;
import io.kgraph.library.cf.CfLongIdSerde;
import io.kgraph.library.cf.Svdpp;
import io.kgraph.pregel.ComputeFunction;
import io.kgraph.utils.KryoSerde;

public enum GraphAlgorithmType {
    bfs,
    lcc,
    lp,
    mssp,
    pagerank,
    sssp,
    svdpp,
    wcc;

    public static ComputeFunction computeFunction(GraphAlgorithmType type) {
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
            case svdpp:
                return new Svdpp();
            case wcc:
                return new ConnectedComponents<>();
            default:
                throw new IllegalArgumentException("Unsupported algorithm type");
        }
    }

    public static GraphSerialized graphSerialized(GraphAlgorithmType type, boolean useDouble) {
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
            case svdpp:
                return GraphSerialized.with(new CfLongIdSerde(), new KryoSerde<>(), Serdes.Float());
            case wcc:
                return useDouble
                    ? GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Double())
                    : GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long());
            default:
                throw new IllegalArgumentException("Unsupported algorithm type");
        }
    }

    public static ValueMapper initialVertexValueMapper(GraphAlgorithmType type) {
        switch (type) {
            case bfs:
                return id -> BreadthFirstSearch.UNVISITED;
            case lcc:
                return id -> 1.0;
            case lp:
                return id -> id;
            case mssp:
                return id -> new HashMap<>();
            case pagerank:
                return id -> Double.POSITIVE_INFINITY;
            case sssp:
                return id -> Double.POSITIVE_INFINITY;
            case svdpp:
                return id -> new Svdpp.SvdppValue(0f, new FloatMatrix(), new FloatMatrix());
            case wcc:
                return id -> id;
            default:
                throw new IllegalArgumentException("Unsupported algorithm type");
        }
    }
}
