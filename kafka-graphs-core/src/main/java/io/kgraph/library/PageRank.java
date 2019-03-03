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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.ComputeFunction;
import io.vavr.Tuple2;

public class PageRank<K> implements ComputeFunction<K, Tuple2<Double, Double>, Double, Double> {
    private static final Logger log = LoggerFactory.getLogger(PageRank.class);

    public static final String TOLERANCE = "tolerance";
    public static final String RESET_PROBABILITY = "resetProbability";
    public static final String SRC_VERTEX_ID = "srcVertexId";

    private double tolerance;
    private double resetProbability;
    private K srcVertexId;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Map<String, ?> configs, InitCallback cb) {
        tolerance = (Double) configs.get(TOLERANCE);
        resetProbability = (Double) configs.get(RESET_PROBABILITY);
        srcVertexId = (K) configs.get(SRC_VERTEX_ID);
    }

    @Override
    public void compute(
        int superstep,
        VertexWithValue<K, Tuple2<Double, Double>> vertex,
        Iterable<Double> messages,
        Iterable<EdgeWithValue<K, Double>> edges,
        Callback<K, Tuple2<Double, Double>, Double, Double> cb) {

        double oldPageRank = vertex.value()._1;
        double oldDelta = vertex.value()._2;

        double messageSum = 0.0;
        for (Double message : messages) {
            messageSum += message;
        }

        boolean isPersonalized = srcVertexId != null;
        double newPageRank = isPersonalized && oldDelta == Double.NEGATIVE_INFINITY
            ? 1.0 : oldPageRank + (1.0 - resetProbability) * messageSum;
        double newDelta = newPageRank - oldPageRank;

        log.debug("step {} vertex {} sum {}", superstep, vertex.id(), messageSum);
        log.debug("old ({},{})", oldPageRank, oldDelta);
        log.debug("new ({},{})", newPageRank, newDelta);
        log.debug("msgs {}", messages);

        cb.setNewVertexValue(new Tuple2<>(newPageRank, newDelta));

        for (EdgeWithValue<K, Double> edge : edges) {
            if (newDelta > tolerance) {
                log.debug("sending to target {} edge {} msg {}", edge.target(), edge.value(), newDelta * edge.value());
                cb.sendMessageTo(edge.target(), newDelta * edge.value());
            }
        }

        cb.voteToHalt();
    }
}
