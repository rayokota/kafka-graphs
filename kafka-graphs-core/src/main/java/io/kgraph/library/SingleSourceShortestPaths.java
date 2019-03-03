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

public class SingleSourceShortestPaths implements ComputeFunction<Long, Double, Double, Double> {
    private static final Logger log = LoggerFactory.getLogger(SingleSourceShortestPaths.class);

    public static final String SRC_VERTEX_ID = "srcVertexId";

    private long srcVertexId;

    @Override
    public void init(Map<String, ?> configs, InitCallback cb) {
        srcVertexId = (Long) configs.get(SRC_VERTEX_ID);
    }

    @Override
    public void compute(
        int superstep,
        VertexWithValue<Long, Double> vertex,
        Iterable<Double> messages,
        Iterable<EdgeWithValue<Long, Double>> edges,
        Callback<Long, Double, Double, Double> cb) {

        double minDistance = (vertex.id().equals(srcVertexId)) ? 0d : Double.POSITIVE_INFINITY;

        for (Double message : messages) {
            minDistance = Math.min(minDistance, message);
        }

        log.debug(">>> Vertex {} got minDist = {} vertex value {}", vertex.id(), minDistance, vertex.value());

        if (minDistance < vertex.value()) {
            cb.setNewVertexValue(minDistance);
            for (EdgeWithValue<Long, Double> edge : edges) {
                double distance = minDistance + edge.value();
                log.debug(">>> Vertex {} sent to {} = {}", vertex.id(), edge.target(), distance);
                cb.sendMessageTo(edge.target(), distance);
            }
        }

        cb.voteToHalt();
    }
}
