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

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.ComputeFunction;

/**
 * Adapted from the Graphalytics implementation.
 */
public class LocalClusteringCoefficient implements ComputeFunction<Long, Double, Double, LocalClusteringCoefficient.LCCMessage> {
    private static final Logger log = LoggerFactory.getLogger(LocalClusteringCoefficient.class);

    @Override
    public void compute(
        int superstep,
        VertexWithValue<Long, Double> vertex,
        Iterable<LCCMessage> messages,
        Iterable<EdgeWithValue<Long, Double>> edges,
        Callback<Long, Double, Double, LCCMessage> cb
    ) {
        log.debug("step {} vertex {} value {}", superstep, vertex.id(), vertex.value());

        if (superstep == 0) {
            LCCMessage message = new LCCMessage(vertex.id());
            for (EdgeWithValue<Long, Double> edge : edges) {
                log.debug(">>> Vertex {} sent self to {}", vertex.id(), edge.target());
                cb.sendMessageTo(edge.target(), message);
            }
        } else if (superstep == 1) {
            Set<Long> neighbors = neighbors(vertex.id(), edges, messages);
            sendConnectionInquiries(vertex.id(), neighbors, cb);
            cb.setNewVertexValue((double) neighbors.size());
        } else if (superstep == 2) {
            sendConnectionReplies(vertex.id(), edges, messages, cb);
        } else if (superstep == 3) {
            cb.setNewVertexValue(computeLCC(vertex.value(), messages));
            cb.voteToHalt();
        }
    }

    private Set<Long> neighbors(
        long vertexId,
        Iterable<EdgeWithValue<Long, Double>> edges,
        Iterable<LCCMessage> messages
    ) {

        Set<Long> neighbors = new HashSet<>();
        for (EdgeWithValue<Long, Double> edge : edges) {
            neighbors.add(edge.target());
        }
        for (LCCMessage message : messages) {
            if (message.source != vertexId) {
                neighbors.add(message.source);
            }
        }
        return neighbors;
    }

    private void sendConnectionInquiries(
        long sourceVertexId, Set<Long> neighbors,
        Callback<Long, Double, Double, LCCMessage> cb
    ) {
        if (neighbors.size() <= 1) {
            log.debug(">>> Vertex {} not sending inquiries to {}", sourceVertexId, neighbors);
            return;
        }

        LCCMessage message = new LCCMessage(
            sourceVertexId,
            neighbors.stream().mapToLong(Long::longValue).toArray()
        );
        for (Long neighbor : neighbors) {
            log.debug(">>> Vertex {} sent inquiry to {}", sourceVertexId, neighbor);
            cb.sendMessageTo(neighbor, message);
        }
    }

    private void sendConnectionReplies(
        Long vertexId,
        Iterable<EdgeWithValue<Long, Double>> edges,
        Iterable<LCCMessage> inquiries,
        Callback<Long, Double, Double, LCCMessage> cb
    ) {
        Set<Long> neighbors = new HashSet<>();
        for (EdgeWithValue<Long, Double> edge : edges) {
            neighbors.add(edge.target());
        }
        for (LCCMessage msg : inquiries) {
            int matchCount = 0;
            for (long edgeId : msg.edgeList) {
                if (neighbors.contains(edgeId)) {
                    matchCount++;
                }
            }
            log.debug(">>> Vertex {} sent reply {} to {}", vertexId, matchCount, msg.source);
            cb.sendMessageTo(msg.source, new LCCMessage(matchCount));
        }
    }

    private double computeLCC(double numberOfNeighbours, Iterable<LCCMessage> messages) {
        if (numberOfNeighbours < 2) {
            return 0.0;
        }

        long numberOfMatches = 0;
        for (LCCMessage msg : messages) {
            numberOfMatches += msg.matchCount;
        }
        double lcc = numberOfMatches / numberOfNeighbours / (numberOfNeighbours - 1);
        return lcc;
    }

    public final static class LCCMessage {

        private long source = 0L;
        private long[] edgeList = null;
        private int matchCount = 0;

        public LCCMessage(long source) {
            this.source = source;
        }

        public LCCMessage(int matchCount) {
            this.matchCount = matchCount;
        }

        public LCCMessage(long source, long[] edgeList) {
            this.source = source;
            this.edgeList = edgeList;
        }
    }
}
