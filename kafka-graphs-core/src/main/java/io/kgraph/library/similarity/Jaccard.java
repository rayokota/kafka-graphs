/*
 * Copyright 2014 Grafos.ml
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kgraph.library.similarity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.ComputeFunction;

/**
 *
 * This class computes the Jaccard similarity or distance
 * for each pair of neighbors in an undirected unweighted graph.  
 *
 * @author dl
 *
 */
public class Jaccard<VV> implements ComputeFunction<Long, VV, Double, Jaccard.MessageWrapper<Long, List<Long>>> {

    /** Enables the conversion to distance conversion */
    public static final String DISTANCE_CONVERSION = "distance.conversion.enabled";

    /** Default value for distance conversion */
    public static final boolean DISTANCE_CONVERSION_DEFAULT = false;

    /**
     * Implements the first step in the exact jaccard similirity algorithm. Each
     * vertex broadcasts the list with the IDs of al its neighbors.
     * @author dl
     *
     */
    public static class SendFriends<K, VV, EV> implements ComputeFunction<K, VV, EV, MessageWrapper<K, List<K>>> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<K, VV> vertex,
            Iterable<MessageWrapper<K, List<K>>> messages,
            Iterable<EdgeWithValue<K, EV>> edges,
            Callback<K, VV, EV, MessageWrapper<K, List<K>>> cb
        ) {

            final List<K> friends = new ArrayList<>();

            for (EdgeWithValue<K, EV> edge : edges) {
                friends.add(edge.target());
            }

            for (EdgeWithValue<K, EV> edge : edges) {
                cb.sendMessageTo(edge.target(), new MessageWrapper<>(edge.source(), friends));
            }
        }
    }

    public static class MessageWrapper<K, Message> {
        /** Message sender vertex Id. */
        private final K sourceId;
        /** Message with data. */
        private final Message message;

        public MessageWrapper(final K sourceId, final Message message) {
            this.sourceId = sourceId;
            this.message = message;
        }

        public final K getSourceId() {
            return sourceId;
        }

        public final Message getMessage() {
            return message;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MessageWrapper<?, ?> that = (MessageWrapper<?, ?>) o;
            return Objects.equals(sourceId, that.sourceId) &&
                Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceId, message);
        }

        @Override
        public String toString() {
            return "MessageWrapper{"
                + ", sourceId=" + sourceId
                + ", message=" + message
                + '}';
        }
    }

    /**
     * Implements the computation of the exact Jaccard vertex similarity. The
     * vertex Jaccard similarity between u and v is the number of common neighbors
     * of u and v divided by the number of vertices that are neighbors of u or v.
     *
     * This computes similarity only between vertices that are connected with
     * edges, not any pair of vertices in the graph.
     *
     * @param superstep the count of the current superstep
     * @param vertex the current vertex with its value
     * @param messages a Map of the source vertex and the message sent from the previous superstep
     * @param edges the adjacent edges with their values
     * @param cb a callback for setting a new vertex value or sending messages to the next superstep
     *
     * @author dl
     */
    public void superstepCompute(
        int superstep,
        VertexWithValue<Long, VV> vertex,
        Iterable<MessageWrapper<Long, List<Long>>> messages,
        Iterable<EdgeWithValue<Long, Double>> edges,
        Callback<Long, VV, Double, MessageWrapper<Long, List<Long>>> cb
    ) {

        int numEdges = 0;
        Map<Long, Double> edgeValues = new HashMap<>();
        for (EdgeWithValue<Long, Double> edge : edges) {
            numEdges++;
            edgeValues.put(edge.target(), edge.value());
        }
        for (MessageWrapper<Long, List<Long>> msg : messages) {
            Long src = msg.getSourceId();
            Double edgeValue = edgeValues.get(src);
            long totalFriends = numEdges;
            long commonFriends = 0;
            for (Long id : msg.getMessage()) {
                if (edgeValues.get(id) != null) { // This is a common friend
                    commonFriends++;
                } else {
                    totalFriends++;
                }
            }
            // The Jaccard similarity is commonFriends/totalFriends
            // If the edge to the vertex with ID src does not exist, which is the
            // case in a directed graph, this call has no effect.
            cb.setNewEdgeValue(src, (double) commonFriends / (double) totalFriends);
        }
        if (!conversionEnabled) {
            cb.voteToHalt();
        }
    }

    public static class ScaleToDistance<VV> implements ComputeFunction<Long, VV, Double, MessageWrapper<Long, List<Long>>> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<Long, VV> vertex,
            Iterable<MessageWrapper<Long, List<Long>>> messages,
            Iterable<EdgeWithValue<Long, Double>> edges,
            Callback<Long, VV, Double, MessageWrapper<Long, List<Long>>> cb
        ) {

            for (EdgeWithValue<Long, Double> e : edges) {
                cb.setNewEdgeValue(e.target(), convertToDistance(e.value()));
            }
            cb.voteToHalt();
        }
    }

    /**
     *
     * Converts the [0,1] similarity value to a distance
     * which takes values in [0, INF].
     *
     */
    private static Double convertToDistance(Double value) {
        if (Math.abs(value) > 0) {
            return (1.0 / value) - 1.0;
        } else {
            return Double.MAX_VALUE;
        }
    }

    private boolean conversionEnabled;

    @SuppressWarnings("unchecked")
    @Override
    public final void init(Map<String, ?> configs, InitCallback cb) {
        Map<String, Object> c = (Map<String, Object>) configs;
        conversionEnabled = (Boolean) c.getOrDefault(DISTANCE_CONVERSION, DISTANCE_CONVERSION_DEFAULT);
    }

    @Override
    public void compute(
        int superstep,
        VertexWithValue<Long, VV> vertex,
        Iterable<MessageWrapper<Long, List<Long>>> messages,
        Iterable<EdgeWithValue<Long, Double>> edges,
        Callback<Long, VV, Double, MessageWrapper<Long, List<Long>>> cb
    ) {
        if (superstep == 0) {
            new SendFriends<Long, VV, Double>().compute(superstep, vertex, messages, edges, cb);
        } else if (superstep == 1) {
            superstepCompute(superstep, vertex, messages, edges, cb);
        } else {
            if (conversionEnabled) {
                new ScaleToDistance<VV>().compute(superstep, vertex, messages, edges, cb);
            }
        }
    }
}