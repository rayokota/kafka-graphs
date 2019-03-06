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

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.ComputeFunction;

/**
 *
 * This class computes the Adamic-Adar similarity or distance
 * for each pair of neighbors in an undirected unweighted graph.  
 *
 * To get the exact Adamic-Adar similarity, run the command:
 *
 * <pre>
 * hadoop jar $OKAPI_JAR org.apache.giraph.GiraphRunner \
 *   ml.grafos.okapi.graphs.AdamicAdar\$ComputeLogOfInverseDegree  \
 *   -mc  ml.grafos.okapi.graphs.AdamicAdar\$MasterCompute  \
 *   -eif ml.grafos.okapi.io.formats.LongDoubleTextEdgeInputFormat  \
 *   -eip $INPUT_EDGES \
 *   -eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat \
 *   -op $OUTPUT \
 *   -w $WORKERS \
 *   -ca giraph.oneToAllMsgSending=true \
 *   -ca giraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges \
 *   -ca adamicadar.approximation.enabled=false
 * </pre>
 *
 * Use -ca distance.conversion.enabled=true to get the Adamic-Adar distance instead.
 *
 */
public class AdamicAdar implements ComputeFunction<Long, Double, Double, AdamicAdar.LongIdDoubleValueFriendsList> {

    /** Enables the conversion to distance conversion */
    public static final String DISTANCE_CONVERSION = "distance.conversion.enabled";

    /** Default value for distance conversion */
    public static final boolean DISTANCE_CONVERSION_DEFAULT = false;

    /**
     * Implements the first step in the Adamic-Adar similarity computation.
     * Each vertex computes the log of its inverse degree and sets this value
     * as its own vertex value.
     *
     */
    public static class ComputeLogOfInverseDegree implements
        ComputeFunction<Long, Double, Double, LongIdDoubleValueFriendsList> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<Long, Double> vertex,
            Iterable<LongIdDoubleValueFriendsList> messages,
            Iterable<EdgeWithValue<Long, Double>> edges,
            Callback<Long, Double, Double, LongIdDoubleValueFriendsList> cb
        ) {
            double vertexValue = 0.0;
            int numEdges = 0;
            for (EdgeWithValue<Long, Double> edge : edges) {
                numEdges++;
            }
            if (numEdges > 0) {
                vertexValue = Math.log(1.0 / (double) numEdges);
            }
            cb.setNewVertexValue(vertexValue);
        }
    }

    /**
     * Implements the first step in the exact Adamic-Adar similarity algorithm.
     * Each vertex broadcasts the list with the IDs of all its neighbors and
     * its own value.
     *
     */
    public static class SendFriendsListAndValue implements
        ComputeFunction<Long, Double, Double, LongIdDoubleValueFriendsList> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<Long, Double> vertex,
            Iterable<LongIdDoubleValueFriendsList> messages,
            Iterable<EdgeWithValue<Long, Double>> edges,
            Callback<Long, Double, Double, LongIdDoubleValueFriendsList> cb
        ) {
            List<Long> friends = new ArrayList<>();

            for (EdgeWithValue<Long, Double> edge : edges) {
                friends.add(edge.target());
            }

            if (!(friends.isEmpty())) {
                LongIdDoubleValueFriendsList msg = new LongIdDoubleValueFriendsList(
                    vertex.value(),
                    friends
                );
                for (EdgeWithValue<Long, Double> edge : edges) {
                    cb.sendMessageTo(edge.target(), msg);
                }
            }
        }
    }

    /**
     * This is the message sent in the implementation of the exact Adamic-Adar
     * similarity. The message contains the source vertex value and a list of vertex
     * ids representing the neighbors of the source.
     *
     */
    public static class LongIdDoubleValueFriendsList {

        private final Double vertexValue;
        private final List<Long> neighbors;

        public LongIdDoubleValueFriendsList() {
            this.vertexValue = 0.0;
            this.neighbors = new ArrayList<>();
        }

        public LongIdDoubleValueFriendsList(
            Double value,
            List<Long> neighborList
        ) {
            this.vertexValue = value;
            this.neighbors = neighborList;
        }

        public Double getVertexValue() {
            return this.vertexValue;
        }

        public List<Long> getNeighborsList() {
            return this.neighbors;
        }
    }

    /**
     * Implements the computation of the exact AdamicAdar vertex similarity. The
     * vertex AdamicAdar similarity between u and v is the sum over the common neighbors
     * of u and v, of the log of the inverse degree of each of them
     *
     * This computes similarity only between vertices that are connected with
     * edges, not any pair of vertices in the graph.
     *
     */
    public void superstepCompute(
        int superstep,
        VertexWithValue<Long, Double> vertex,
        Iterable<LongIdDoubleValueFriendsList> messages,
        Iterable<EdgeWithValue<Long, Double>> edges,
        Callback<Long, Double, Double, LongIdDoubleValueFriendsList> cb
    ) {
        Map<Long, Double> edgeValues = new HashMap<>();
        for (EdgeWithValue<Long, Double> edge : edges) {
            edgeValues.put(edge.target(), edge.value());
        }
        for (LongIdDoubleValueFriendsList msg : messages) {
            Double partialValue = msg.getVertexValue();
            for (long id : msg.getNeighborsList()) {
                if (id != vertex.id()) {
                    Double currentEdgeValue = edgeValues.get(id);
                    if (currentEdgeValue != null) {
                        // if the edge exists, add up the partial value to the current sum
                        edgeValues.put(id, currentEdgeValue + partialValue);
                    }
                }
            }
        }
        for (Map.Entry<Long, Double> edge : edgeValues.entrySet()) {
            cb.setNewEdgeValue(edge.getKey(), edge.getValue());
        }
        if (!conversionEnabled) {
            cb.voteToHalt();
        }
    }

    public static class ScaleToDistance implements
        ComputeFunction<Long, Double, Double, LongIdDoubleValueFriendsList> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<Long, Double> vertex,
            Iterable<LongIdDoubleValueFriendsList> messages,
            Iterable<EdgeWithValue<Long, Double>> edges,
            Callback<Long, Double, Double, LongIdDoubleValueFriendsList> cb
        ) {
            for (EdgeWithValue<Long, Double> e : edges) {
                cb.setNewEdgeValue(e.target(), e.value() * -1.0);
            }
            cb.voteToHalt();
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
        VertexWithValue<Long, Double> vertex,
        Iterable<LongIdDoubleValueFriendsList> messages,
        Iterable<EdgeWithValue<Long, Double>> edges,
        Callback<Long, Double, Double, LongIdDoubleValueFriendsList> cb
    ) {
        if (superstep == 0) {
            new ComputeLogOfInverseDegree().compute(superstep, vertex, messages, edges, cb);
        } else {
            if (superstep == 1) {
                new SendFriendsListAndValue().compute(superstep, vertex, messages, edges, cb);
            } else if (superstep == 2) {
                superstepCompute(superstep, vertex, messages, edges, cb);
            } else {
                if (conversionEnabled) {
                    new ScaleToDistance().compute(superstep, vertex, messages, edges, cb);
                }
            }
        }
    }
}