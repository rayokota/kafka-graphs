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

package io.kgraph.pregel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.aggregators.Aggregator;

/**
 * The user-defined compute function for a Pregel computation.
 *
 * @param <K> The type of the vertex key (the vertex identifier).
 * @param <VV> The type of the vertex value (the state of the vertex).
 * @param <EV> The type of the values that are associated with the edges.
 * @param <Message> The type of the message sent between vertices along the edges.
 */
public interface ComputeFunction<K, VV, EV, Message> {

    default void preSuperstep(Aggregates aggregates) {
    }

    default void postSuperstep(Aggregates aggregates) {
    }

    /**
     * The function for computing a new vertex value or sending messages to the next superstep.
     *
     * @param superstep the count of the current superstep
     * @param vertex the current vertex with its value
     * @param messages a Map of the source vertex and the message sent from the previous superstep
     * @param edges the adjacent edges with their values
     * @param cb a callback for setting a new vertex value or sending messages to the next superstep
     */
    void compute(int superstep,
                 VertexWithValue<K, VV> vertex,
                 Iterable<Message> messages,
                 Iterable<EdgeWithValue<K, EV>> edges,
                 Callback<K, VV, Message> cb);

    class Aggregates {

        protected Map<String, ?> previousAggregates;

        protected Map<String, Aggregator<?>> aggregators;

        public Aggregates(Map<String, ?> previousAggregates, Map<String, Aggregator<?>> aggregators) {
            this.previousAggregates = previousAggregates;
            this.aggregators = aggregators;
        }

        @SuppressWarnings("unchecked")
        public final <T> T previousAggregate(String name) {
            return (T) previousAggregates.get(name);
        }

        @SuppressWarnings("unchecked")
        public final <T> Aggregator<T> aggregator(String name) {
            return (Aggregator<T>) aggregators.get(name);
        }
    }

    final class Callback<K, VV, Message> extends Aggregates {

        protected VV newVertexValue = null;

        protected final Map<K, List<Message>> outgoingMessages = new HashMap<>();

        public Callback(Map<String, ?> previousAggregates, Map<String, Aggregator<?>> aggregators) {
            super(previousAggregates, aggregators);
        }

        public final void sendMessageTo(K target, Message m) {
            List<Message> messages = outgoingMessages.computeIfAbsent(target, k -> new ArrayList<>());
            messages.add(m);
        }

        public final void setNewVertexValue(VV vertexValue) {
            newVertexValue = vertexValue;
        }
    }
}
