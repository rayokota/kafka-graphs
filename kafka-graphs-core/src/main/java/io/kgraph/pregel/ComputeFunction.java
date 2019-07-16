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

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.PregelComputation.AggregatorWrapper;
import io.kgraph.pregel.aggregators.Aggregator;

/**
 * The user-defined compute function for a Pregel computation.
 *
 * @param <K> The type of the vertex key (the vertex identifier).
 * @param <VV> The type of the vertex value (the state of the vertex).
 * @param <EV> The type of the values that are associated with the edges.
 * @param <Message> The type of the message sent between vertices along the edges.
 */
@FunctionalInterface
public interface ComputeFunction<K, VV, EV, Message> {

    /**
     * Initialize the ComputeFunction, this is the place to register aggregators.
     *
     * @param configs configuration parameters
     * @param cb a callback for registering aggregators
     */
    default void init(Map<String, ?> configs, InitCallback cb) {
    }

    /**
     * A function for performing sequential computations between supersteps.
     *
     * @param superstep the superstep
     * @param cb a callback for writing to aggregators or halting the computation
     */
    default void masterCompute(int superstep, MasterCallback cb) {
    }

    /**
     * Prepare for computation.  This method is executed exactly once prior to compute() being called
     * for any of the vertices in the partition.
     *
     * @param superstep the superstep
     * @param aggregators the aggregators
     */
    default void preSuperstep(int superstep, Aggregators aggregators) {
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
                 Callback<K, VV, EV, Message> cb);

    /**
     * Finish computation.  This method is executed exactly once after computation
     * for all vertices in the partition is complete.
     *
     * @param superstep the superstep
     * @param aggregators the aggregators
     */
    default void postSuperstep(int superstep, Aggregators aggregators) {
    }


    final class InitCallback {

        protected final Map<String, AggregatorWrapper<?>> aggregators;

        public InitCallback(Map<String, AggregatorWrapper<?>> aggregators) {
            this.aggregators = aggregators;
        }

        public <T> void registerAggregator(String name,
                                           Class<? extends Aggregator<T>> aggregatorClass) {
            registerAggregator(name, aggregatorClass, false);
        }

        public <T> void registerAggregator(String name,
                                           Class<? extends Aggregator<T>> aggregatorClass,
                                           boolean persistent) {
            aggregators.put(name, new AggregatorWrapper<>(aggregatorClass, persistent));
        }
    }

    interface ReadAggregators {
        <T> T getAggregatedValue(String name);
    }

    interface ReadWriteAggregators extends ReadAggregators {
        <T> void aggregate(String name, T value);
    }

    final class MasterCallback implements ReadAggregators {

        protected final Map<String, Aggregator<?>> previousAggregators;

        protected boolean haltComputation = false;

        public MasterCallback(Map<String, Aggregator<?>> previousAggregators) {
            this.previousAggregators = previousAggregators;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T getAggregatedValue(String name) {
            return (T) previousAggregators.get(name).getAggregate();
        }

        @SuppressWarnings("unchecked")
        public final <T> void setAggregatedValue(String name, T value) {
            ((Aggregator<T>) previousAggregators.get(name)).setAggregate(value);
        }

        public void haltComputation() {
            haltComputation = true;
        }
    }

    final class Aggregators implements ReadWriteAggregators {

        protected final Map<String, ?> previousAggregates;

        protected final Map<String, Aggregator<?>> aggregators;

        public Aggregators(Map<String, ?> previousAggregates, Map<String, Aggregator<?>> aggregators) {
            this.previousAggregates = previousAggregates;
            this.aggregators = aggregators;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T getAggregatedValue(String name) {
            return (T) previousAggregates.get(name);
        }

        @Override
        public final <T> void aggregate(String name, T value) {
            aggregator(name).aggregate(value);
        }

        @SuppressWarnings("unchecked")
        private <T> Aggregator<T> aggregator(String name) {
            return (Aggregator<T>) aggregators.get(name);
        }
    }

    final class Callback<K, VV, EV, Message> implements ReadWriteAggregators {

        protected final ProcessorContext context;

        protected final K key;

        protected final TimestampedKeyValueStore<K, Map<K, EV>> edgesStore;

        protected VV newVertexValue = null;

        protected final Map<K, List<Message>> outgoingMessages = new HashMap<>();

        protected boolean voteToHalt = false;

        protected final Map<String, ?> previousAggregates;

        protected final Map<String, Map<K, ?>> aggregators;

        public Callback(ProcessorContext context,
                        K key,
                        TimestampedKeyValueStore<K, Map<K, EV>> edgesStore,
                        Map<String, ?> previousAggregates,
                        Map<String, Map<K, ?>> aggregators) {
            this.context = context;
            this.previousAggregates = previousAggregates;
            this.aggregators = aggregators;
            this.key = key;
            this.edgesStore = edgesStore;
        }

        public final void sendMessageTo(K target, Message m) {
            List<Message> messages = outgoingMessages.computeIfAbsent(target, k -> new ArrayList<>());
            messages.add(m);
        }

        public final void setNewVertexValue(VV vertexValue) {
            newVertexValue = vertexValue;
        }

        public final void addEdge(K target, EV value) {
            Map<K, EV> edges = ValueAndTimestamp.getValueOrNull(edgesStore.get(key));
            if (edges == null) {
                edges = new HashMap<>();
            }
            edges.put(target, value);
            edgesStore.put(key, ValueAndTimestamp.make(edges, context.timestamp()));
        }

        public final void removeEdge(K target) {
            Map<K, EV> edges = ValueAndTimestamp.getValueOrNull(edgesStore.get(key));
            if (edges == null) {
                return;
            }
            edges.remove(target);
            edgesStore.put(key, ValueAndTimestamp.make(edges, context.timestamp()));
        }

        public final void setNewEdgeValue(K target, EV value) {
            Map<K, EV> edges = ValueAndTimestamp.getValueOrNull(edgesStore.get(key));
            if (edges == null) {
                return;
            }
            edges.replace(target, value);
            edgesStore.put(key, ValueAndTimestamp.make(edges, context.timestamp()));
        }

        public void voteToHalt() {
            voteToHalt = true;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T getAggregatedValue(String name) {
            return (T) previousAggregates.get(name);
        }

        @Override
        public final <T> void aggregate(String name, T value) {
            aggregator(name).put(key, value);
        }

        @SuppressWarnings("unchecked")
        private <T> Map<K, T> aggregator(String name) {
            return (Map<K, T>) aggregators.get(name);
        }
    }
}
