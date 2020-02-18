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

package io.kgraph.streaming;

import java.util.Set;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;

import io.kgraph.Edge;
import io.kgraph.EdgeDirection;

/**
 * The super-class of all graph stream types.
 *
 * @param <K>  the vertex ID type
 * @param <VV> the vertex value type
 * @param <EV> the edge value type
 */
public interface KGraphStream<K, VV, EV> {

    static final short GLOBAL_KEY = 0;

    /**
     * @return the edge DataStream.
     */
    KStream<Edge<K>, EV> edges();

    /**
     * @return the vertex DataStream.
     */
    KStream<K, VV> vertices();

    /**
     * Apply a function to the attribute of each edge in the graph stream.
     *
     * @param <NV> the new vertex value type
     * @param mapper the map function to apply.
     * @param newValueSerde the new value serde
     * @return a new graph stream.
     */
    <NV> KGraphStream<K, VV, NV> mapEdges(
        final KeyValueMapper<Edge<K>, EV, KeyValue<Edge<K>, NV>> mapper, Serde<NV> newValueSerde);

    /**
     * Apply a filter to each edge in the graph stream
     *
     * @param filter the filter function to apply.
     * @return the filtered graph stream.
     */
    KGraphStream<K, VV, EV> filterEdges(Predicate<Edge<K>, EV> filter);

    /**
     * Apply a filter to each vertex in the graph stream
     * Since this is an edge-only stream, the vertex filter can only access the key of vertices
     *
     * @param filter the filter function to apply.
     * @return the filtered graph stream.
     */
    KGraphStream<K, VV, EV> filterVertices(Predicate<K, Void> filter);

    /**
     * @return a data stream representing the number of all edges in the streamed graph, including possible duplicates
     */
    KStream<Short, Long> numberOfEdges();

    /**
     * @return a continuously improving data stream representing the number of vertices in the streamed graph
     */
    KStream<Short, Long> numberOfVertices();

    /**
     * Removes the duplicate edges by storing a neighborhood set for each vertex
     *
     * @return a graph stream with no duplicate edges
     */
    KGraphStream<K, VV, EV> distinct();

    /**
     * @return a graph stream where edges are undirected
     */
    KGraphStream<K, VV, EV> undirected();

    /**
     * @return a graph stream with the edge directions reversed
     */
    KGraphStream<K, VV, EV> reverse();

    /**
     * Get the degree stream
     *
     * @return a stream of vertices, with the degree as the vertex value
     */
    KStream<K, Long> degrees();

    /**
     * Get the in-degree stream
     *
     * @return a stream of vertices, with the in-degree as the vertex value
     */
    KStream<K, Long> inDegrees();

    /**
     * Get the out-degree stream
     *
     * @return a stream of vertices, with the out-degree as the vertex value
     */
    KStream<K, Long> outDegrees();

    /**
     * @param graph the streamed graph to union with
     * @return a streamed graph where the two edge streams are merged
     */
    KGraphStream<K, VV, EV> union(KGraphStream<K, VV, EV> graph);

    /**
     * The aggregate function splits the edge stream up into a vertex stream and applies
     * a mapper on the resulting vertices
     *
     * @param edgeMapper   the mapper that converts the edge stream to a vertex stream
     * @param vertexMapper the mapper that aggregates vertex values
     * @param <VV>         the vertex value used
     * @return a stream of vertices with the aggregated vertex value
     */
    <VV> KStream<K, VV> aggregate(
        KeyValueMapper<Edge<K>, EV, Iterable<KeyValue<K, VV>>> edgeMapper,
        KeyValueMapper<K, VV, KeyValue<K, VV>> vertexMapper);

    /**
     * Returns a global aggregate on the previously split vertex stream
     *
     * @param edgeMapper     the mapper that converts the edge stream to a vertex stream
     * @param vertexMapper   the mapper that aggregates vertex values
     * @param collectUpdates boolean specifying whether the aggregate should only be collected when there is an update
     * @param <VV>           the return value type
     * @return a stream of the aggregated values
     */
    <VV> KStream<Short, VV> globalAggregate(
        KeyValueMapper<Edge<K>, EV, Iterable<KeyValue<K, VV>>> edgeMapper,
        KeyValueMapper<K, VV, Iterable<KeyValue<Short, VV>>> vertexMapper, boolean collectUpdates);

    /**
     * Builds the neighborhood state by creating adjacency lists.
     *
     * @param directed if true, only the out-neighbors will be stored
     *                 otherwise both directions are considered
     * @return a stream of Tuple3, where the first 2 fields identify the edge processed
     * and the third field is the adjacency list that was updated by processing this edge.
     */
    KStream<Edge<K>, Set<K>> buildNeighborhood(boolean directed);

    /**
     * Discretizes the edge stream into tumbling windows of the specified size.
     * <p>
     * The edge stream is partitioned so that all neighbors of a vertex belong to the same partition.
     * The KeyedStream is then windowed into tumbling time windows.
     * <p>
     * By default, each vertex is grouped with its outgoing edges.
     * Use {@link #slice(Windows, EdgeDirection)} to manually set the edge direction grouping.
     *
     * @param size the size of the window
     * @return a GraphWindowStream of the specified size
     */
    KGraphWindowedStream<K, EV> slice(Windows<? extends Window> size);

    /**
     * Discretizes the edge stream into tumbling windows of the specified size.
     * <p>
     * The edge stream is partitioned so that all neighbors of a vertex belong to the same partition.
     * The KeyedStream is then windowed into tumbling time windows.
     *
     * @param size      the size of the window
     * @param direction the EdgeDirection to key by
     * @return a GraphWindowStream of the specified size, keyed by
     */
    KGraphWindowedStream<K, EV> slice(Windows<? extends Window> size, EdgeDirection direction);

    /**
     * Applies an incremental aggregation on a graphstream and returns a stream of aggregation results
     *
     * @param summaryAggregation the summary aggregation
     * @param <S> initial type
     * @param <T> result type
     * @return the aggregation results
     */
    <S, T> KTable<Windowed<Short>, T> aggregate(SummaryAggregation<K, EV, S, T> summaryAggregation);
}