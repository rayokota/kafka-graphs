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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;

import io.kgraph.Edge;
import io.kgraph.EdgeDirection;
import io.kgraph.EdgeWithValue;
import io.kgraph.GraphSerialized;
import io.kgraph.KGraph;

/**
 * Represents a graph stream where the stream consists solely of edges.
 * <p>
 *
 * @param <K>  the key type for edge and vertex identifiers.
 * @param <EV> the value type for edges.
 */
public class EdgeStream<K, EV> implements KGraphStream<K, Void, EV> {

    private final KStream<Edge<K>, EV> edges;
    private final GraphSerialized<K, Void, EV> serialized;

    /**
     * Creates a graph from an edge stream.
     *
     * @param edges a DataStream of edges.
     */
    public EdgeStream(KStream<Edge<K>, EV> edges, GraphSerialized<K, Void, EV> serialized) {
        this.edges = edges;
        this.serialized = serialized;
    }

    /**
     * @return the edge DataStream.
     */
    @Override
    public KStream<Edge<K>, EV> edges() {
        return edges;
    }

    /**
     * @return the vertex DataStream.
     */
    @Override
    public KStream<K, Void> vertices() {
        ValueMapper<K, Void> vertexValueInitializer = e -> null;
        return edges
            .flatMap(new KGraph.EmitSrcAndTarget<K, Void, EV>(vertexValueInitializer))
            .filter(new FilterDistinctVertices<K>());
    }

    private static final class FilterDistinctVertices<K> implements Predicate<K, Void> {
        private final Set<K> keys = new HashSet<>();

        @Override
        public boolean test(K key, Void value) {
            if (!keys.contains(key)) {
                keys.add(key);
                return true;
            }
            return false;
        }
    }

    /**
     * Apply a function to the attribute of each edge in the graph stream.
     *
     * @param mapper the map function to apply.
     * @return a new graph stream.
     */
    @Override
    public <NV> EdgeStream<K, NV> mapEdges(
        final KeyValueMapper<Edge<K>, EV, KeyValue<Edge<K>, NV>> mapper, Serde<NV> newValueSerde
    ) {
        KStream<Edge<K>, NV> mappedEdges = edges.map(mapper);
        return new EdgeStream<K, NV>(
            mappedEdges,
            GraphSerialized.with(serialized.keySerde(), serialized.vertexValueSerde(), newValueSerde)
        );
    }

    /**
     * Apply a filter to each edge in the graph stream
     *
     * @param filter the filter function to apply.
     * @return the filtered graph stream.
     */
    @Override
    public EdgeStream<K, EV> filterEdges(Predicate<Edge<K>, EV> filter) {
        KStream<Edge<K>, EV> remainingEdges = edges.filter(filter);
        return new EdgeStream<>(remainingEdges, serialized);
    }

    /**
     * Apply a filter to each vertex in the graph stream
     * Since this is an edge-only stream, the vertex filter can only access the key of vertices
     *
     * @param filter the filter function to apply.
     * @return the filtered graph stream.
     */
    @Override
    public EdgeStream<K, EV> filterVertices(Predicate<K, Void> filter) {
        KStream<Edge<K>, EV> remainingEdges = edges
            .filter(new ApplyVertexFilterToEdges<K, EV>(filter));

        return new EdgeStream<>(remainingEdges, serialized);
    }

    private static final class ApplyVertexFilterToEdges<K, EV>
        implements Predicate<Edge<K>, EV> {
        private final Predicate<K, Void> vertexFilter;

        public ApplyVertexFilterToEdges(Predicate<K, Void> vertexFilter) {
            this.vertexFilter = vertexFilter;
        }

        @Override
        public boolean test(Edge<K> edge, EV value) {
            boolean sourceVertexKept = vertexFilter.test(edge.source(), null);
            boolean targetVertexKept = vertexFilter.test(edge.target(), null);
            return sourceVertexKept && targetVertexKept;
        }
    }

    /**
     * @return a data stream representing the number of all edges in the streamed graph, including possible duplicates
     */
    @Override
    public KStream<Short, Long> numberOfEdges() {
        return edges.map(new TotalEdgeCountMapper<K, EV>());
    }

    private static final class TotalEdgeCountMapper<K, EV>
        implements KeyValueMapper<Edge<K>, EV, KeyValue<Short, Long>> {
        private long edgeCount;

        public TotalEdgeCountMapper() {
            edgeCount = 0;
        }

        @Override
        public KeyValue<Short, Long> apply(Edge<K> edge, EV value) {
            edgeCount++;
            return new KeyValue<>(GLOBAL_KEY, edgeCount);
        }
    }

    /**
     * @return a continuously improving data stream representing the number of vertices in the streamed graph
     */
    @Override
    public KStream<Short, Long> numberOfVertices() {
        return globalAggregate(new DegreeTypeSeparator<K, EV>(true, true),
            new VertexCountMapper<K>(), true
        );
    }

    private static final class VertexCountMapper<K> implements KeyValueMapper<K, Long, Iterable<KeyValue<Short, Long>>> {
        private final Set<K> vertices;

        public VertexCountMapper() {
            this.vertices = new HashSet<>();
        }

        @Override
        public Iterable<KeyValue<Short, Long>> apply(K vertex, Long count) {
            vertices.add(vertex);
            return Collections.singletonList(new KeyValue<>(GLOBAL_KEY, (long) vertices.size()));
        }
    }

    /**
     * Removes the duplicate edges by storing a neighborhood set for each vertex
     *
     * @return a graph stream with no duplicate edges
     */
    @Override
    public EdgeStream<K, EV> distinct() {
        KStream<Edge<K>, EV> edgeStream = edges.flatMap(new DistinctEdgeMapper<K, EV>());

        return new EdgeStream<>(edgeStream, serialized);
    }

    private static final class DistinctEdgeMapper<K, EV>
        implements KeyValueMapper<Edge<K>, EV, Iterable<KeyValue<Edge<K>, EV>>> {
        private final Set<K> neighbors;

        public DistinctEdgeMapper() {
            this.neighbors = new HashSet<>();
        }

        @Override
        public Iterable<KeyValue<Edge<K>, EV>> apply(Edge<K> edge, EV value) {
            if (!neighbors.contains(edge.target())) {
                neighbors.add(edge.target());
                return Collections.singletonList(new KeyValue<>(edge, value));
            }
            return Collections.emptyList();
        }
    }

    /**
     * @return a graph stream where edges are undirected
     */
    @Override
    public EdgeStream<K, EV> undirected() {
        KStream<Edge<K>, EV> reversedEdges = edges.flatMap(new KGraph.UndirectEdges<K, EV>());
        return new EdgeStream<>(reversedEdges, serialized);
    }

    /**
     * @return a graph stream with the edge directions reversed
     */
    @Override
    public EdgeStream<K, EV> reverse() {
        return new EdgeStream<>(edges.map(new ReverseEdgeMapper<K, EV>()), serialized);
    }

    private static final class ReverseEdgeMapper<K, EV> implements KeyValueMapper<Edge<K>, EV, KeyValue<Edge<K>, EV>> {
        @Override
        public KeyValue<Edge<K>, EV> apply(Edge<K> edge, EV value) {
            return new KeyValue<>(edge.reverse(), value);
        }
    }

    /**
     * Get the degree stream
     *
     * @return a stream of vertices, with the degree as the vertex value
     */
    @Override
    public KStream<K, Long> degrees() {
        return aggregate(
            new DegreeTypeSeparator<K, EV>(true, true),
            new DegreeMapFunction<K>()
        );
    }

    /**
     * Get the in-degree stream
     *
     * @return a stream of vertices, with the in-degree as the vertex value
     */
    @Override
    public KStream<K, Long> inDegrees() {
        return aggregate(
            new DegreeTypeSeparator<K, EV>(true, false),
            new DegreeMapFunction<K>()
        );
    }

    /**
     * Get the out-degree stream
     *
     * @return a stream of vertices, with the out-degree as the vertex value
     */
    @Override
    public KStream<K, Long> outDegrees() {
        return aggregate(
            new DegreeTypeSeparator<K, EV>(false, true),
            new DegreeMapFunction<K>()
        );
    }

    private static final class DegreeTypeSeparator<K, EV>
        implements KeyValueMapper<Edge<K>, EV, Iterable<KeyValue<K, Long>>> {
        private final boolean collectIn;
        private final boolean collectOut;

        public DegreeTypeSeparator(boolean collectIn, boolean collectOut) {
            this.collectIn = collectIn;
            this.collectOut = collectOut;
        }

        @Override
        public Iterable<KeyValue<K, Long>> apply(Edge<K> edge, EV value) {
            List<KeyValue<K, Long>> result = new ArrayList<>();
            if (collectOut) {
                result.add(new KeyValue<>(edge.source(), 1L));
            }
            if (collectIn) {
                result.add(new KeyValue<>(edge.target(), 1L));
            }
            return result;
        }
    }

    private static final class DegreeMapFunction<K>
        implements KeyValueMapper<K, Long, KeyValue<K, Long>> {
        private final Map<K, Long> localDegrees;

        public DegreeMapFunction() {
            localDegrees = new HashMap<>();
        }

        @Override
        public KeyValue<K, Long> apply(K key, Long degree) {
            long newDegree = localDegrees.compute(key, (k, v) -> (v == null) ? degree : v + degree);
            return new KeyValue<>(key, newDegree);
        }
    }

    /**
     * @param graph the streamed graph to union with
     * @return a streamed graph where the two edge streams are merged
     */
    @Override
    public EdgeStream<K, EV> union(KGraphStream<K, Void, EV> graph) {
        return new EdgeStream<>(edges.merge(graph.edges()), serialized);
    }

    /**
     * The aggregate function splits the edge stream up into a vertex stream and applies
     * a mapper on the resulting vertices
     *
     * @param edgeMapper   the mapper that converts the edge stream to a vertex stream
     * @param vertexMapper the mapper that aggregates vertex values
     * @param <VV>         the vertex value used
     * @return a stream of vertices with the aggregated vertex value
     */
    @Override
    public <VV> KStream<K, VV> aggregate(
        KeyValueMapper<Edge<K>, EV, Iterable<KeyValue<K, VV>>> edgeMapper,
        KeyValueMapper<K, VV, KeyValue<K, VV>> vertexMapper
    ) {
        return edges.flatMap(edgeMapper)
            .map(vertexMapper);
    }

    /**
     * Returns a global aggregate on the previously split vertex stream
     *
     * @param edgeMapper     the mapper that converts the edge stream to a vertex stream
     * @param vertexMapper   the mapper that aggregates vertex values
     * @param collectUpdates boolean specifying whether the aggregate should only be collected when there is an update
     * @param <VV>           the return value type
     * @return a stream of the aggregated values
     */
    @Override
    public <VV> KStream<Short, VV> globalAggregate(
        KeyValueMapper<Edge<K>, EV, Iterable<KeyValue<K, VV>>> edgeMapper,
        KeyValueMapper<K, VV, Iterable<KeyValue<Short, VV>>> vertexMapper, boolean collectUpdates
    ) {

        KStream<Short, VV> result = edges.flatMap(edgeMapper).flatMap(vertexMapper);

        if (collectUpdates) {
            result = result.flatMap(new GlobalAggregateMapper<VV>());
        }

        return result;
    }


    private static final class GlobalAggregateMapper<VV> implements KeyValueMapper<Short, VV, Iterable<KeyValue<Short, VV>>> {
        VV previousValue;

        public GlobalAggregateMapper() {
            previousValue = null;
        }

        @Override
        public Iterable<KeyValue<Short, VV>> apply(Short key, VV vv) {
            if (!vv.equals(previousValue)) {
                previousValue = vv;
                return Collections.singletonList(new KeyValue<>(GLOBAL_KEY, vv));
            }
            return Collections.emptyList();
        }
    }
    /**
     * Builds the neighborhood state by creating adjacency lists.
     *
     * @param directed if true, only the out-neighbors will be stored
     *                 otherwise both directions are considered
     * @return a stream of Tuple3, where the first 2 fields identify the edge processed
     * and the third field is the adjacency list that was updated by processing this edge.
     */
    @Override
    public KStream<Edge<K>, Set<K>> buildNeighborhood(boolean directed) {

        KStream<Edge<K>, EV> result = edges();
        if (!directed) {
            result = undirected().edges();
        }
        return result.map(new BuildNeighborhoods<K, EV>());
    }


    private static final class BuildNeighborhoods<K, EV>
        implements KeyValueMapper<Edge<K>, EV, KeyValue<Edge<K>, Set<K>>> {

        private final Map<K, Set<K>> neighborhoods = new HashMap<>();

        @Override
        public KeyValue<Edge<K>, Set<K>> apply(Edge<K> e, EV value) {
            Set<K> t = neighborhoods.computeIfAbsent(e.source(), k -> new TreeSet<>());
            t.add(e.target());
            return new KeyValue<>(e, t);
        }
    }

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
    @Override
    public KGraphWindowedStream<K, EV> slice(Windows<? extends Window> size) {
        return slice(size, EdgeDirection.OUT);
    }


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
    @Override
    public KGraphWindowedStream<K, EV> slice(Windows<? extends Window> size, EdgeDirection direction)
        throws IllegalArgumentException {

        switch (direction) {
            case IN:
                return new KGraphWindowedStream<K, EV>(
                    reverse().edges()
                        .mapValues(EdgeWithValue::new)
                        .groupBy(new NeighborKeySelector<K, EV>())
                        .windowedBy(size));
            case OUT:
                return new KGraphWindowedStream<K, EV>(
                    edges()
                        .mapValues(EdgeWithValue::new)
                        .groupBy(new NeighborKeySelector<K, EV>())
                        .windowedBy(size));
            case BOTH:
                return new KGraphWindowedStream<K, EV>(
                    undirected().edges()
                        .mapValues(EdgeWithValue::new)
                        .groupBy(new NeighborKeySelector<K, EV>())
                        .windowedBy(size));
            default:
                throw new IllegalArgumentException("Illegal edge direction");
        }
    }

    private static final class NeighborKeySelector<K, EV>
        implements KeyValueMapper<Edge<K>, EdgeWithValue<K, EV>, K> {

        public NeighborKeySelector() {
        }

        @Override
        public K apply(Edge<K> edge, EdgeWithValue<K, EV> value) {
            return edge.source();
        }
    }

    /**
     * Applies an incremental aggregation on a graphstream and returns a stream of aggregation results
     *
     * @param summaryAggregation the summary aggregation
     * @param <S> initial type
     * @param <T> result type
     * @return the aggregation results
     */
    @Override
    public <S, T> KTable<Windowed<Short>, T> aggregate(SummaryAggregation<K, EV, S, T> summaryAggregation) {
        return summaryAggregation.run(edges());
    }
}
