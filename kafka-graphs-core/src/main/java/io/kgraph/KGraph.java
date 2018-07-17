package io.kgraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.state.KeyValueStore;

import io.kgraph.utils.KryoSerde;
import io.vavr.Tuple2;

public class KGraph<K, VV, EV> {

    private static final String STORE_PREFIX = "KGRAPH-STATE-STORE-";

    private final KTable<K, VV> vertices;
    private final KTable<Edge<K>, EV> edges;
    private final GraphSerialized<K, VV, EV> serialized;

    public KGraph(KTable<K, VV> vertices, KTable<Edge<K>, EV> edges,
                  GraphSerialized<K, VV, EV> serialized) {
        this.vertices = vertices;
        this.edges = edges;
        this.serialized = serialized;
    }

    public KTable<K, VV> vertices() {
        return vertices;
    }

    public KTable<Edge<K>, EV> edges() {
        return edges;
    }

    public GraphSerialized<K, VV, EV> serialized() {
        return serialized;
    }

    public Serde<K> keySerde() {
        return serialized.keySerde();
    }

    public Serde<VV> vertexValueSerde() {
        return serialized.vertexValueSerde();
    }

    public Serde<EV> edgeValueSerde() {
        return serialized.edgeValueSerde();
    }

    public KStream<K, EdgeWithValue<K, EV>> edgesBySource() {
        return edgesBy(Edge::source);
    }

    public KStream<K, EdgeWithValue<K, EV>> edgesByTarget() {
        return edgesBy(Edge::target);
    }

    private KStream<K, EdgeWithValue<K, EV>> edgesBy(Function<Edge<K>, K> fun) {
        return edges
            .toStream()
            .map((edge, ev) -> new KeyValue<>(fun.apply(edge), new EdgeWithValue<>(edge, ev)));
    }

    public KTable<K, Iterable<EdgeWithValue<K, EV>>> edgesGroupedBySource() {
        return edgesGroupedBy(Edge::source);
    }

    public KTable<K, Iterable<EdgeWithValue<K, EV>>> edgesGroupedByTarget() {
        return edgesGroupedBy(Edge::target);
    }

    private KTable<K, Iterable<EdgeWithValue<K, EV>>> edgesGroupedBy(Function<Edge<K>, K> fun) {
        return edges()
            .groupBy(new GroupEdges(fun), Serialized.with(keySerde(), new KryoSerde<>()))
            .aggregate(
                HashSet::new,
                (aggKey, value, aggregate) -> {
                    ((Set<EdgeWithValue<K, EV>>) aggregate).add(value);
                    return aggregate;
                },
                (aggKey, value, aggregate) -> {
                    ((Set<EdgeWithValue<K, EV>>) aggregate).remove(value);
                    return aggregate;
                },
                Materialized.with(keySerde(), new KryoSerde<>()));
    }

    private final class GroupEdges implements KeyValueMapper<Edge<K>, EV, KeyValue<K, EdgeWithValue<K, EV>>> {

        private final Function<Edge<K>, K> fun;

        GroupEdges(Function<Edge<K>, K> fun) {
            this.fun = fun;
        }

        @Override
        public KeyValue<K, EdgeWithValue<K, EV>> apply(Edge<K> edge, EV value) {
            return new KeyValue<>(fun.apply(edge), new EdgeWithValue<>(edge, value));
        }
    }

    public static <K, VV, EV> KGraph<K, VV, EV> fromEdges(
        KTable<Edge<K>, EV> edges,
        ValueMapper<K, VV> vertexValueInitializer,
        GraphSerialized<K, VV, EV> serialized) {

        KTable<K, VV> vertices = edges
            .toStream()
            .flatMap(new EmitSrcAndTargetAsTuple1<>(vertexValueInitializer))
            .groupByKey(Serialized.with(serialized.keySerde(), new KryoSerde<>()))
            .<VV>reduce((v1, v2) -> v2,
                Materialized.with(serialized.keySerde(), serialized.vertexValueSerde()));

        return new KGraph<>(vertices, edges, serialized);
    }

    private static final class EmitSrcAndTargetAsTuple1<K, VV, EV>
        implements KeyValueMapper<Edge<K>, EV, Iterable<KeyValue<K, VV>>> {

        private final ValueMapper<K, VV> vertexValueInitializer;

        public EmitSrcAndTargetAsTuple1(ValueMapper<K, VV> vertexValueInitializer) {
            this.vertexValueInitializer = vertexValueInitializer;
        }

        @Override
        public Iterable<KeyValue<K, VV>> apply(Edge<K> edge, EV value) {
            List<KeyValue<K, VV>> result = new ArrayList<>();
            result.add(new KeyValue<>(edge.source(), vertexValueInitializer.apply(edge.source())));
            result.add(new KeyValue<>(edge.target(), vertexValueInitializer.apply(edge.target())));
            return result;
        }
    }

    public <NV> KGraph<K, NV, EV> mapVertices(ValueMapperWithKey<K, VV, NV> mapper, Serde<NV> newVertexValueSerde) {
        KTable<K, NV> mappedVertices = vertices.mapValues(mapper, Materialized.with(keySerde(), newVertexValueSerde));
        return new KGraph<>(mappedVertices, edges,
            GraphSerialized.with(keySerde(), newVertexValueSerde, edgeValueSerde()));
    }

    public <NV> KGraph<K, VV, NV> mapEdges(ValueMapperWithKey<Edge<K>, EV, NV> mapper, Serde<NV> newEdgeValueSerde) {
        KTable<Edge<K>, NV> mappedEdges = edges.mapValues(mapper, Materialized.with(new KryoSerde<>(),
            newEdgeValueSerde));
        return new KGraph<>(vertices, mappedEdges,
            GraphSerialized.with(keySerde(), vertexValueSerde(), newEdgeValueSerde));
    }

    public <T> KGraph<K, VV, EV> joinWithVertices(KTable<K, T> inputDataSet,
                                                  final VertexJoinFunction<VV, T> vertexJoinFunction) {

        KTable<K, VV> resultedVertices = vertices()
            .leftJoin(inputDataSet,
                new ApplyLeftJoinToVertexValues<>(vertexJoinFunction), Materialized.with(keySerde(), vertexValueSerde()));
        return new KGraph<>(resultedVertices, edges, serialized);
    }

    private static final class ApplyLeftJoinToVertexValues<K, VV, T>
        implements ValueJoiner<VV, T, VV> {

        private final VertexJoinFunction<VV, T> vertexJoinFunction;

        public ApplyLeftJoinToVertexValues(VertexJoinFunction<VV, T> mapper) {
            this.vertexJoinFunction = mapper;
        }

        @Override
        public VV apply(VV value, T input) {
            if (value != null) {
                if (input != null) {
                    return vertexJoinFunction.vertexJoin(value, input);
                } else {
                    return value;
                }
            }
            return null;
        }
    }

    public <T> KGraph<K, VV, EV> joinWithEdges(KTable<Edge<K>, T> inputDataSet,
                                               final EdgeJoinFunction<EV, T> edgeJoinFunction) {

        KTable<Edge<K>, EV> resultedEdges = edges()
            .leftJoin(inputDataSet,
                new ApplyLeftJoinToEdgeValues<>(edgeJoinFunction), Materialized.with(new KryoSerde<>(), edgeValueSerde()));
        return new KGraph<>(vertices, resultedEdges, serialized);
    }

    private static final class ApplyLeftJoinToEdgeValues<K, EV, T>
        implements ValueJoiner<EV, T, EV> {

        private final EdgeJoinFunction<EV, T> edgeJoinFunction;

        public ApplyLeftJoinToEdgeValues(EdgeJoinFunction<EV, T> mapper) {
            this.edgeJoinFunction = mapper;
        }

        @Override
        public EV apply(EV value, T input) {
            if (value != null) {
                if (input != null) {
                    return edgeJoinFunction.edgeJoin(value, input);
                } else {
                    return value;
                }
            }
            return null;
        }
    }

    public <T> KGraph<K, VV, EV> joinWithEdgesOnSource(KTable<K, T> inputDataSet,
                                                       final EdgeJoinFunction<EV, T> edgeJoinFunction) {

        KTable<Edge<K>, EV> resultedEdges = edgesGroupedBySource()
            .leftJoin(inputDataSet,
                new ApplyLeftJoinToEdgeValuesOnEitherSourceOrTarget<>(edgeJoinFunction),
                Materialized.with(keySerde(), new KryoSerde<>()))
            .toStream()
            .flatMap((k, edgeWithValues) -> {
                List<KeyValue<Edge<K>, EV>> edges = new ArrayList<>();
                for (EdgeWithValue<K, EV> edge : edgeWithValues) {
                    edges.add(new KeyValue<>(new Edge<>(edge.source(), edge.target()), edge.value()));
                }
                return edges;
            })
            .groupByKey(Serialized.with(new KryoSerde<>(), edgeValueSerde()))
            .<EV>reduce((v1, v2) -> v2, Materialized.with(new KryoSerde<>(), edgeValueSerde()));

        return new KGraph<>(this.vertices, resultedEdges, serialized);
    }

    public <T> KGraph<K, VV, EV> joinWithEdgesOnTarget(KTable<K, T> inputDataSet,
                                                       final EdgeJoinFunction<EV, T> edgeJoinFunction) {

        KTable<Edge<K>, EV> resultedEdges = edgesGroupedByTarget()
            .leftJoin(inputDataSet,
                new ApplyLeftJoinToEdgeValuesOnEitherSourceOrTarget<>(edgeJoinFunction), Materialized.with(keySerde()
                    , new KryoSerde<>()))
            .toStream()
            .flatMap((k, edgeWithValues) -> {
                List<KeyValue<Edge<K>, EV>> edges = new ArrayList<>();
                for (EdgeWithValue<K, EV> edge : edgeWithValues) {
                    edges.add(new KeyValue<>(new Edge<>(edge.source(), edge.target()), edge.value()));
                }
                return edges;
            })
            .groupByKey(Serialized.with(new KryoSerde<>(), edgeValueSerde()))
            .<EV>reduce((v1, v2) -> v2, Materialized.with(new KryoSerde<>(), edgeValueSerde()));

        return new KGraph<>(vertices, resultedEdges, serialized);
    }

    private static final class ApplyLeftJoinToEdgeValuesOnEitherSourceOrTarget<K, EV, T>
        implements ValueJoiner<Iterable<EdgeWithValue<K, EV>>, T, Iterable<EdgeWithValue<K, EV>>> {

        private final EdgeJoinFunction<EV, T> edgeJoinFunction;

        public ApplyLeftJoinToEdgeValuesOnEitherSourceOrTarget(EdgeJoinFunction<EV, T> mapper) {
            this.edgeJoinFunction = mapper;
        }

        @Override
        public Iterable<EdgeWithValue<K, EV>> apply(Iterable<EdgeWithValue<K, EV>> edges, T input) {

            Iterable<EdgeWithValue<K, EV>> edgesIter = edges != null ? edges : Collections.emptyList();

            if (input != null) {
                List<EdgeWithValue<K, EV>> result = new ArrayList<>();

                for (EdgeWithValue<K, EV> edge : edgesIter) {
                    EV value = edgeJoinFunction.edgeJoin(edge.value(), input);
                    result.add(new EdgeWithValue<>(edge.source(), edge.target(), value));
                }

                return result;
            } else {
                return edgesIter;
            }
        }
    }

    public KGraph<K, VV, EV> subgraph(Predicate<K, VV> vertexFilter, Predicate<Edge<K>, EV> edgeFilter) {
        KTable<K, VV> filteredVertices = vertices.filter(vertexFilter);

        KTable<Edge<K>, EV> remainingEdges = edgesBySource()
            .join(filteredVertices, (e, v) -> e, Joined.with(keySerde(), new KryoSerde<>(), vertexValueSerde()))
            .map((k, edge) -> new KeyValue<>(edge.target(), edge))
            .join(filteredVertices, (e, v) -> e, Joined.with(keySerde(), new KryoSerde<>(), vertexValueSerde()))
            .map((k, edge) -> new KeyValue<>(new Edge<>(edge.source(), edge.target()), edge.value()))
            .groupByKey(Serialized.with(new KryoSerde<>(), edgeValueSerde()))
            .reduce((v1, v2) -> v2, Materialized.with(new KryoSerde<>(), edgeValueSerde()));

        KTable<Edge<K>, EV> filteredEdges = remainingEdges
            .filter(edgeFilter, Materialized.<Edge<K>, EV, KeyValueStore<Bytes, byte[]>>as(generateStoreName()).withKeySerde(new KryoSerde<>()).withValueSerde(edgeValueSerde()));

        return new KGraph<>(filteredVertices, filteredEdges, serialized);
    }

    // TODO test
    public KGraph<K, VV, EV> filterOnVertices(Predicate<K, VV> vertexFilter) {
        KTable<K, VV> filteredVertices = vertices.filter(vertexFilter);

        KTable<Edge<K>, EV> remainingEdges = edgesBySource()
            .join(filteredVertices, (e, v) -> e, Joined.with(keySerde(), new KryoSerde<>(), vertexValueSerde()))
            .map((k, edge) -> new KeyValue<>(edge.target(), edge))
            .join(filteredVertices, (e, v) -> e, Joined.with(keySerde(), new KryoSerde<>(), vertexValueSerde()))
            .map((k, edge) -> new KeyValue<>(new Edge<>(edge.source(), edge.target()), edge.value()))
            .groupByKey(Serialized.with(new KryoSerde<>(), edgeValueSerde()))
            .reduce((v1, v2) -> v2, Materialized.with(new KryoSerde<>(), edgeValueSerde()));

        return new KGraph<>(filteredVertices, remainingEdges, serialized);
    }

    // TODO test
    public KGraph<K, VV, EV> filterOnEdges(Predicate<Edge<K>, EV> edgeFilter) {
        KTable<Edge<K>, EV> filteredEdges = edges
            .filter(edgeFilter, Materialized.with(new KryoSerde<>(), edgeValueSerde()));

        return new KGraph<>(vertices, filteredEdges, serialized);
    }

    public KTable<K, Long> outDegrees() {
        return vertices.leftJoin(edgesGroupedBySource(), new CountNeighborsLeftJoin<>(),
            Materialized.with(keySerde(), Serdes.Long()));
    }

    // TODO test
    public KTable<K, Long> inDegrees() {
        return vertices.leftJoin(edgesGroupedByTarget(), new CountNeighborsLeftJoin<>(),
            Materialized.with(keySerde(), Serdes.Long()));
    }

    private static final class CountNeighborsLeftJoin<K, VV, EV>
        implements ValueJoiner<VV, Iterable<EdgeWithValue<K, EV>>, Long> {

        @Override
        public Long apply(VV value, Iterable<EdgeWithValue<K, EV>> edges) {
            long count = 0;
            if (edges != null) {
                for (EdgeWithValue<K, EV> edge : edges) {
                    count++;
                }
            }
            return count;
        }
    }

    public KGraph<K, VV, EV> undirected() {

        KTable<Edge<K>, EV> undirectedEdges = edges
            .toStream()
            .flatMap(new RegularAndReversedEdgesMap<>())
            .groupByKey(Serialized.with(new KryoSerde<>(), serialized.edgeValueSerde()))
            .reduce((v1, v2) -> v2, Materialized.<Edge<K>, EV, KeyValueStore<Bytes, byte[]>>as(generateStoreName())
                .withKeySerde(new KryoSerde<>()).withValueSerde(serialized.edgeValueSerde()));

        return new KGraph<>(vertices, undirectedEdges, serialized);
    }

    private static final class RegularAndReversedEdgesMap<K, EV>
        implements KeyValueMapper<Edge<K>, EV, Iterable<KeyValue<Edge<K>, EV>>> {

        @Override
        public Iterable<KeyValue<Edge<K>, EV>> apply(Edge<K> edge, EV value) {
            List<KeyValue<Edge<K>, EV>> result = new ArrayList<>();
            result.add(new KeyValue<>(edge, value));
            result.add(new KeyValue<>(edge.reverse(), value));
            return result;
        }
    }

    public <T> KTable<K, T> groupReduceOnEdges(EdgesFunctionWithVertexValue<K, VV, EV, T> edgesFunction,
                                               EdgeDirection direction) throws IllegalArgumentException {

        switch (direction) {
            case IN:
                return vertices()
                    .leftJoin(edgesGroupedByTarget(),
                        new ApplyEdgeLeftJoinFunction<>(edgesFunction), Materialized.with(keySerde(), new KryoSerde<>()));
            case OUT:
                return vertices()
                    .leftJoin(edgesGroupedBySource(),
                        new ApplyEdgeLeftJoinFunction<>(edgesFunction), Materialized.with(keySerde(), new KryoSerde<>()));
            case BOTH:
                throw new UnsupportedOperationException();
            default:
                throw new IllegalArgumentException("Illegal edge direction");
        }
    }

    public <T> KTable<K, T> groupReduceOnNeighbors(NeighborsFunctionWithVertexValue<K, VV, EV, T> neighborsFunction,
                                                   EdgeDirection direction) throws IllegalArgumentException {
        switch (direction) {
            case IN:
                KStream<K, Tuple2<EdgeWithValue<K, EV>, VV>> edgesWithSources =
                    edgesBySource()
                        .join(vertices, Tuple2::new, Joined.with(keySerde(), new KryoSerde<>(), vertexValueSerde()));
                KTable<K, Map<EdgeWithValue<K, EV>, VV>> neighborsGroupedByTarget = edgesWithSources
                    .map(new MapNeighbors(EdgeWithValue::target))
                    .groupByKey(Serialized.with(keySerde(), new KryoSerde<>()))
                    .aggregate(
                        HashMap::new,
                        (aggKey, value, aggregate) -> {
                            aggregate.put(value._1, value._2);
                            return aggregate;
                        },
                        Materialized.with(keySerde(), new KryoSerde<>()));
                return vertices()
                    .leftJoin(neighborsGroupedByTarget,
                        new ApplyNeighborLeftJoinFunction<>(neighborsFunction), Materialized.with(keySerde(), new KryoSerde<>()));
            case OUT:
                KStream<K, Tuple2<EdgeWithValue<K, EV>, VV>> edgesWithTargets =
                    edgesByTarget()
                        .join(vertices, Tuple2::new, Joined.with(keySerde(), new KryoSerde<>(), vertexValueSerde()));
                KTable<K, Map<EdgeWithValue<K, EV>, VV>> neighborsGroupedBySource = edgesWithTargets
                    .map(new MapNeighbors(EdgeWithValue::source))
                    .groupByKey(Serialized.with(keySerde(), new KryoSerde<>()))
                    .aggregate(
                        HashMap::new,
                        (aggKey, value, aggregate) -> {
                            aggregate.put(value._1, value._2);
                            return aggregate;
                        },
                        Materialized.with(keySerde(), new KryoSerde<>()));
                return vertices()
                    .leftJoin(neighborsGroupedBySource,
                        new ApplyNeighborLeftJoinFunction<>(neighborsFunction), Materialized.with(keySerde(), new KryoSerde<>()));
            case BOTH:
                throw new UnsupportedOperationException();
            default:
                throw new IllegalArgumentException("Illegal edge direction");
        }
    }

    public KTable<K, EV> reduceOnEdges(Reducer<EV> reducer,
                                       EdgeDirection direction) throws IllegalArgumentException {
        switch (direction) {
            case IN:
                return edgesGroupedByTarget()
                    .mapValues(v -> {
                        EV result = null;
                        for (EdgeWithValue<K, EV> edge : v) {
                            result = result != null ? reducer.apply(result, edge.value()) : edge.value();
                        }
                        return result;
                    },
                    Materialized.<K, EV, KeyValueStore<Bytes, byte[]>>as(generateStoreName()).withKeySerde(keySerde()).withValueSerde(edgeValueSerde()));
            case OUT:
                return edgesGroupedBySource()
                    .mapValues(v -> {
                        EV result = null;
                        for (EdgeWithValue<K, EV> edge : v) {
                            result = result != null ? reducer.apply(result, edge.value()) : edge.value();
                        }
                        return result;
                    },
                    Materialized.<K, EV, KeyValueStore<Bytes, byte[]>>as(generateStoreName()).withKeySerde(keySerde()).withValueSerde(edgeValueSerde()));
            case BOTH:
                throw new UnsupportedOperationException();
            default:
                throw new IllegalArgumentException("Illegal edge direction");
        }
    }

    public KTable<K, VV> reduceOnNeighbors(Reducer<VV> reducer,
                                           EdgeDirection direction) throws IllegalArgumentException {
        switch (direction) {
            case IN:
                KStream<K, Tuple2<EdgeWithValue<K, EV>, VV>> edgesWithSources =
                    edgesBySource()
                        .join(vertices, Tuple2::new, Joined.with(keySerde(), new KryoSerde<>(), vertexValueSerde()));
                KTable<K, Map<EdgeWithValue<K, EV>, VV>> neighborsGroupedByTarget = edgesWithSources
                    .map(new MapNeighbors(EdgeWithValue::target))
                    .groupByKey(Serialized.with(keySerde(), new KryoSerde<>()))
                    .aggregate(
                        HashMap::new,
                        (aggKey, value, aggregate) -> {
                            aggregate.put(value._1, value._2);
                            return aggregate;
                        },
                        Materialized.with(keySerde(), new KryoSerde<>()));
                KTable<K, VV> neighborsReducedByTarget = neighborsGroupedByTarget
                    .mapValues(v -> v.values().stream().reduce(reducer::apply).orElse(null),
                        Materialized.<K, VV, KeyValueStore<Bytes, byte[]>>as(generateStoreName())
                            .withKeySerde(keySerde()).withValueSerde(vertexValueSerde()));
                return neighborsReducedByTarget;
            case OUT:
                KStream<K, Tuple2<EdgeWithValue<K, EV>, VV>> edgesWithTargets =
                    edgesByTarget()
                        .join(vertices, Tuple2::new, Joined.with(keySerde(), new KryoSerde<>(), vertexValueSerde()));
                KTable<K, Map<EdgeWithValue<K, EV>, VV>> neighborsGroupedBySource = edgesWithTargets
                    .map(new MapNeighbors(EdgeWithValue::source))
                    .groupByKey(Serialized.with(keySerde(), new KryoSerde<>()))
                    .aggregate(
                        HashMap::new,
                        (aggKey, value, aggregate) -> {
                            aggregate.put(value._1, value._2);
                            return aggregate;
                        },
                        Materialized.with(keySerde(), new KryoSerde<>()));
                KTable<K, VV> neighborsReducedBySource = neighborsGroupedBySource
                    .mapValues(v -> v.values().stream().reduce(reducer::apply).orElse(null),
                        Materialized.<K, VV, KeyValueStore<Bytes, byte[]>>as(generateStoreName())
                            .withKeySerde(keySerde()).withValueSerde(vertexValueSerde()));
                return neighborsReducedBySource;
            case BOTH:
                throw new UnsupportedOperationException();
            default:
                throw new IllegalArgumentException("Illegal edge direction");
        }
    }

    private static final class ApplyEdgeLeftJoinFunction<K, VV, EV, T>
        implements ValueJoiner<VV, Iterable<EdgeWithValue<K, EV>>, T> {

        private final EdgesFunctionWithVertexValue<K, VV, EV, T> function;

        public ApplyEdgeLeftJoinFunction(EdgesFunctionWithVertexValue<K, VV, EV, T> fun) {
            this.function = fun;
        }

        @Override
        public T apply(VV value, Iterable<EdgeWithValue<K, EV>> edges) {
            Iterable<EdgeWithValue<K, EV>> edgesIter = edges != null ? edges : Collections.emptyList();
            if (value != null) {
                return function.iterateEdges(value, edgesIter);
            } else {
                throw new NoSuchElementException("The edge src/trg id could not be found within the vertexIds");
            }
        }
    }

    private final class MapNeighbors implements KeyValueMapper<K, Tuple2<EdgeWithValue<K, EV>, VV>,
        KeyValue<K, Tuple2<EdgeWithValue<K, EV>, VV>>> {

        private final Function<EdgeWithValue<K, EV>, K> fun;

        MapNeighbors(Function<EdgeWithValue<K, EV>, K> fun) {
            this.fun = fun;
        }

        @Override
        public KeyValue<K, Tuple2<EdgeWithValue<K, EV>, VV>> apply(K key, Tuple2<EdgeWithValue<K, EV>, VV> value) {
            EdgeWithValue<K, EV> edge = value._1;
            VV vertexValue = value._2;
            return new KeyValue<>(fun.apply(edge), new Tuple2<>(edge, vertexValue));
        }
    }

    private static final class ApplyNeighborLeftJoinFunction<K, VV, EV, T>
        implements ValueJoiner<VV, Map<EdgeWithValue<K, EV>, VV>, T> {

        private final NeighborsFunctionWithVertexValue<K, VV, EV, T> function;

        public ApplyNeighborLeftJoinFunction(NeighborsFunctionWithVertexValue<K, VV, EV, T> fun) {
            this.function = fun;
        }

        @Override
        public T apply(VV value, Map<EdgeWithValue<K, EV>, VV> edges) {
            Map<EdgeWithValue<K, EV>, VV> edgesMap = edges != null ? edges : Collections.emptyMap();
            if (value != null) {
                return function.iterateNeighbors(value, edgesMap);
            } else {
                throw new NoSuchElementException("The edge src/trg id could not be found within the vertexIds");
            }
        }
    }

    private String generateStoreName() {
        String name = STORE_PREFIX + UUID.randomUUID().toString();
        return name;
    }
}
