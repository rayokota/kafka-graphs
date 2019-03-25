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

package io.kgraph.library.clustering;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kgraph.AbstractIntegrationTest;
import io.kgraph.Edge;
import io.kgraph.GraphAlgorithm;
import io.kgraph.GraphAlgorithmState;
import io.kgraph.GraphSerialized;
import io.kgraph.KGraph;
import io.kgraph.pregel.PregelGraphAlgorithm;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.KryoSerializer;
import io.kgraph.utils.StreamUtils;

public class KMeansClusteringTest extends AbstractIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(KMeansClusteringTest.class);

    GraphAlgorithm<Long, KMeansVertexValue, Long, KTable<Long, KMeansVertexValue>> algorithm;

    @Test
    public void test1() throws Exception {
        String suffix = "1";
        StreamsBuilder builder = new StreamsBuilder();

        List<KeyValue<Long, KMeansVertexValue>> list = new ArrayList<>();
        list.add(new KeyValue<>(1L, new KMeansVertexValue(Stream.of(1.0, 1.0).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(2L, new KMeansVertexValue(Stream.of(1.5, 2.0).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(3L, new KMeansVertexValue(Stream.of(3.0, 4.0).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(4L, new KMeansVertexValue(Stream.of(5.0, 7.0).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(5L, new KMeansVertexValue(Stream.of(3.5, 5.0).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(6L, new KMeansVertexValue(Stream.of(4.5, 5.0).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(7L, new KMeansVertexValue(Stream.of(3.5, 4.5).collect(Collectors.toList()), 0)));
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            KryoSerializer.class, new Properties()
        );
        KTable<Long, KMeansVertexValue> vertices =
            StreamUtils.tableFromCollection(builder, producerConfig, "vertices-" + suffix, 2, (short) 1, Serdes.Long(), new KryoSerde<>(), list);
        Properties producerConfig2 = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            LongSerializer.class, new Properties()
        );
        // Empty edges
        KTable<Edge<Long>, Long> edges =
            StreamUtils.tableFromCollection(builder, producerConfig2, new KryoSerde<>(), Serdes.Long(), Collections.emptyList());
        ClientUtils.createTopic("edgesGroupedBySource-" + suffix, 2, (short) 1, producerConfig2);
        KGraph<Long, KMeansVertexValue, Long> graph = new KGraph<>(vertices, edges,
            GraphSerialized.with(Serdes.Long(), new KryoSerde<>(), Serdes.Long()));

        Map<String, Object> configs = new HashMap<>();
        List<List<Double>> testInitialCenters = new ArrayList<>();
        List<Double> c1 = Stream.of(1.0, 1.0).collect(Collectors.toList());
        List<Double> c2 = Stream.of(1.5, 2.0).collect(Collectors.toList());
        testInitialCenters.add(c1);
        testInitialCenters.add(c2);
        configs.put(KMeansClustering.TEST_INITIAL_CENTERS, testInitialCenters);
        configs.put(KMeansClustering.CLUSTER_CENTERS_COUNT, 2);
        configs.put(KMeansClustering.DIMENSIONS, 2);
        configs.put(KMeansClustering.POINTS_COUNT, 7);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix,
                Collections.emptyMap(), graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                configs, Optional.empty(), new KMeansClustering<>());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<Long, KMeansVertexValue>> paths = algorithm.run();
        paths.result().get();

        Thread.sleep(2000);

        Map<Long, KMeansVertexValue> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);

        assertEquals("{1=0, 2=0, 3=1, 4=1, 5=1, 6=1, 7=1}", map.toString());
    }

    @Test
    public void test2() throws Exception {
        String suffix = "2";
        StreamsBuilder builder = new StreamsBuilder();

        List<KeyValue<Long, KMeansVertexValue>> list = new ArrayList<>();
        list.add(new KeyValue<>(1L, new KMeansVertexValue(Stream.of(2.0, 10.0).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(2L, new KMeansVertexValue(Stream.of(2.0, 5.0).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(3L, new KMeansVertexValue(Stream.of(8.0, 4.0).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(4L, new KMeansVertexValue(Stream.of(5.0, 8.0).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(5L, new KMeansVertexValue(Stream.of(7.0, 5.0).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(6L, new KMeansVertexValue(Stream.of(6.0, 4.0).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(7L, new KMeansVertexValue(Stream.of(1.0, 2.0).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(8L, new KMeansVertexValue(Stream.of(4.0, 9.0).collect(Collectors.toList()), 0)));
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            KryoSerializer.class, new Properties()
        );
        KTable<Long, KMeansVertexValue> vertices =
            StreamUtils.tableFromCollection(builder, producerConfig, "vertices-" + suffix, 2, (short) 1, Serdes.Long(), new KryoSerde<>(), list);
        Properties producerConfig2 = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            LongSerializer.class, new Properties()
        );
        // Empty edges
        KTable<Edge<Long>, Long> edges =
            StreamUtils.tableFromCollection(builder, producerConfig2, new KryoSerde<>(), Serdes.Long(), Collections.emptyList());
        ClientUtils.createTopic("edgesGroupedBySource-" + suffix, 2, (short) 1, producerConfig2);
        KGraph<Long, KMeansVertexValue, Long> graph = new KGraph<>(vertices, edges,
            GraphSerialized.with(Serdes.Long(), new KryoSerde<>(), Serdes.Long()));

        Map<String, Object> configs = new HashMap<>();
        List<List<Double>> testInitialCenters = new ArrayList<>();
        List<Double> c1 = Stream.of(2.0, 10.0).collect(Collectors.toList());
        List<Double> c2 = Stream.of(2.0, 5.0).collect(Collectors.toList());
        List<Double> c3 = Stream.of(8.0, 4.0).collect(Collectors.toList());
        testInitialCenters.add(c1);
        testInitialCenters.add(c2);
        testInitialCenters.add(c3);
        configs.put(KMeansClustering.TEST_INITIAL_CENTERS, testInitialCenters);
        configs.put(KMeansClustering.CLUSTER_CENTERS_COUNT, 3);
        configs.put(KMeansClustering.DIMENSIONS, 2);
        configs.put(KMeansClustering.POINTS_COUNT, 8);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix,
                Collections.emptyMap(), graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                configs, Optional.empty(), new KMeansClustering<>());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<Long, KMeansVertexValue>> paths = algorithm.run();
        paths.result().get();

        Map<Long, KMeansVertexValue> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);

        assertEquals("{1=0, 2=1, 3=2, 4=0, 5=2, 6=2, 7=1, 8=0}", map.toString());
    }

    @Test
    public void test3() throws Exception {
        String suffix = "3";
        StreamsBuilder builder = new StreamsBuilder();

        List<KeyValue<Long, KMeansVertexValue>> list = new ArrayList<>();
        list.add(new KeyValue<>(1L, new KMeansVertexValue(Stream.of(-4.31568, -0.396959, -6.29507).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(2L, new KMeansVertexValue(Stream.of(-4.56112, -1.74917, -4.57874).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(3L, new KMeansVertexValue(Stream.of(4.54508, 0.102845, 6.35385).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(4L, new KMeansVertexValue(Stream.of(4.87746, -0.832591, 7.06942).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(5L, new KMeansVertexValue(Stream.of(-5.91254, -0.278006, -4.25934).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(6L, new KMeansVertexValue(Stream.of(6.95139, 0.120139, 4.89531).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(7L, new KMeansVertexValue(Stream.of(-6.28538, -0.88527, -4.74988).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(8L, new KMeansVertexValue(Stream.of(-6.84791, 0.887664, -4.91919).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(9L, new KMeansVertexValue(Stream.of(7.47117, 1.67911, 6.02221).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(10L, new KMeansVertexValue(Stream.of(-4.78011, 1.2099, -4.55519).collect(Collectors.toList()), 0)));
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            KryoSerializer.class, new Properties()
        );
        KTable<Long, KMeansVertexValue> vertices =
            StreamUtils.tableFromCollection(builder, producerConfig, "vertices-" + suffix, 2, (short) 1, Serdes.Long(), new KryoSerde<>(), list);
        Properties producerConfig2 = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            LongSerializer.class, new Properties()
        );
        // Empty edges
        KTable<Edge<Long>, Long> edges =
            StreamUtils.tableFromCollection(builder, producerConfig2, new KryoSerde<>(), Serdes.Long(), Collections.emptyList());
        ClientUtils.createTopic("edgesGroupedBySource-" + suffix, 2, (short) 1, producerConfig2);
        KGraph<Long, KMeansVertexValue, Long> graph = new KGraph<>(vertices, edges,
            GraphSerialized.with(Serdes.Long(), new KryoSerde<>(), Serdes.Long()));

        Map<String, Object> configs = new HashMap<>();
        List<List<Double>> testInitialCenters = new ArrayList<>();
        List<Double> c1 = Stream.of(-4.31568, -0.396959, -6.29507).collect(Collectors.toList());
        List<Double> c2 = Stream.of(-4.56112, -1.74917, -4.57874).collect(Collectors.toList());
        testInitialCenters.add(c1);
        testInitialCenters.add(c2);
        configs.put(KMeansClustering.TEST_INITIAL_CENTERS, testInitialCenters);
        configs.put(KMeansClustering.CLUSTER_CENTERS_COUNT, 2);
        configs.put(KMeansClustering.DIMENSIONS, 3);
        configs.put(KMeansClustering.POINTS_COUNT, 10);
        configs.put(KMeansClustering.PRINT_FINAL_CENTERS, true);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix,
                Collections.emptyMap(), graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                configs, Optional.empty(), new KMeansClustering<>());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<Long, KMeansVertexValue>> paths = algorithm.run();
        paths.result().get();

        Map<Long, KMeansVertexValue> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);

        assertEquals("{1=0, 2=0, 3=1, 4=1, 5=0, 6=1, 7=0, 8=0, 9=1, 10=0}", map.toString());
    }

    @Test
    public void test4() throws Exception {
        String suffix = "4";
        StreamsBuilder builder = new StreamsBuilder();

        List<KeyValue<Long, KMeansVertexValue>> list = new ArrayList<>();
        list.add(new KeyValue<>(1L, new KMeansVertexValue(Stream.of(-3.78, -42.01).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(2L, new KMeansVertexValue(Stream.of(-45.96, 30.67).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(3L, new KMeansVertexValue(Stream.of(56.37, -46.62).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(4L, new KMeansVertexValue(Stream.of(8.78, -37.95).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(5L, new KMeansVertexValue(Stream.of(-26.95, 43.10).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(6L, new KMeansVertexValue(Stream.of(37.87, -51.30).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(7L, new KMeansVertexValue(Stream.of(-2.61, -30.43).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(8L, new KMeansVertexValue(Stream.of(-23.33, 26.23).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(9L, new KMeansVertexValue(Stream.of(38.19, -36.27).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(10L, new KMeansVertexValue(Stream.of(-13.63, -42.26).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(11L, new KMeansVertexValue(Stream.of(-36.57, 32.63).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(12L, new KMeansVertexValue(Stream.of(50.65, -52.40).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(13L, new KMeansVertexValue(Stream.of(-5.76, -51.83).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(14L, new KMeansVertexValue(Stream.of(-34.43, 42.66).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(15L, new KMeansVertexValue(Stream.of(40.35, -47.14).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(16L, new KMeansVertexValue(Stream.of(-23.40, -48.70).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(17L, new KMeansVertexValue(Stream.of(-29.58, 17.77).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(18L, new KMeansVertexValue(Stream.of(43.08, -61.96).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(19L, new KMeansVertexValue(Stream.of(9.06, -49.26).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(20L, new KMeansVertexValue(Stream.of(-20.13, 44.16).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(21L, new KMeansVertexValue(Stream.of(41.62, -45.84).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(22L, new KMeansVertexValue(Stream.of(5.23, -41.20).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(23L, new KMeansVertexValue(Stream.of(-23.00, 38.15).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(24L, new KMeansVertexValue(Stream.of(44.55, -51.50).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(25L, new KMeansVertexValue(Stream.of(-15.63, -26.81).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(26L, new KMeansVertexValue(Stream.of(-24.33, 22.63).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(27L, new KMeansVertexValue(Stream.of(52.51, -54.75).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(28L, new KMeansVertexValue(Stream.of(-0.04, -39.69).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(29L, new KMeansVertexValue(Stream.of(-32.92, 43.87).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(30L, new KMeansVertexValue(Stream.of(47.99, -36.93).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(31L, new KMeansVertexValue(Stream.of(-7.34, -57.90).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(32L, new KMeansVertexValue(Stream.of(-36.17, 34.74).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(33L, new KMeansVertexValue(Stream.of(51.52, -41.83).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(34L, new KMeansVertexValue(Stream.of(-21.91, -49.01).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(35L, new KMeansVertexValue(Stream.of(-46.68, 46.04).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(36L, new KMeansVertexValue(Stream.of(48.52, -43.67).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(37L, new KMeansVertexValue(Stream.of(-0.20, -36.62).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(38L, new KMeansVertexValue(Stream.of(-27.71, 35.12).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(39L, new KMeansVertexValue(Stream.of(41.29, -42.00).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(40L, new KMeansVertexValue(Stream.of(-9.17, -43.28).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(41L, new KMeansVertexValue(Stream.of(-41.16, 50.66).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(42L, new KMeansVertexValue(Stream.of(49.63, -45.28).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(43L, new KMeansVertexValue(Stream.of(-8.10, -29.83).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(44L, new KMeansVertexValue(Stream.of(-49.38, 38.57).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(45L, new KMeansVertexValue(Stream.of(35.38, -34.90).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(46L, new KMeansVertexValue(Stream.of(-6.51, -55.58).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(47L, new KMeansVertexValue(Stream.of(-38.17, 40.21).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(48L, new KMeansVertexValue(Stream.of(47.47, -45.95).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(49L, new KMeansVertexValue(Stream.of(-17.66, -51.12).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(50L, new KMeansVertexValue(Stream.of(-32.60, 41.13).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(51L, new KMeansVertexValue(Stream.of(40.68, -49.10).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(52L, new KMeansVertexValue(Stream.of(-10.31, -40.69).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(53L, new KMeansVertexValue(Stream.of(-22.05, 42.91).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(54L, new KMeansVertexValue(Stream.of(51.16, -47.58).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(55L, new KMeansVertexValue(Stream.of(-12.42, -57.29).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(56L, new KMeansVertexValue(Stream.of(-17.72, 39.90).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(57L, new KMeansVertexValue(Stream.of(44.57, -41.75).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(58L, new KMeansVertexValue(Stream.of(3.14, -35.46).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(59L, new KMeansVertexValue(Stream.of(-53.73, 32.84).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(60L, new KMeansVertexValue(Stream.of(53.16, -50.16).collect(Collectors.toList()), 0)));
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            KryoSerializer.class, new Properties()
        );
        KTable<Long, KMeansVertexValue> vertices =
            StreamUtils.tableFromCollection(builder, producerConfig, "vertices-" + suffix, 2, (short) 1, Serdes.Long(), new KryoSerde<>(), list);
        Properties producerConfig2 = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            LongSerializer.class, new Properties()
        );
        // Empty edges
        KTable<Edge<Long>, Long> edges =
            StreamUtils.tableFromCollection(builder, producerConfig2, new KryoSerde<>(), Serdes.Long(), Collections.emptyList());
        ClientUtils.createTopic("edgesGroupedBySource-" + suffix, 2, (short) 1, producerConfig2);
        KGraph<Long, KMeansVertexValue, Long> graph = new KGraph<>(vertices, edges,
            GraphSerialized.with(Serdes.Long(), new KryoSerde<>(), Serdes.Long()));

        Map<String, Object> configs = new HashMap<>();
        List<List<Double>> testInitialCenters = new ArrayList<>();
        List<Double> c1 = Stream.of(-3.78, -42.01).collect(Collectors.toList());
        List<Double> c2 = Stream.of(-36.57, 32.63).collect(Collectors.toList());
        List<Double> c3 = Stream.of(50.65, -52.40).collect(Collectors.toList());
        testInitialCenters.add(c1);
        testInitialCenters.add(c2);
        testInitialCenters.add(c3);
        configs.put(KMeansClustering.TEST_INITIAL_CENTERS, testInitialCenters);
        configs.put(KMeansClustering.CLUSTER_CENTERS_COUNT, 3);
        configs.put(KMeansClustering.DIMENSIONS, 2);
        configs.put(KMeansClustering.POINTS_COUNT, 60);
        configs.put(KMeansClustering.PRINT_FINAL_CENTERS, true);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix,
                Collections.emptyMap(), graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                configs, Optional.empty(), new KMeansClustering<>());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<Long, KMeansVertexValue>> paths = algorithm.run();
        paths.result().get();

        Map<Long, KMeansVertexValue> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);

        assertEquals("{1=0, 2=1, 3=2, 4=0, 5=1, 6=2, 7=0, 8=1, 9=2, 10=0, 11=1, 12=2, 13=0, 14=1, 15=2, 16=0, 17=1, 18=2, 19=0, 20=1, 21=2, 22=0, 23=1, 24=2, 25=0, 26=1, 27=2, 28=0, 29=1, 30=2, 31=0, 32=1, 33=2, 34=0, 35=1, 36=2, 37=0, 38=1, 39=2, 40=0, 41=1, 42=2, 43=0, 44=1, 45=2, 46=0, 47=1, 48=2, 49=0, 50=1, 51=2, 52=0, 53=1, 54=2, 55=0, 56=1, 57=2, 58=0, 59=1, 60=2}", map.toString());
    }

    @Test
    public void test5() throws Exception {
        String suffix = "5";
        StreamsBuilder builder = new StreamsBuilder();

        List<KeyValue<Long, KMeansVertexValue>> list = new ArrayList<>();
        list.add(new KeyValue<>(1L, new KMeansVertexValue(Stream.of(-3.78, -42.01).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(2L, new KMeansVertexValue(Stream.of(-45.96, 30.67).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(3L, new KMeansVertexValue(Stream.of(56.37, -46.62).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(4L, new KMeansVertexValue(Stream.of(8.78, -37.95).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(5L, new KMeansVertexValue(Stream.of(-26.95, 43.10).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(6L, new KMeansVertexValue(Stream.of(37.87, -51.30).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(7L, new KMeansVertexValue(Stream.of(-2.61, -30.43).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(8L, new KMeansVertexValue(Stream.of(-23.33, 26.23).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(9L, new KMeansVertexValue(Stream.of(38.19, -36.27).collect(Collectors.toList()), 0)));
        list.add(new KeyValue<>(10L, new KMeansVertexValue(Stream.of(-13.63, -42.26).collect(Collectors.toList()), 0)));
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            KryoSerializer.class, new Properties()
        );
        KTable<Long, KMeansVertexValue> vertices =
            StreamUtils.tableFromCollection(builder, producerConfig, "vertices-" + suffix, 2, (short) 1, Serdes.Long(), new KryoSerde<>(), list);
        Properties producerConfig2 = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            LongSerializer.class, new Properties()
        );
        // Empty edges
        KTable<Edge<Long>, Long> edges =
            StreamUtils.tableFromCollection(builder, producerConfig2, new KryoSerde<>(), Serdes.Long(), Collections.emptyList());
        ClientUtils.createTopic("edgesGroupedBySource-" + suffix, 2, (short) 1, producerConfig2);
        KGraph<Long, KMeansVertexValue, Long> graph = new KGraph<>(vertices, edges,
            GraphSerialized.with(Serdes.Long(), new KryoSerde<>(), Serdes.Long()));

        Map<String, Object> configs = new HashMap<>();
        List<List<Double>> testInitialCenters = new ArrayList<>();
        List<Double> c1 = Stream.of(-3.78, -42.01).collect(Collectors.toList());
        List<Double> c2 = Stream.of(-26.95, 43.10).collect(Collectors.toList());
        List<Double> c3 = Stream.of(56.37, -46.62).collect(Collectors.toList());
        testInitialCenters.add(c1);
        testInitialCenters.add(c2);
        testInitialCenters.add(c3);
        configs.put(KMeansClustering.TEST_INITIAL_CENTERS, testInitialCenters);
        configs.put(KMeansClustering.CLUSTER_CENTERS_COUNT, 3);
        configs.put(KMeansClustering.DIMENSIONS, 2);
        configs.put(KMeansClustering.POINTS_COUNT, 10);
        configs.put(KMeansClustering.PRINT_FINAL_CENTERS, true);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix,
                Collections.emptyMap(), graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                configs, Optional.empty(), new KMeansClustering<>());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<Long, KMeansVertexValue>> paths = algorithm.run();
        paths.result().get();

        Map<Long, KMeansVertexValue> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);

        assertEquals("{1=0, 2=1, 3=2, 4=0, 5=1, 6=2, 7=0, 8=1, 9=2, 10=0}", map.toString());
    }
    @After
    public void tearDown() throws Exception {
        algorithm.close();
    }
}
