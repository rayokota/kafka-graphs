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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
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
import io.kgraph.TestGraphUtils;
import io.kgraph.pregel.PregelGraphAlgorithm;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.GraphGenerators;
import io.kgraph.utils.GraphUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.StreamUtils;
import io.vavr.Tuple2;

public class ConnectedComponentsTest extends AbstractIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(ConnectedComponentsTest.class);

    GraphAlgorithm<Long, Long, Long, KTable<Long, Long>> algorithm;

    @Test
    public void testConnectedComponents() throws Exception {
        String suffix = "cc";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            LongSerializer.class, new Properties()
        );
        KTable<Edge<Long>, Long> edges =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.Long(),
                TestGraphUtils.getTwoChains());
        KGraph<Long, Long, Long> graph = KGraph.fromEdges(edges, id -> id,
            GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        Map<TopicPartition, Long> offsets = state.get();

        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                Collections.emptyMap(), Optional.empty(), new ConnectedComponents<>());
        props = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), props).streams();
        GraphAlgorithmState<KTable<Long, Long>> paths = algorithm.run();
        paths.result().get();

        Thread.sleep(2000);

        Map<Long, Long> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);

        Map<Long, Long> expectedResult = new HashMap<>();
        expectedResult.put(0L, 0L);
        expectedResult.put(1L, 0L);
        expectedResult.put(2L, 0L);
        expectedResult.put(3L, 0L);
        expectedResult.put(4L, 0L);
        expectedResult.put(5L, 0L);
        expectedResult.put(6L, 0L);
        expectedResult.put(7L, 0L);
        expectedResult.put(8L, 0L);
        expectedResult.put(9L, 0L);
        expectedResult.put(10L, 10L);
        expectedResult.put(11L, 10L);
        expectedResult.put(12L, 10L);
        expectedResult.put(13L, 10L);
        expectedResult.put(14L, 10L);
        expectedResult.put(15L, 10L);
        expectedResult.put(16L, 10L);
        expectedResult.put(17L, 10L);
        expectedResult.put(18L, 10L);
        expectedResult.put(19L, 10L);
        expectedResult.put(20L, 10L);

        assertEquals(expectedResult, map);
    }

    @Test
    public void testGridConnectedComponents() throws Exception {
        String suffix = "grid";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            LongSerializer.class, new Properties()
        );
        KGraph<Long, Tuple2<Long, Long>, Long> gridGraph = GraphGenerators.gridGraph(builder, producerConfig, 10, 10);
        KTable<Long, Long> initialVertices = gridGraph.vertices().mapValues((id, v) -> id);
        KGraph<Long, Long, Long> graph = new KGraph<>(initialVertices, gridGraph.edges(),
            GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        Map<TopicPartition, Long> offsets = state.get();

        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                Collections.emptyMap(), Optional.empty(), new ConnectedComponents<>());
        props = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix, CLUSTER.bootstrapServers(),
            graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), props).streams();
        GraphAlgorithmState<KTable<Long, Long>> paths = algorithm.run();
        paths.result().get();

        Thread.sleep(2000);

        Map<Long, Long> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);

        for (long i = 0; i < 100; i++) {
            assertEquals(0L, map.get(i).longValue());
        }
    }

    //@Test
    public void testGridConnectedComponentsMultipleClients() throws Exception {
        // Run against a locally running Kafka broker
        String bootstrapServers = "localhost:9092";
        String zookeeperConnect = "localhost:2181";
        String suffix = "grid-" + ClientUtils.generateRandomString(8);
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(bootstrapServers, LongSerializer.class,
            LongSerializer.class, new Properties()
        );
        KGraph<Long, Tuple2<Long, Long>, Long> gridGraph = GraphGenerators.gridGraph(builder, producerConfig, 10, 10);
        KTable<Long, Long> initialVertices = gridGraph.vertices().mapValues((id, v) -> id);
        KGraph<Long, Long, Long> graph = new KGraph<>(initialVertices, gridGraph.edges(),
            GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            bootstrapServers, graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        Map<TopicPartition, Long> offsets = state.get();

        GraphAlgorithm<Long, Long, Long, KTable<Long, Long>> algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, bootstrapServers,
                zookeeperConnect, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                Collections.emptyMap(), Optional.empty(), new ConnectedComponents<>());
        GraphAlgorithm<Long, Long, Long, KTable<Long, Long>> algorithm2 =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, bootstrapServers,
                zookeeperConnect, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                Collections.emptyMap(), Optional.empty(), new ConnectedComponents<>());
        props = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix, bootstrapServers,
            graph.keySerde().getClass(), KryoSerde.class);
        algorithm.configure(new StreamsBuilder(), props);
        // Need separate STATE_DIR_CONFIG
        props.put(StreamsConfig.STATE_DIR_CONFIG, ClientUtils.tempDirectory().getAbsolutePath());
        KafkaStreams streams = algorithm2.configure(new StreamsBuilder(), props).streams();
        GraphAlgorithmState<KTable<Long, Long>> paths = algorithm.run(Integer.MAX_VALUE);
        GraphAlgorithmState<KTable<Long, Long>> paths2 = algorithm2.run(Integer.MAX_VALUE);
        paths.result().get();
        paths2.result().get();

        Thread.sleep(2000);

        log.debug("getting result");
        Map<Long, Long> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        Map<Long, Long> map2 = StreamUtils.mapFromStore(paths2.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);
        log.debug("result: {}", map2);

        for (long i = 0; i < 100; i++) {
            Long value = map.get(i);
            if (value == null) value = map2.get(i);
            assertEquals(0L, value.longValue());
        }

        algorithm.close();
        algorithm2.close();
    }

    //@Test
    public void testConnectedComponentsMultipleClientsFromFile() throws Exception {
        // Run against a locally running Kafka broker
        String bootstrapServers = "localhost:9092";
        String zookeeperConnect = "localhost:2181";
        String suffix = "file-" + ClientUtils.generateRandomString(8);
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(bootstrapServers, LongSerializer.class,
            LongSerializer.class, new Properties()
        );
        GraphUtils.verticesToTopic(GraphUtils.class.getResourceAsStream("/vertices.txt"),
            Long::parseLong, Long::parseLong, new LongSerializer(), new LongSerializer(),
            producerConfig, "initVertices-" + suffix, 50, (short) 1);
        GraphUtils.edgesToTopic(GraphUtils.class.getResourceAsStream("/edges.txt"),
            Long::parseLong, Long::parseLong, Long::parseLong,
            new LongSerializer(),
            producerConfig, "initEdges-" + suffix, 50, (short) 1);
        KGraph<Long, Long, Long> graph = new KGraph<>(
            builder.table("initVertices-" + suffix, Consumed.with(Serdes.Long(), Serdes.Long())),
            builder.table("initEdges-"+ suffix, Consumed.with(new KryoSerde<>(), Serdes.Long())),
            GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            bootstrapServers, graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        Map<TopicPartition, Long> offsets = state.get();

        GraphAlgorithm<Long, Long, Long, KTable<Long, Long>> algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, bootstrapServers,
                zookeeperConnect, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                Collections.emptyMap(), Optional.empty(), new ConnectedComponents<>());
        GraphAlgorithm<Long, Long, Long, KTable<Long, Long>> algorithm2 =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, bootstrapServers,
                zookeeperConnect, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                Collections.emptyMap(), Optional.empty(), new ConnectedComponents<>());
        props = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix, bootstrapServers,
            graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), props).streams();
        // Need separate STATE_DIR_CONFIG
        props.put(StreamsConfig.STATE_DIR_CONFIG, ClientUtils.tempDirectory().getAbsolutePath());
        KafkaStreams streams2 = algorithm2.configure(new StreamsBuilder(), props).streams();
        GraphAlgorithmState<KTable<Long, Long>> paths = algorithm.run();
        GraphAlgorithmState<KTable<Long, Long>> paths2 = algorithm2.run();
        paths.result().get();
        paths2.result().get();

        Thread.sleep(2000);

        log.debug("getting result");
        Map<Long, Long> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        Map<Long, Long> map2 = StreamUtils.mapFromStore(paths2.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);
        log.debug("result: {}", map2);

        for (long i = 0; i < 10; i++) {
            Long value = map.get(i);
            if (value == null) value = map2.get(i);
            assertEquals(0L, value.longValue());
        }
        for (long i = 10; i < 21; i++) {
            Long value = map.get(i);
            if (value == null) value = map2.get(i);
            assertEquals(10L, value.longValue());
        }

        algorithm.close();
        algorithm2.close();
    }

    @After
    public void tearDown() throws Exception {
        if (algorithm != null) {
            algorithm.close();
        }
    }
}
