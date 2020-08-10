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

package io.kgraph.pregel.aggregators;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
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
import io.kgraph.utils.GraphUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.KryoSerializer;
import io.kgraph.utils.StreamUtils;

public class AggregatorTest extends AbstractIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(AggregatorTest.class);

    GraphAlgorithm<Long, Long, Long, KTable<Long, Long>> algorithm;

    @Test
    public void testVertexCountToValue() throws Exception {
        String suffix = "";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            LongSerializer.class, new Properties()
        );
        KTable<Edge<Long>, Long> edges =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.Long(),
                TestGraphUtils.getTwoChains());
        KGraph<Long, Long, Long> graph = KGraph.fromEdges(edges, new InitVertices(),
            GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(
            builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        Map<TopicPartition, Long> offsets = state.get();

        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                Collections.emptyMap(), Optional.empty(), new VertexCountToValue<>());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<Long, Long>> paths = algorithm.run();
        paths.result().get();

        Map<Long, Long> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);

        Map<Long, Long> expectedResult = new HashMap<>();
        expectedResult.put(0L, 220021L);
        expectedResult.put(1L, 220021L);
        expectedResult.put(2L, 220021L);
        expectedResult.put(3L, 220021L);
        expectedResult.put(4L, 220021L);
        expectedResult.put(5L, 220021L);
        expectedResult.put(6L, 220021L);
        expectedResult.put(7L, 220021L);
        expectedResult.put(8L, 220021L);
        expectedResult.put(9L, 220021L);
        expectedResult.put(10L, 220021L);
        expectedResult.put(11L, 220021L);
        expectedResult.put(12L, 220021L);
        expectedResult.put(13L, 220021L);
        expectedResult.put(14L, 220021L);
        expectedResult.put(15L, 220021L);
        expectedResult.put(16L, 220021L);
        expectedResult.put(17L, 220021L);
        expectedResult.put(18L, 220021L);
        expectedResult.put(19L, 220021L);
        expectedResult.put(20L, 220021L);

        assertEquals(expectedResult, map);
    }

    @Test
    public void testVertexCountToValueFromFile() throws Exception {
        String suffix = "vertexFile";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            FloatSerializer.class, new Properties()
        );
        GraphUtils.edgesToTopic(GraphUtils.class.getResourceAsStream("/ratings.txt"),
            Long::parseLong,
            Long::parseLong,
            s -> 0L,
            new LongSerializer(),
            producerConfig, "initEdges-" + suffix, 10, (short) 1);
        KTable<Edge<Long>, Long> edges =
            builder.table("initEdges-" + suffix, Consumed.with(new KryoSerde<>(), Serdes.Long()),
                Materialized.with(new KryoSerde<>(), Serdes.Long()));
        KGraph<Long, Long, Long> graph = KGraph.fromEdges(edges, new InitVertices(),
            GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(
            builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 10, (short) 1);
        Map<TopicPartition, Long> offsets = state.get();

        Thread.sleep(2000);

        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 10, (short) 1,
                Collections.emptyMap(), Optional.empty(), new VertexCountToValue<>());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<Long, Long>> paths = algorithm.run();
        paths.result().get();

        Map<Long, Long> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.info("result: {}", map);

        assertEquals(10L * 100000L + 10L * 10000L + 2071L, map.values().iterator().next().longValue());
    }

    @Test
    public void testEdgeCountToValueFromFile() throws Exception {
        String suffix = "edgeFile";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            FloatSerializer.class, new Properties()
        );
        GraphUtils.edgesToTopic(GraphUtils.class.getResourceAsStream("/ratings.txt"),
            Long::parseLong,
            Long::parseLong,
            s -> 0L,
            new LongSerializer(),
            producerConfig, "initEdges-" + suffix, 10, (short) 1);
        KTable<Edge<Long>, Long> edges =
            builder.table("initEdges-" + suffix, Consumed.with(new KryoSerde<>(), Serdes.Long()),
                Materialized.with(new KryoSerde<>(), Serdes.Long()));
        KGraph<Long, Long, Long> graph = KGraph.fromEdges(edges, new InitVertices(),
            GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(
            builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 10, (short) 1);
        Map<TopicPartition, Long> offsets = state.get();

        Thread.sleep(2000);

        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 10, (short) 1,
                Collections.emptyMap(), Optional.empty(), new EdgeCountToValue<>());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<Long, Long>> paths = algorithm.run();
        paths.result().get();

        Map<Long, Long> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.info("result: {}", map);

        assertEquals(35494L, map.values().iterator().next().longValue());
    }

    @After
    public void tearDown() throws Exception {
        algorithm.close();
    }

    private static final class InitVertices implements ValueMapper<Long, Long> {
        @Override
        public Long apply(Long id) {
            return 0L;
        }
    }
}
