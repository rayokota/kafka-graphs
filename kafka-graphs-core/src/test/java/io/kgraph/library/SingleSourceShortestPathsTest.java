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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
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
import io.kgraph.TestGraphUtils;
import io.kgraph.pregel.PregelGraphAlgorithm;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.GraphUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.StreamUtils;

public class SingleSourceShortestPathsTest extends AbstractIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(SingleSourceShortestPathsTest.class);

    GraphAlgorithm<Long, Double, Double, KTable<Long, Double>> algorithm;

    @Test
    public void testSingleSourceShortestPaths() throws Exception {
        String suffix = "";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            DoubleSerializer.class, new Properties()
        );
        KTable<Edge<Long>, Double> edges =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.Double(),
                TestGraphUtils.getLongDoubleEdges());
        KGraph<Long, Double, Double> graph = KGraph.fromEdges(edges,
            GraphAlgorithmType.initialVertexValueMapper(GraphAlgorithmType.sssp),
            GraphSerialized.with(Serdes.Long(), Serdes.Double(), Serdes.Double()));

        Properties props = ClientUtils.streamsConfig("prepare", "prepare-client", CLUSTER.bootstrapServers(),
            graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        Map<TopicPartition, Long> offsets = state.get();

        Map<String, Object> configs = new HashMap<>();
        configs.put(SingleSourceShortestPaths.SRC_VERTEX_ID, 1L);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run", CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet", "solutionSetStore", "workSet", 2, (short) 1,
                configs, Optional.empty(), new SingleSourceShortestPaths());
        props = ClientUtils.streamsConfig("run", "run-client", CLUSTER.bootstrapServers(),
            graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), props).streams();
        GraphAlgorithmState<KTable<Long, Double>> paths = algorithm.run();
        paths.result().get();

        Map<Long, Double> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore");
        log.debug("result: {}", map);

        Map<Long, Double> expectedResult = new HashMap<>();
        expectedResult.put(1L, 0.0);
        expectedResult.put(2L, 12.0);
        expectedResult.put(3L, 13.0);
        expectedResult.put(4L, 47.0);
        expectedResult.put(5L, 48.0);

        assertEquals(expectedResult, map);
    }

    @After
    public void tearDown() throws Exception {
        algorithm.close();
    }
}
