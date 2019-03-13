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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
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
import io.kgraph.pregel.PregelGraphAlgorithm;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.GraphUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.StreamUtils;

public class MultipleSourceShortestPathsTest extends AbstractIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(MultipleSourceShortestPathsTest.class);

    GraphAlgorithm<Long, Map<Long, Double>, Double, KTable<Long, Map<Long, Double>>> algorithm;

    @Test
    public void testMultipleSourceShortestPaths() throws Exception {
        String suffix = "";
        StreamsBuilder builder = new StreamsBuilder();

        List<KeyValue<Edge<Long>, Double>> edges = new ArrayList<>();
        edges.add(new KeyValue<>(new Edge<>(1L, 2L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(1L, 5L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(2L, 3L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(2L, 5L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(3L, 4L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(4L, 5L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(4L, 6L), 1.0));

        edges.add(new KeyValue<>(new Edge<>(2L, 1L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(5L, 1L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(3L, 2L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(5L, 2L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(4L, 3L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(5L, 4L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(6L, 4L), 1.0));
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            DoubleSerializer.class, new Properties()
        );
        KTable<Edge<Long>, Double> table =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.Double(),
                edges);
        KGraph<Long, Map<Long, Double>, Double> graph = KGraph.fromEdges(table, new InitVertices(),
            GraphSerialized.with(Serdes.Long(), new KryoSerde<>(), Serdes.Double()));

        Properties props = ClientUtils.streamsConfig("prepare", "prepare-client", CLUSTER.bootstrapServers(),
            graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Void> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        state.get();

        Set<Long> landmarks = new HashSet<>();
        landmarks.add(1L);
        landmarks.add(4L);
        Map<String, Object> configs = new HashMap<>();
        configs.put(MultipleSourceShortestPaths.LANDMARK_VERTEX_IDS, landmarks);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run", CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, graph.serialized(),
                "solutionSet", "solutionSetStore", "workSet", 2, (short) 1,
                configs, Optional.empty(), new MultipleSourceShortestPaths());
        props = ClientUtils.streamsConfig("run", "run-client", CLUSTER.bootstrapServers(),
            graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), props).streams();
        GraphAlgorithmState<KTable<Long, Map<Long, Double>>> paths = algorithm.run();
        paths.result().get();

        Map<Long, Map<Long, Double>> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore");
        log.debug("result: {}", map);

        Map<Long, Map<Long, Double>> expectedResult = new HashMap<>();
        expectedResult.put(1L, new HashMap<Long, Double>() {{
            put(1L, 0.0);
            put(4L, 2.0);
        }});
        expectedResult.put(2L, new HashMap<Long, Double>() {{
            put(1L, 1.0);
            put(4L, 2.0);
        }});
        expectedResult.put(3L, new HashMap<Long, Double>() {{
            put(1L, 2.0);
            put(4L, 1.0);
        }});
        expectedResult.put(4L, new HashMap<Long, Double>() {{
            put(1L, 2.0);
            put(4L, 0.0);
        }});
        expectedResult.put(5L, new HashMap<Long, Double>() {{
            put(1L, 1.0);
            put(4L, 1.0);
        }});
        expectedResult.put(6L, new HashMap<Long, Double>() {{
            put(1L, 3.0);
            put(4L, 1.0);
        }});

        assertEquals(expectedResult, map);
    }

    @After
    public void tearDown() throws Exception {
        algorithm.close();
    }

    private static final class InitVertices implements ValueMapper<Long, Map<Long, Double>> {
        @Override
        public Map<Long, Double> apply(Long id) {
            return new HashMap<>();
        }
    }
}
