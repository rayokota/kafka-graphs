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

package io.kgraph.library.basic;

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
import io.kgraph.TestGraphUtils;
import io.kgraph.pregel.PregelGraphAlgorithm;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.GraphUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.StreamUtils;

public class ReverseEdgesTest extends AbstractIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(ReverseEdgesTest.class);

    GraphAlgorithm<Long, Long, Long, KTable<Long, Long>> algorithm;

    @Test
    public void testReverseEdges() throws Exception {
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
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        Map<TopicPartition, Long> offsets = state.get();

        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                Collections.emptyMap(), Optional.empty(), new ReverseEdges<>());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<Long, Long>> paths = algorithm.run();
        paths.result().get();

        Map<Long, Map<Long, Long>> map = StreamUtils.mapFromStore(paths.streams(), "edgesStore-run-" + suffix);
        log.debug("result: {}", map);

        Map<Long, Map<Long, Long>> expectedResult = new HashMap<>();
        expectedResult.put(0L, new HashMap<Long, Long>() {{ put(1L, 1L); }});
        expectedResult.put(1L, new HashMap<Long, Long>() {{ put(0L, 1L); put(2L, 1L); }});
        expectedResult.put(2L, new HashMap<Long, Long>() {{ put(1L, 1L); put(3L, 1L); }});
        expectedResult.put(3L, new HashMap<Long, Long>() {{ put(2L, 1L); put(4L, 1L); }});
        expectedResult.put(4L, new HashMap<Long, Long>() {{ put(3L, 1L); put(5L, 1L); }});
        expectedResult.put(5L, new HashMap<Long, Long>() {{ put(4L, 1L); put(6L, 1L); }});
        expectedResult.put(6L, new HashMap<Long, Long>() {{ put(5L, 1L); put(7L, 1L); }});
        expectedResult.put(7L, new HashMap<Long, Long>() {{ put(6L, 1L); put(8L, 1L); }});
        expectedResult.put(8L, new HashMap<Long, Long>() {{ put(7L, 1L); put(9L, 1L); }});
        expectedResult.put(9L, new HashMap<Long, Long>() {{ put(8L, 1L); }});
        expectedResult.put(10L, new HashMap<Long, Long>() {{ put(11L, 1L); }});
        expectedResult.put(11L, new HashMap<Long, Long>() {{ put(10L, 1L); put(12L, 1L); }});
        expectedResult.put(12L, new HashMap<Long, Long>() {{ put(11L, 1L); put(13L, 1L); }});
        expectedResult.put(13L, new HashMap<Long, Long>() {{ put(12L, 1L); put(14L, 1L); }});
        expectedResult.put(14L, new HashMap<Long, Long>() {{ put(13L, 1L); put(15L, 1L); }});
        expectedResult.put(15L, new HashMap<Long, Long>() {{ put(14L, 1L); put(16L, 1L); }});
        expectedResult.put(16L, new HashMap<Long, Long>() {{ put(15L, 1L); put(17L, 1L); }});
        expectedResult.put(17L, new HashMap<Long, Long>() {{ put(16L, 1L); put(18L, 1L); }});
        expectedResult.put(18L, new HashMap<Long, Long>() {{ put(17L, 1L); put(19L, 1L); }});
        expectedResult.put(19L, new HashMap<Long, Long>() {{ put(18L, 1L); put(20L, 1L); }});
        expectedResult.put(20L, new HashMap<Long, Long>() {{ put(19L, 1L); }});

        assertEquals(expectedResult, map);
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
