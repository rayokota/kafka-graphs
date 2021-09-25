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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.DoubleSerializer;
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
import io.kgraph.library.SybilRank.VertexValue;
import io.kgraph.pregel.PregelGraphAlgorithm;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.GraphUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.KryoSerializer;
import io.kgraph.utils.StreamUtils;

public class SybilRankTest extends AbstractIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(SybilRankTest.class);

    GraphAlgorithm<Long, VertexValue, Double, KTable<Long, VertexValue>> algorithm;

    @Test
    public void testSybilRank() throws Exception {
        String suffix = "";
        StreamsBuilder builder = new StreamsBuilder();

        List<KeyValue<Edge<Long>, Double>> list = new ArrayList<>();
        list.add(new KeyValue<>(new Edge<>(1L, 2L), 5.0));
        list.add(new KeyValue<>(new Edge<>(2L, 1L), 5.0));
        list.add(new KeyValue<>(new Edge<>(2L, 4L), 4.0));
        list.add(new KeyValue<>(new Edge<>(4L, 2L), 4.0));
        list.add(new KeyValue<>(new Edge<>(4L, 5L), 3.0));
        list.add(new KeyValue<>(new Edge<>(5L, 4L), 3.0));
        list.add(new KeyValue<>(new Edge<>(3L, 5L), 3.0));
        list.add(new KeyValue<>(new Edge<>(5L, 3L), 3.0));
        list.add(new KeyValue<>(new Edge<>(1L, 3L), 2.0));
        list.add(new KeyValue<>(new Edge<>(3L, 1L), 2.0));
        list.add(new KeyValue<>(new Edge<>(3L, 7L), 1.0));
        list.add(new KeyValue<>(new Edge<>(7L, 3L), 1.0));
        list.add(new KeyValue<>(new Edge<>(6L, 7L), 3.0));
        list.add(new KeyValue<>(new Edge<>(7L, 6L), 3.0));
        list.add(new KeyValue<>(new Edge<>(6L, 9L), 3.0));
        list.add(new KeyValue<>(new Edge<>(9L, 6L), 3.0));
        list.add(new KeyValue<>(new Edge<>(8L, 9L), 2.0));
        list.add(new KeyValue<>(new Edge<>(9L, 8L), 2.0));
        list.add(new KeyValue<>(new Edge<>(7L, 8L), 3.0));
        list.add(new KeyValue<>(new Edge<>(8L, 7L), 3.0));
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            DoubleSerializer.class, new Properties()
        );
        KTable<Edge<Long>, Double> edges =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.Double(), list);
        KGraph<Long, VertexValue, Double> graph = KGraph.fromEdges(edges, new InitVertices(),
            GraphSerialized.with(Serdes.Long(), new KryoSerde<>(), Serdes.Double()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        Map<TopicPartition, Long> offsets = state.get();

        Map<String, Object> configs = new HashMap<>();
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                configs, Optional.empty(), new SybilRank());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<Long, VertexValue>> paths = algorithm.run();
        paths.result().get();

        Thread.sleep(5000);

        Map<Long, Map<Long, Long>> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);

        assertEquals("{1=0.2380952380952381, 2=0.23809523809523808, 3=0.39285714285714285, 4=0.4047619047619047, 5=0.0, 6=0.0, 7=0.0, 8=0.0, 9=0.0}", map.toString());
    }

    @After
    public void tearDown() throws Exception {
        algorithm.close();
    }

    private static final class InitVertices implements ValueMapper<Long, VertexValue> {
        @Override
        public VertexValue apply(Long id) {
            switch (id.intValue()) {
                case 1:
                case 2:
                case 5:
                    return new VertexValue(0.0, true);
                default:
                    return new VertexValue(0.0, false);
            }
        }
    }
}
