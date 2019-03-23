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

package io.kgraph.library.similarity;

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
import io.kgraph.pregel.PregelGraphAlgorithm;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.GraphUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.KryoSerializer;
import io.kgraph.utils.StreamUtils;

public class JaccardTest extends AbstractIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(JaccardTest.class);

    GraphAlgorithm<Long, Double, Double, KTable<Long, Double>> algorithm;

    @Test
    public void testExactSimilarity() throws Exception {
        String suffix = "similarity";
        StreamsBuilder builder = new StreamsBuilder();

        List<KeyValue<Edge<Long>, Double>> list = new ArrayList<>();
        list.add(new KeyValue<>(new Edge<>(1L, 2L), 0.0));
        list.add(new KeyValue<>(new Edge<>(1L, 3L), 0.0));
        list.add(new KeyValue<>(new Edge<>(1L, 4L), 0.0));
        list.add(new KeyValue<>(new Edge<>(2L, 1L), 0.0));
        list.add(new KeyValue<>(new Edge<>(2L, 4L), 0.0));
        list.add(new KeyValue<>(new Edge<>(2L, 5L), 0.0));
        list.add(new KeyValue<>(new Edge<>(3L, 1L), 0.0));
        list.add(new KeyValue<>(new Edge<>(3L, 4L), 0.0));
        list.add(new KeyValue<>(new Edge<>(4L, 1L), 0.0));
        list.add(new KeyValue<>(new Edge<>(4L, 2L), 0.0));
        list.add(new KeyValue<>(new Edge<>(4L, 3L), 0.0));
        list.add(new KeyValue<>(new Edge<>(4L, 5L), 0.0));
        list.add(new KeyValue<>(new Edge<>(5L, 2L), 0.0));
        list.add(new KeyValue<>(new Edge<>(5L, 4L), 0.0));
        list.add(new KeyValue<>(new Edge<>(5L, 6L), 0.0));
        list.add(new KeyValue<>(new Edge<>(6L, 5L), 0.0));
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            DoubleSerializer.class, new Properties()
        );
        KTable<Edge<Long>, Double> edges =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.Double(), list);
        KGraph<Long, Double, Double> graph = KGraph.fromEdges(edges, new InitVertices(),
            GraphSerialized.with(Serdes.Long(), Serdes.Double(), Serdes.Double()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        state.get();

        Map<String, Object> configs = new HashMap<>();
        configs.put("distance.conversion.enabled", false);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                configs, Optional.empty(), new Jaccard<>());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<Long, Double>> paths = algorithm.run();
        paths.result().get();

        Map<Long, Map<Long, Double>> edgesMap = StreamUtils.mapFromStore(paths.streams(), "edgesStore-run-" + suffix);
        log.debug("edges : {}", edgesMap);

        assertEquals("{1={2=0.2, 3=0.25, 4=0.4}, 2={1=0.2, 4=0.4, 5=0.2}, 3={1=0.25, 4=0.2}, 4={1=0.4, 2=0.4, 3=0.2, 5=0.16666666666666666}, 5={2=0.2, 4=0.16666666666666666, 6=0.0}, 6={5=0.0}}", edgesMap.toString());
    }

    @Test
    public void testExactDistance() throws Exception {
        String suffix = "distance";
        StreamsBuilder builder = new StreamsBuilder();

        List<KeyValue<Edge<Long>, Double>> list = new ArrayList<>();
        list.add(new KeyValue<>(new Edge<>(1L, 2L), 0.0));
        list.add(new KeyValue<>(new Edge<>(1L, 3L), 0.0));
        list.add(new KeyValue<>(new Edge<>(1L, 4L), 0.0));
        list.add(new KeyValue<>(new Edge<>(2L, 1L), 0.0));
        list.add(new KeyValue<>(new Edge<>(2L, 4L), 0.0));
        list.add(new KeyValue<>(new Edge<>(2L, 5L), 0.0));
        list.add(new KeyValue<>(new Edge<>(3L, 1L), 0.0));
        list.add(new KeyValue<>(new Edge<>(3L, 4L), 0.0));
        list.add(new KeyValue<>(new Edge<>(4L, 1L), 0.0));
        list.add(new KeyValue<>(new Edge<>(4L, 2L), 0.0));
        list.add(new KeyValue<>(new Edge<>(4L, 3L), 0.0));
        list.add(new KeyValue<>(new Edge<>(4L, 5L), 0.0));
        list.add(new KeyValue<>(new Edge<>(5L, 2L), 0.0));
        list.add(new KeyValue<>(new Edge<>(5L, 4L), 0.0));
        list.add(new KeyValue<>(new Edge<>(5L, 6L), 0.0));
        list.add(new KeyValue<>(new Edge<>(6L, 5L), 0.0));
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            DoubleSerializer.class, new Properties()
        );
        KTable<Edge<Long>, Double> edges =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.Double(), list);
        KGraph<Long, Double, Double> graph = KGraph.fromEdges(edges, new InitVertices(),
            GraphSerialized.with(Serdes.Long(), Serdes.Double(), Serdes.Double()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        state.get();

        Map<String, Object> configs = new HashMap<>();
        configs.put("distance.conversion.enabled", true);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                configs, Optional.empty(), new Jaccard<>());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<Long, Double>> paths = algorithm.run();
        paths.result().get();

        Thread.sleep(2000);

        Map<Long, Map<Long, Double>> edgesMap = StreamUtils.mapFromStore(paths.streams(), "edgesStore-run-" + suffix);
        log.debug("edges : {}", edgesMap);

        assertEquals("{1={2=4.0, 3=3.0, 4=1.5}, 2={1=4.0, 4=1.5, 5=4.0}, 3={1=3.0, 4=4.0}, 4={1=1.5, 2=1.5, 3=4.0, 5=5.0}, 5={2=4.0, 4=5.0, 6=1.7976931348623157E308}, 6={5=1.7976931348623157E308}}", edgesMap.toString());
    }

    @After
    public void tearDown() throws Exception {
        algorithm.close();
    }

    private static final class InitVertices implements ValueMapper<Long, Double> {
        @Override
        public Double apply(Long id) {
            return 0.0;
        }
    }
}
