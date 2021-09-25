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

package io.kgraph.library.cf;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
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
import io.kgraph.library.GraphAlgorithmType;
import io.kgraph.pregel.PregelGraphAlgorithm;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.GraphUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.KryoSerializer;
import io.kgraph.utils.StreamUtils;

public class SvdppTest extends AbstractIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(SvdppTest.class);

    GraphAlgorithm<CfLongId, Svdpp.SvdppValue, Float, KTable<CfLongId, Svdpp.SvdppValue>> algorithm;

    @Test
    public void testSvdpp() throws Exception {
        String suffix = "";
        StreamsBuilder builder = new StreamsBuilder();

        List<KeyValue<Edge<CfLongId>, Float>> list = new ArrayList<>();
        list.add(new KeyValue<>(new Edge<>(new CfLongId((byte) 0, 1), new CfLongId((byte) 1, 1)), 1.0f));
        list.add(new KeyValue<>(new Edge<>(new CfLongId((byte) 0, 1), new CfLongId((byte) 1, 2)), 2.0f));
        list.add(new KeyValue<>(new Edge<>(new CfLongId((byte) 0, 2), new CfLongId((byte) 1, 1)), 3.0f));
        list.add(new KeyValue<>(new Edge<>(new CfLongId((byte) 0, 2), new CfLongId((byte) 1, 2)), 4.0f));
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            FloatSerializer.class, new Properties()
        );
        KTable<Edge<CfLongId>, Float> edges =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.Float(), list);
        KGraph<CfLongId, Svdpp.SvdppValue, Float> graph = KGraph.fromEdges(edges,
            GraphAlgorithmType.initialVertexValueMapper(GraphAlgorithmType.svdpp),
            GraphSerialized.with(new CfLongIdSerde(), new KryoSerde<>(), Serdes.Float()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(
            builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        Map<TopicPartition, Long> offsets = state.get();

        Map<String, Object> configs = new HashMap<>();
        configs.put(Svdpp.BIAS_LAMBDA, 0.005f);
        configs.put(Svdpp.BIAS_GAMMA, 0.01f);
        configs.put(Svdpp.FACTOR_LAMBDA, 0.005f);
        configs.put(Svdpp.FACTOR_GAMMA, 0.01f);
        configs.put(Svdpp.MIN_RATING, 0f);
        configs.put(Svdpp.MAX_RATING, 5f);
        configs.put(Svdpp.VECTOR_SIZE, 2);
        configs.put(Svdpp.RANDOM_SEED, 0L);
        configs.put(Svdpp.ITERATIONS, 6);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                configs, Optional.empty(), new Svdpp());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<CfLongId, Svdpp.SvdppValue>> paths = algorithm.run();
        paths.result().get();

        Thread.sleep(5000);

        Map<CfLongId, Svdpp.SvdppValue> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.info("result: {}", map);

        assertEquals("{(1, 0)=(0.26593804, [0.007494, 0.008374]), (2, 0)=(0.18473601, [0.006907, 0.008184]), (1, 1)=(0.6420079, [0.007407, 0.002487]), (2, 1)=(0.6016106, [0.006642, 0.001807])}", map.toString());
    }

    //@Test
    public void testSvdppFromRankingsFile() throws Exception {
        String suffix = "file";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            FloatSerializer.class, new Properties()
        );
        GraphUtils.edgesToTopic(GraphUtils.class.getResourceAsStream("/ratings.txt"),
            new EdgeCfLongIdFloatValueParser(),
            new FloatSerializer(),
            producerConfig, "initEdges-" + suffix, 50, (short) 1);
        KTable<Edge<CfLongId>, Float> edges =
            builder.table("initEdges-" + suffix, Consumed.with(new KryoSerde<>(), Serdes.Float()),
                Materialized.with(new KryoSerde<>(), Serdes.Float()));
        KGraph<CfLongId, Svdpp.SvdppValue, Float> graph = KGraph.fromEdges(edges,
            GraphAlgorithmType.initialVertexValueMapper(GraphAlgorithmType.svdpp),
            GraphSerialized.with(new CfLongIdSerde(), new KryoSerde<>(), Serdes.Float()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(
            builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 50, (short) 1);
        Map<TopicPartition, Long> offsets = state.get();

        Thread.sleep(5000);

        Map<String, Object> configs = new HashMap<>();
        configs.put(Svdpp.BIAS_LAMBDA, 0.005f);
        configs.put(Svdpp.BIAS_GAMMA, 0.01f);
        configs.put(Svdpp.FACTOR_LAMBDA, 0.005f);
        configs.put(Svdpp.FACTOR_GAMMA, 0.01f);
        configs.put(Svdpp.MIN_RATING, 0f);
        configs.put(Svdpp.MAX_RATING, 5f);
        configs.put(Svdpp.VECTOR_SIZE, 2);
        configs.put(Svdpp.RANDOM_SEED, 0L);
        configs.put(Svdpp.ITERATIONS, 3);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 50, (short) 1,
                configs, Optional.empty(), new Svdpp());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        //streamsConfiguration.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(10 * 1024 * 1024));
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<CfLongId, Svdpp.SvdppValue>> paths = algorithm.run();
        paths.result().get();

        Thread.sleep(5000);

        NavigableMap<CfLongId, Svdpp.SvdppValue> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        Set<String> result = new TreeSet<>();
        for (Map.Entry<CfLongId, Svdpp.SvdppValue> entry : map.entrySet()) {
            result.add(entry.getKey().toString() + " " + entry.getValue().toString());
        }
        log.info("result: {}", result);
        log.info("first: {}", map.firstEntry());
        log.info("last: {}", map.lastEntry());

        assertEquals("(1, 0)=(0.10995317, [0.006352, 0.007996])", map.firstEntry().toString());
        assertEquals("(2071, 1)=(0.6374174, [0.007310, 0.002405])", map.lastEntry().toString());
    }

    @Test
    public void testSvdppFromRankingsSimpleFile() throws Exception {
        String suffix = "simplefile";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            FloatSerializer.class, new Properties()
        );
        GraphUtils.edgesToTopic(GraphUtils.class.getResourceAsStream("/ratings_simple.txt"),
            new EdgeCfLongIdFloatValueParser(),
            new FloatSerializer(),
            producerConfig, "initEdges-" + suffix, 50, (short) 1);
        KTable<Edge<CfLongId>, Float> edges =
            builder.table("initEdges-" + suffix, Consumed.with(new KryoSerde<>(), Serdes.Float()),
                Materialized.with(new KryoSerde<>(), Serdes.Float()));
        KGraph<CfLongId, Svdpp.SvdppValue, Float> graph = KGraph.fromEdges(edges,
            GraphAlgorithmType.initialVertexValueMapper(GraphAlgorithmType.svdpp),
            GraphSerialized.with(new CfLongIdSerde(), new KryoSerde<>(), Serdes.Float()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Map<TopicPartition, Long>> state = GraphUtils.groupEdgesBySourceAndRepartition(
            builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 50, (short) 1);
        Map<TopicPartition, Long> offsets = state.get();

        Thread.sleep(5000);

        Map<String, Object> configs = new HashMap<>();
        configs.put(Svdpp.BIAS_LAMBDA, 0.005f);
        configs.put(Svdpp.BIAS_GAMMA, 0.01f);
        configs.put(Svdpp.FACTOR_LAMBDA, 0.005f);
        configs.put(Svdpp.FACTOR_GAMMA, 0.01f);
        configs.put(Svdpp.MIN_RATING, 0f);
        configs.put(Svdpp.MAX_RATING, 5f);
        configs.put(Svdpp.VECTOR_SIZE, 2);
        configs.put(Svdpp.RANDOM_SEED, 0L);
        configs.put(Svdpp.ITERATIONS, 3);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, offsets, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 50, (short) 1,
                configs, Optional.empty(), new Svdpp());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        //streamsConfiguration.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(10 * 1024 * 1024));
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<CfLongId, Svdpp.SvdppValue>> paths = algorithm.run();
        paths.result().get();
        log.info("agg: {}", algorithm.state().aggregates());

        Thread.sleep(5000);

        NavigableMap<CfLongId, Svdpp.SvdppValue> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        Set<String> result = new TreeSet<>();
        for (Map.Entry<CfLongId, Svdpp.SvdppValue> entry : map.entrySet()) {
            result.add(entry.getKey().toString() + " " + entry.getValue().toString());
        }
        log.info("result: {}", result);
        log.info("first: {}", map.firstEntry());
        log.info("last: {}", map.lastEntry());

        assertEquals("(1, 0)=(0.11611404, [0.006397, 0.008010])", map.firstEntry().toString());
        assertEquals("(20, 1)=(0.6374174, [0.007310, 0.002405])", map.lastEntry().toString());
    }
    @After
    public void tearDown() throws Exception {
        algorithm.close();
    }
}
