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

package io.kgraph.library.maxbmatching;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

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

public class MaxBMatchingTest extends AbstractIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(MaxBMatchingTest.class);

    GraphAlgorithm<Long, Integer, MBMEdgeValue, KTable<Long, Integer>> algorithm;

    @Test
    public void testMaxBMatching() throws Exception {
        String suffix = "";
        StreamsBuilder builder = new StreamsBuilder();

        List<KeyValue<Edge<Long>, MBMEdgeValue>> list = new ArrayList<>();
        list.add(new KeyValue<>(new Edge<>(1L, 2L), new MBMEdgeValue(3.0)));
        list.add(new KeyValue<>(new Edge<>(1L, 3L), new MBMEdgeValue(1.0)));
        list.add(new KeyValue<>(new Edge<>(2L, 1L), new MBMEdgeValue(3.0)));
        list.add(new KeyValue<>(new Edge<>(2L, 4L), new MBMEdgeValue(1.0)));
        list.add(new KeyValue<>(new Edge<>(2L, 5L), new MBMEdgeValue(1.0)));
        list.add(new KeyValue<>(new Edge<>(3L, 1L), new MBMEdgeValue(1.0)));
        list.add(new KeyValue<>(new Edge<>(3L, 5L), new MBMEdgeValue(3.0)));
        list.add(new KeyValue<>(new Edge<>(4L, 2L), new MBMEdgeValue(1.0)));
        list.add(new KeyValue<>(new Edge<>(4L, 5L), new MBMEdgeValue(2.0)));
        list.add(new KeyValue<>(new Edge<>(5L, 2L), new MBMEdgeValue(1.0)));
        list.add(new KeyValue<>(new Edge<>(5L, 3L), new MBMEdgeValue(3.0)));
        list.add(new KeyValue<>(new Edge<>(5L, 4L), new MBMEdgeValue(2.0)));
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), KryoSerializer.class,
            KryoSerializer.class, new Properties()
        );
        KTable<Edge<Long>, MBMEdgeValue> edges =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), new KryoSerde<>(), list);
        KGraph<Long, Integer, MBMEdgeValue> graph = KGraph.fromEdges(edges, new InitVertices(),
            GraphSerialized.with(Serdes.Long(), Serdes.Integer(), new KryoSerde<>()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Void> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        state.get();

        Thread.sleep(2000);

        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                Collections.emptyMap(), Optional.empty(), new MaxBMatching());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<Long, Integer>> paths = algorithm.run();
        paths.result().get();

        Thread.sleep(2000);

        Map<Long, Map<Long, MBMEdgeValue>> map = StreamUtils.mapFromStore(paths.streams(), "edgesStore-run-" + suffix);
        log.debug("result: {}", map);

        assertEquals("{1={2=3.0	INCLUDED}, 2={1=3.0	INCLUDED, 5=1.0	INCLUDED}, 3={5=3.0	INCLUDED}, 4={5=2.0	INCLUDED}, 5={2=1.0	INCLUDED, 3=3.0	INCLUDED, 4=2.0	INCLUDED}}", map.toString());
    }

    @After
    public void tearDown() throws Exception {
        algorithm.close();
    }

    private static final class InitVertices implements ValueMapper<Long, Integer> {
        @Override
        public Integer apply(Long id) {
            switch (id.intValue()) {
                case 1:
                case 3:
                case 4:
                    return 1;
                case 2:
                    return 2;
                case 5:
                    return 3;
            }
            return 0;
        }
    }
}
