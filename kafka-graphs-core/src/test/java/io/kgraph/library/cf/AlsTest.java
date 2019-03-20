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
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.jblas.FloatMatrix;
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

public class AlsTest extends AbstractIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(AlsTest.class);

    GraphAlgorithm<CfLongId, FloatMatrix, Float, KTable<CfLongId, FloatMatrix>> algorithm;

    @Test
    public void testAls() throws Exception {
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
        KGraph<CfLongId, FloatMatrix, Float> graph = KGraph.fromEdges(edges, new InitVertices(),
            GraphSerialized.with(new KryoSerde<>(), new KryoSerde<>(), Serdes.Float()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Void> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 2, (short) 1);
        state.get();

        Thread.sleep(2000);

        Map<String, Object> configs = new HashMap<>();
        configs.put(Als.LAMBDA, 0.01f);
        configs.put(Als.VECTOR_SIZE, 2);
        configs.put(Als.ITERATIONS, 5);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 2, (short) 1,
                configs, Optional.empty(), new Als());
        streamsConfiguration = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix,
            CLUSTER.bootstrapServers(), graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), streamsConfiguration).streams();
        GraphAlgorithmState<KTable<CfLongId, FloatMatrix>> paths = algorithm.run();
        paths.result().get();

        Thread.sleep(2000);

        Map<CfLongId, FloatMatrix> map = StreamUtils.mapFromStore(paths.streams(), "solutionSetStore-" + suffix);
        log.debug("result: {}", map);

        assertEquals("{1 0=[1.100964; 1.252018], 2 0=[2.488711; 2.831024], 1 1=[0.499041; 0.567667], 2 1=[0.706991; 0.804180]}", map.toString());
    }

    @After
    public void tearDown() throws Exception {
        algorithm.close();
    }

    private static final class InitVertices implements ValueMapper<CfLongId, FloatMatrix> {
        @Override
        public FloatMatrix apply(CfLongId id) {
            return new FloatMatrix();
        }
    }
}
