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

package io.kgraph;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Reducer;
import org.junit.Test;

import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.StreamUtils;
import io.kgraph.utils.TestUtils;

public class ReduceOnEdgesMethodsITCase extends AbstractIntegrationTest {

    private String expectedResult;

    @Test
    public void testLowestWeightOutNeighbor() throws Exception {
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            LongSerializer.class, new Properties()
        );
        StreamsBuilder builder = new StreamsBuilder();

        KTable<Long, Long> vertices =
            StreamUtils.tableFromCollection(builder, producerConfig, Serdes.Long(), Serdes.Long(),
                TestGraphUtils.getLongLongVertices());

        KTable<Edge<Long>, Long> edges =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.Long(),
                TestGraphUtils.getLongLongEdges());

        KGraph<Long, Long, Long> graph = new KGraph<>(
            vertices, edges, GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));

        KTable<Long, Long> verticesWithLowestOutNeighbor =
            graph.groupReduceOnEdges(new SelectMinWeightNeighbor(), EdgeDirection.OUT);

        startStreams(builder, Serdes.Long(), Serdes.Long());

        Thread.sleep(5000);

        List<KeyValue<Long, Long>> result = StreamUtils.listFromTable(streams, verticesWithLowestOutNeighbor);

        expectedResult = "1,2\n" +
            "2,3\n" +
            "3,4\n" +
            "4,5\n" +
            "5,1\n";

        TestUtils.compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testLowestWeightOutNeighborNoValue() throws Exception {
        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            LongSerializer.class, new Properties()
        );
        StreamsBuilder builder = new StreamsBuilder();

        KTable<Long, Long> vertices =
            StreamUtils.tableFromCollection(builder, producerConfig, Serdes.Long(), Serdes.Long(),
                TestGraphUtils.getLongLongVertices());

        KTable<Edge<Long>, Long> edges =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.Long(),
                TestGraphUtils.getLongLongEdges());

        KGraph<Long, Long, Long> graph = new KGraph<>(
            vertices, edges, GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));

        KTable<Long, Long> verticesWithLowestOutNeighbor =
            graph.reduceOnEdges(new SelectMinWeightNeighborNoValue(), EdgeDirection.OUT);

        startStreams(builder, Serdes.Long(), Serdes.Long());

        Thread.sleep(5000);

        List<KeyValue<Long, Long>> result = StreamUtils.listFromTable(streams, verticesWithLowestOutNeighbor);

        expectedResult = "1,12\n" +
            "2,23\n" +
            "3,34\n" +
            "4,45\n" +
            "5,51\n";

        TestUtils.compareResultAsTuples(result, expectedResult);
    }

    @SuppressWarnings("serial")
    private static final class SelectMinWeightNeighbor
        implements EdgesFunctionWithVertexValue<Long, Long, Long, Long> {

        @Override
        public Long iterateEdges(Long vertexValue, Iterable<EdgeWithValue<Long, Long>> edges) {
            long weight = Long.MAX_VALUE;
            long minNeighborId = 0;

            if (edges != null) {
                for (EdgeWithValue<Long, Long> edge : edges) {
                    if (edge.value() < weight) {
                        weight = edge.value();
                        minNeighborId = edge.target();
                    }
                }
            }
            return minNeighborId;
        }
    }

    @SuppressWarnings("serial")
    private static final class SelectMinWeightNeighborNoValue implements Reducer<Long> {

        @Override
        public Long apply(Long firstEdgeValue, Long secondEdgeValue) {
            return Math.min(firstEdgeValue, secondEdgeValue);
        }
    }
}
