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

import static io.kgraph.utils.TestUtils.compareResultAsTuples;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.Test;

import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.StreamUtils;

public class ReduceOnNeighborMethodsITCase extends AbstractIntegrationTest {

    private String expectedResult;

    @Test
    public void testSumOfOutNeighbors() throws Exception {
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

        KTable<Long, Long> verticesWithSumOfOutNeighborValues =
            graph.groupReduceOnNeighbors(new SumOutNeighbors(), EdgeDirection.OUT);

        startStreams(builder, Serdes.Long(), Serdes.Long());

        Thread.sleep(5000);

        List<KeyValue<Long, Long>> result = StreamUtils.listFromTable(streams, verticesWithSumOfOutNeighborValues);

        expectedResult = "1,5\n" +
            "2,3\n" +
            "3,9\n" +
            "4,5\n" +
            "5,1\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testSumOfOutNeighborsNoValue() throws Exception {
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

        KTable<Long, Long> verticesWithSumOfOutNeighborValues =
            graph.reduceOnNeighbors((v1, v2) -> v1 + v2, EdgeDirection.OUT);

        startStreams(builder, Serdes.Long(), Serdes.Long());

        Thread.sleep(5000);

        List<KeyValue<Long, Long>> result = StreamUtils.listFromTable(streams, verticesWithSumOfOutNeighborValues);

        expectedResult = "1,5\n" +
            "2,3\n" +
            "3,9\n" +
            "4,5\n" +
            "5,1\n";

        compareResultAsTuples(result, expectedResult);
    }

    private static final class SumOutNeighbors implements
        NeighborsFunctionWithVertexValue<Long, Long, Long, Long> {

        @Override
        public Long iterateNeighbors(Long vertex,
                                     Map<EdgeWithValue<Long, Long>, Long> neighbors) {

            long sum = 0;
            for (Long value : neighbors.values()) {
                sum += value;
            }
            return sum;
        }
    }
}
