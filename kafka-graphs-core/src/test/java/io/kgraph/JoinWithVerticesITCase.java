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
import org.junit.Test;

import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.StreamUtils;
import io.kgraph.utils.TestUtils;

public class JoinWithVerticesITCase extends AbstractIntegrationTest {

    private String expectedResult;

    @Test
    public void testJoinWithVertexSet() throws Exception {
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

        KGraph<Long, Long, Long> res = graph.joinWithVertices(graph.vertices()
            .mapValues(v -> v), new AddValuesMapper());

        KTable<Long, Long> data = res.vertices();

        startStreams(builder, Serdes.Long(), Serdes.Long());

        Thread.sleep(5000);

        List<KeyValue<Long, Long>> result = StreamUtils.listFromTable(streams, data);

        expectedResult = "1,2\n" +
            "2,4\n" +
            "3,6\n" +
            "4,8\n" +
            "5,10\n";

        TestUtils.compareResultAsTuples(result, expectedResult);

	    /*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		KGraph<Long, Long, Long> graph = KGraph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		Graph<Long, Long, Long> res = graph.joinWithVertices(graph.vertices()
			.map(new VertexToTuple2Map<>()), new AddValuesMapper());

		DataSet<Vertex<Long, Long>> data = res.vertices();
		List<Vertex<Long, Long>> result = data.collect();

		expectedResult = "1,2\n" +
			"2,4\n" +
			"3,6\n" +
			"4,8\n" +
			"5,10\n";

		compareResultAsTuples(result, expectedResult);
		*/
    }

	/*
	@Test
	public void testWithLessElements() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		Graph<Long, Long, Long> res = graph.joinWithVertices(graph.vertices().first(3)
			.map(new VertexToTuple2Map<>()), new AddValuesMapper());

		DataSet<Vertex<Long, Long>> data = res.vertices();
		List<Vertex<Long, Long>> result = data.collect();

		expectedResult = "1,2\n" +
			"2,4\n" +
			"3,6\n" +
			"4,4\n" +
			"5,5\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testWithDifferentType() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		Graph<Long, Long, Long> res = graph.joinWithVertices(graph.vertices().first(3)
			.map(new ProjectIdWithTrue()), new DoubleIfTrueMapper());

		DataSet<Vertex<Long, Long>> data = res.vertices();
		List<Vertex<Long, Long>> result = data.collect();

		expectedResult = "1,2\n" +
			"2,4\n" +
			"3,6\n" +
			"4,4\n" +
			"5,5\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testWithDifferentKeys() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		Graph<Long, Long, Long> res = graph.joinWithVertices(TestGraphUtils.getLongLongTuple2Data(env),
			new ProjectSecondMapper());

		DataSet<Vertex<Long, Long>> data = res.vertices();
		List<Vertex<Long, Long>> result = data.collect();

		expectedResult = "1,10\n" +
			"2,20\n" +
			"3,30\n" +
			"4,40\n" +
			"5,5\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testWithCustomType() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		Graph<Long, Long, Long> res = graph.joinWithVertices(TestGraphUtils.getLongCustomTuple2Data(env),
			new CustomValueMapper());

		DataSet<Vertex<Long, Long>> data = res.vertices();
		List<Vertex<Long, Long>> result = data.collect();

		expectedResult = "1,10\n" +
			"2,20\n" +
			"3,30\n" +
			"4,40\n" +
			"5,5\n";

		compareResultAsTuples(result, expectedResult);
	}
	*/

    @SuppressWarnings("serial")
    private static final class AddValuesMapper implements VertexJoinFunction<Long, Long> {

        public Long vertexJoin(Long vertexValue, Long inputValue) {
            return vertexValue + inputValue;
        }
    }

	/*
	@SuppressWarnings("serial")
	private static final class ProjectIdWithTrue implements MapFunction<Vertex<Long, Long>, Tuple2<Long, Boolean>> {
		public Tuple2<Long, Boolean> map(Vertex<Long, Long> vertex) throws Exception {
			return new Tuple2<>(vertex.id(), true);
		}
	}

	@SuppressWarnings("serial")
	private static final class DoubleIfTrueMapper implements VertexJoinFunction<Long, Boolean> {

		public Long vertexJoin(Long vertexValue, Boolean inputValue) {
			if (inputValue) {
				return vertexValue * 2;
			} else {
				return vertexValue;
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class ProjectSecondMapper implements VertexJoinFunction<Long, Long> {

		public Long vertexJoin(Long vertexValue, Long inputValue) {
			return inputValue;
		}
	}

	@SuppressWarnings("serial")
	private static final class CustomValueMapper implements VertexJoinFunction<Long,
		DummyCustomParameterizedType<Float>> {

		public Long vertexJoin(Long vertexValue, DummyCustomParameterizedType<Float> inputValue) {
			return (long) inputValue.getIntField();
		}
	}
	*/
}
