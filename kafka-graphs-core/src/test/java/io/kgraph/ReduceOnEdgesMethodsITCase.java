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

	/*@Test
	public void testLowestWeightInNeighbor() throws Exception {
		*//*
     * Get the lowest-weight in-neighbor
     * for each vertex
     *//*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithLowestOutNeighbor =
			graph.groupReduceOnEdges(new SelectMinWeightInNeighbor(), EdgeDirection.IN);
		List<Tuple2<Long, Long>> result = verticesWithLowestOutNeighbor.collect();

		expectedResult = "1,5\n" +
			"2,1\n" +
			"3,1\n" +
			"4,3\n" +
			"5,3\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllOutNeighbors() throws Exception {
		*//*
     * Get the all the out-neighbors for each vertex
     *//*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllOutNeighbors =
			graph.groupReduceOnEdges(new SelectOutNeighbors(), EdgeDirection.OUT);
		List<Tuple2<Long, Long>> result = verticesWithAllOutNeighbors.collect();

		expectedResult = "1,2\n" +
			"1,3\n" +
			"2,3\n" +
			"3,4\n" +
			"3,5\n" +
			"4,5\n" +
			"5,1";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllOutNeighborsNoValue() throws Exception {
		*//*
     * Get the all the out-neighbors for each vertex except for the vertex with id 5.
     *//*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllOutNeighbors =
			graph.groupReduceOnEdges(new SelectOutNeighborsExcludeFive(), EdgeDirection.OUT);
		List<Tuple2<Long, Long>> result = verticesWithAllOutNeighbors.collect();

		expectedResult = "1,2\n" +
			"1,3\n" +
			"2,3\n" +
			"3,4\n" +
			"3,5\n" +
			"4,5";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllOutNeighborsWithValueGreaterThanTwo() throws Exception {
		*//*
     * Get the all the out-neighbors for each vertex that have a value greater than two.
     *//*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllOutNeighbors =
			graph.groupReduceOnEdges(new SelectOutNeighborsValueGreaterThanTwo(), EdgeDirection.OUT);
		List<Tuple2<Long, Long>> result = verticesWithAllOutNeighbors.collect();

		expectedResult = "3,4\n" +
			"3,5\n" +
			"4,5\n" +
			"5,1";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllInNeighbors() throws Exception {
		*//*
     * Get the all the in-neighbors for each vertex
     *//*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllInNeighbors =
			graph.groupReduceOnEdges(new SelectInNeighbors(), EdgeDirection.IN);
		List<Tuple2<Long, Long>> result = verticesWithAllInNeighbors.collect();

		expectedResult = "1,5\n" +
			"2,1\n" +
			"3,1\n" +
			"3,2\n" +
			"4,3\n" +
			"5,3\n" +
			"5,4";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllInNeighborsNoValue() throws Exception {
		*//*
     * Get the all the in-neighbors for each vertex except for the vertex with id 5.
     *//*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllInNeighbors =
			graph.groupReduceOnEdges(new SelectInNeighborsExceptFive(), EdgeDirection.IN);
		List<Tuple2<Long, Long>> result = verticesWithAllInNeighbors.collect();

		expectedResult = "1,5\n" +
			"2,1\n" +
			"3,1\n" +
			"3,2\n" +
			"4,3";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllInNeighborsWithValueGreaterThanTwo() throws Exception {
		*//*
     * Get the all the in-neighbors for each vertex that have a value greater than two.
     *//*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllInNeighbors =
			graph.groupReduceOnEdges(new SelectInNeighborsValueGreaterThanTwo(), EdgeDirection.IN);
		List<Tuple2<Long, Long>> result = verticesWithAllInNeighbors.collect();

		expectedResult = "3,1\n" +
			"3,2\n" +
			"4,3\n" +
			"5,3\n" +
			"5,4";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllNeighbors() throws Exception {
		*//*
     * Get the all the neighbors for each vertex
     *//*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllNeighbors =
			graph.groupReduceOnEdges(new SelectNeighbors(), EdgeDirection.BOTH);
		List<Tuple2<Long, Long>> result = verticesWithAllNeighbors.collect();

		expectedResult = "1,2\n" +
			"1,3\n" +
			"1,5\n" +
			"2,1\n" +
			"2,3\n" +
			"3,1\n" +
			"3,2\n" +
			"3,4\n" +
			"3,5\n" +
			"4,3\n" +
			"4,5\n" +
			"5,1\n" +
			"5,3\n" +
			"5,4";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllNeighborsNoValue() throws Exception {
		*//*
     * Get the all the neighbors for each vertex except for vertices with id 5 and 2.
     *//*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllNeighbors =
			graph.groupReduceOnEdges(new SelectNeighborsExceptFiveAndTwo(), EdgeDirection.BOTH);
		List<Tuple2<Long, Long>> result = verticesWithAllNeighbors.collect();

		expectedResult = "1,2\n" +
			"1,3\n" +
			"1,5\n" +
			"3,1\n" +
			"3,2\n" +
			"3,4\n" +
			"3,5\n" +
			"4,3\n" +
			"4,5";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllNeighborsWithValueGreaterThanFour() throws Exception {
		*//*
     * Get the all the neighbors for each vertex that have a value greater than four.
     *//*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllNeighbors =
			graph.groupReduceOnEdges(new SelectNeighborsValueGreaterThanFour(), EdgeDirection.BOTH);
		List<Tuple2<Long, Long>> result = verticesWithAllNeighbors.collect();

		expectedResult = "5,1\n" +
			"5,3\n" +
			"5,4";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testMaxWeightEdge() throws Exception {
		*//*
     * Get the maximum weight among all edges
     * of a vertex
     *//*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithMaxEdgeWeight =
			graph.groupReduceOnEdges(new SelectMaxWeightNeighbor(), EdgeDirection.BOTH);
		List<Tuple2<Long, Long>> result = verticesWithMaxEdgeWeight.collect();

		expectedResult = "1,51\n" +
			"2,23\n" +
			"3,35\n" +
			"4,45\n" +
			"5,51\n";

		compareResultAsTuples(result, expectedResult);
	}
	*/

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

	/*
	@Test
	public void testLowestWeightInNeighborNoValue() throws Exception {
		*//*
     * Get the lowest-weight out of all the in-neighbors
     * of each vertex
     *//*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithLowestOutNeighbor =
			graph.reduceOnEdges(new SelectMinWeightNeighborNoValue(), EdgeDirection.IN);
		List<Tuple2<Long, Long>> result = verticesWithLowestOutNeighbor.collect();

		expectedResult = "1,51\n" +
			"2,12\n" +
			"3,13\n" +
			"4,34\n" +
			"5,35\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testMaxWeightAllNeighbors() throws Exception {
		*//*
     * Get the maximum weight among all edges
     * of a vertex
     *//*
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithMaxEdgeWeight =
			graph.reduceOnEdges(new SelectMaxWeightNeighborNoValue(), EdgeDirection.BOTH);
		List<Tuple2<Long, Long>> result = verticesWithMaxEdgeWeight.collect();

		expectedResult = "1,51\n" +
			"2,23\n" +
			"3,35\n" +
			"4,45\n" +
			"5,51\n";

		compareResultAsTuples(result, expectedResult);
	}
	*/

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

	/*
	@SuppressWarnings("serial")
	private static final class SelectMaxWeightNeighbor implements EdgesFunctionWithVertexValue<Long, Long, Long,
	Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v,
				Iterable<Edge<Long, Long>> edges, Collector<Tuple2<Long, Long>> out) throws Exception {

			long weight = Long.MIN_VALUE;

			for (Edge<Long, Long> edge : edges) {
				if (edge.value() > weight) {
					weight = edge.value();
				}
			}
			out.collect(new Tuple2<>(v.id(), weight));
		}
	}
	*/

    @SuppressWarnings("serial")
    private static final class SelectMinWeightNeighborNoValue implements Reducer<Long> {

        @Override
        public Long apply(Long firstEdgeValue, Long secondEdgeValue) {
            return Math.min(firstEdgeValue, secondEdgeValue);
        }
    }

	/*
	@SuppressWarnings("serial")
	private static final class SelectMaxWeightNeighborNoValue implements ReduceEdgesFunction<Long> {

		@Override
		public Long reduceEdges(Long firstEdgeValue, Long secondEdgeValue) {
			return Math.max(firstEdgeValue, secondEdgeValue);
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMinWeightInNeighbor implements EdgesFunctionWithVertexValue<Long, Long, Long,
	Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v,
				Iterable<Edge<Long, Long>> edges, Collector<Tuple2<Long, Long>> out) throws Exception {

			long weight = Long.MAX_VALUE;
			long minNeighborId = 0;

			for (Edge<Long, Long> edge : edges) {
				if (edge.value() < weight) {
					weight = edge.value();
					minNeighborId = edge.source();
				}
			}
			out.collect(new Tuple2<>(v.id(), minNeighborId));
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectOutNeighbors implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Tuple2<Long, Edge<Long, Long>> edge : edges) {
				out.collect(new Tuple2<>(edge.f0, edge.f1.target()));
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectOutNeighborsExcludeFive implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Tuple2<Long, Edge<Long, Long>> edge : edges) {
				if (edge.f0 != 5) {
					out.collect(new Tuple2<>(edge.f0, edge.f1.target()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectOutNeighborsValueGreaterThanTwo implements
		EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v, Iterable<Edge<Long, Long>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Edge<Long, Long> edge : edges) {
				if (v.value() > 2) {
					out.collect(new Tuple2<>(v.id(), edge.target()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectInNeighbors implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Tuple2<Long, Edge<Long, Long>> edge : edges) {
				out.collect(new Tuple2<>(edge.f0, edge.f1.source()));
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectInNeighborsExceptFive implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Tuple2<Long, Edge<Long, Long>> edge : edges) {
				if (edge.f0 != 5) {
					out.collect(new Tuple2<>(edge.f0, edge.f1.source()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectInNeighborsValueGreaterThanTwo implements
		EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v, Iterable<Edge<Long, Long>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Edge<Long, Long> edge : edges) {
				if (v.value() > 2) {
					out.collect(new Tuple2<>(v.id(), edge.source()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectNeighbors implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {
			for (Tuple2<Long, Edge<Long, Long>> edge : edges) {
				if (Objects.equals(edge.f0, edge.f1.target())) {
					out.collect(new Tuple2<>(edge.f0, edge.f1.source()));
				} else {
					out.collect(new Tuple2<>(edge.f0, edge.f1.target()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectNeighborsExceptFiveAndTwo implements EdgesFunction<Long, Long, Tuple2<Long,
	Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Tuple2<Long, Edge<Long, Long>> edge : edges) {
				if (edge.f0 != 5 && edge.f0 != 2) {
					if (Objects.equals(edge.f0, edge.f1.target())) {
						out.collect(new Tuple2<>(edge.f0, edge.f1.source()));
					} else {
						out.collect(new Tuple2<>(edge.f0, edge.f1.target()));
					}
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectNeighborsValueGreaterThanFour implements
		EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v, Iterable<Edge<Long, Long>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Edge<Long, Long> edge : edges) {
				if (v.value() > 4) {
					if (v.id().equals(edge.target())) {
						out.collect(new Tuple2<>(v.id(), edge.source()));
					} else {
						out.collect(new Tuple2<>(v.id(), edge.target()));
					}
				}
			}
		}
	}*/
}
