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
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
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
import io.kgraph.EdgeJoinFunction;
import io.kgraph.GraphAlgorithm;
import io.kgraph.GraphAlgorithmState;
import io.kgraph.GraphSerialized;
import io.kgraph.KGraph;
import io.kgraph.TestGraphUtils;
import io.kgraph.pregel.PregelGraphAlgorithm;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.GraphGenerators;
import io.kgraph.utils.GraphUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.StreamUtils;
import io.vavr.Tuple2;

public class PageRankTest extends AbstractIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(PageRankTest.class);

    GraphAlgorithm<Long, Tuple2<Double, Double>, Double, KTable<Long, Tuple2<Double, Double>>> algorithm;

    @Test
    public void testChainPageRank() throws Exception {
        String suffix = "chain";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            DoubleSerializer.class, new Properties()
        );
        KTable<Edge<Long>, Double> edges =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.Double(),
                TestGraphUtils.getChain());
        // TODO set initial vertices to tuples?
        KGraph<Long, Double, Double> initialGraph = KGraph.fromEdges(edges, new InitVertices(),
            GraphSerialized.with(Serdes.Long(), Serdes.Double(), Serdes.Double()));
        KTable<Long, Long> vertexOutDegrees = initialGraph.outDegrees();
        KGraph<Long, Double, Double> joinedGraph = initialGraph
            .joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());
        KTable<Long, Tuple2<Double, Double>> vertices =
            joinedGraph.vertices().mapValues((k, v) -> new Tuple2<>(0.0, 0.0));
        KGraph<Long, Tuple2<Double, Double>, Double> graph =
            new KGraph<>(vertices, joinedGraph.edges(), GraphSerialized.with(joinedGraph.keySerde(), new KryoSerde<>
                (), Serdes.Double()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix, CLUSTER
                .bootstrapServers(),
            graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Void> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 50, (short) 1);
        state.get();

        Thread.sleep(2000);

        double resetProb = 0.15;
        double tol = 0.0001;
        Map<String, Object> configs = new HashMap<>();
        configs.put(PageRank.RESET_PROBABILITY, resetProb);
        configs.put(PageRank.TOLERANCE, tol);
        Optional<Double> initMsg = Optional.of(resetProb / (1.0 - resetProb));
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 50, (short) 1,
                configs, initMsg, new PageRank<>());
        props = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix, CLUSTER.bootstrapServers(),
            graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), props).streams();
        int maxIterations = 1;  //Integer.MAX_VALUE;
        GraphAlgorithmState<KTable<Long, Tuple2<Double, Double>>> ranks = algorithm.run(maxIterations);
        ranks.result().get();

        Thread.sleep(2000);

        Map<Long, Tuple2<Double, Double>> map = StreamUtils.mapFromStore(ranks.streams(), "solutionSetStore-" +
            suffix);
        List<Double> list = map.values().stream().map(Tuple2::_1).sorted().collect(Collectors.toList());

        log.debug("result: {}", map);

        List<Double> expectedResult = new ArrayList<>();
        expectedResult.add(0.15);
        expectedResult.add(0.27749999999999997);
        expectedResult.add(0.27749999999999997);
        expectedResult.add(0.27749999999999997);
        expectedResult.add(0.27749999999999997);
        expectedResult.add(0.27749999999999997);
        expectedResult.add(0.27749999999999997);
        expectedResult.add(0.27749999999999997);
        expectedResult.add(0.27749999999999997);
        expectedResult.add(0.27749999999999997);
        assertEquals(expectedResult, list);
    }

    @Test
    public void testChainLongerPageRank() throws Exception {
        String suffix = "longer";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            DoubleSerializer.class, new Properties()
        );
        KTable<Edge<Long>, Double> edges =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.Double(),
                TestGraphUtils.getChain());
        // TODO set initial vertices to tuples?
        KGraph<Long, Double, Double> initialGraph = KGraph.fromEdges(edges, new InitVertices(),
            GraphSerialized.with(Serdes.Long(), Serdes.Double(), Serdes.Double()));
        KTable<Long, Long> vertexOutDegrees = initialGraph.outDegrees();
        KGraph<Long, Double, Double> joinedGraph = initialGraph
            .joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());
        KTable<Long, Tuple2<Double, Double>> vertices =
            joinedGraph.vertices().mapValues((k, v) -> new Tuple2<>(0.0, 0.0));
        KGraph<Long, Tuple2<Double, Double>, Double> graph =
            new KGraph<>(vertices, joinedGraph.edges(), GraphSerialized.with(joinedGraph.keySerde(), new KryoSerde<>
                (), Serdes.Double()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix, CLUSTER
                .bootstrapServers(),
            graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Void> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 50, (short) 1);
        state.get();

        Thread.sleep(2000);

        double resetProb = 0.15;
        double tol = 0.0001;
        Map<String, Object> configs = new HashMap<>();
        configs.put(PageRank.RESET_PROBABILITY, resetProb);
        configs.put(PageRank.TOLERANCE, tol);
        Optional<Double> initMsg = Optional.of(resetProb / (1.0 - resetProb));
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 50, (short) 1,
                configs, initMsg, new PageRank<>());
        props = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix, CLUSTER.bootstrapServers(),
            graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), props).streams();
        int maxIterations = 10;
        GraphAlgorithmState<KTable<Long, Tuple2<Double, Double>>> ranks = algorithm.run(maxIterations);
        ranks.result().get();

        Thread.sleep(2000);

        Map<Long, Tuple2<Double, Double>> map = StreamUtils.mapFromStore(ranks.streams(), "solutionSetStore-" +
            suffix);
        List<Double> list = map.values().stream().map(Tuple2::_1).sorted().collect(Collectors.toList());

        log.debug("result: {}", map);

        List<Double> expectedResult = new ArrayList<>();
        expectedResult.add(0.15);
        expectedResult.add(0.27749999999999997);
        expectedResult.add(0.38587499999999997);
        expectedResult.add(0.47799375);
        expectedResult.add(0.5562946875);
        expectedResult.add(0.622850484375);
        expectedResult.add(0.67942291171875);
        expectedResult.add(0.7275094749609375);
        expectedResult.add(0.7683830537167969);
        expectedResult.add(0.8031255956592774);
        assertEquals(expectedResult, list);
    }

    @Test
    public void testChainPersonalPageRank() throws Exception {
        String suffix = "chain-personal";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            DoubleSerializer.class, new Properties()
        );
        KTable<Edge<Long>, Double> edges =
            StreamUtils.tableFromCollection(builder, producerConfig, new KryoSerde<>(), Serdes.Double(),
                TestGraphUtils.getChain());
        // TODO set initial vertices to tuples?
        KGraph<Long, Double, Double> initialGraph = KGraph.fromEdges(edges, new InitVertices(),
            GraphSerialized.with(Serdes.Long(), Serdes.Double(), Serdes.Double()));
        KTable<Long, Long> vertexOutDegrees = initialGraph.outDegrees();
        KGraph<Long, Double, Double> joinedGraph = initialGraph
            .joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());

        long srcVertexId = 4L;
        KTable<Long, Tuple2<Double, Double>> vertices =
            joinedGraph.vertices().mapValues((k, v) -> new Tuple2<>(0.0, k == srcVertexId ? Double.NEGATIVE_INFINITY
                : 0.0));
        KGraph<Long, Tuple2<Double, Double>, Double> graph =
            new KGraph<>(vertices, joinedGraph.edges(), GraphSerialized.with(joinedGraph.keySerde(), new KryoSerde<>
                (), Serdes.Double()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix, CLUSTER
                .bootstrapServers(),
            graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Void> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 50, (short) 1);
        state.get();

        Thread.sleep(2000);

        double resetProb = 0.15;
        double tol = 0.0001;
        Map<String, Object> configs = new HashMap<>();
        configs.put(PageRank.RESET_PROBABILITY, resetProb);
        configs.put(PageRank.TOLERANCE, tol);
        configs.put(PageRank.SRC_VERTEX_ID, srcVertexId);
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 50, (short) 1,
                configs, Optional.of(0.0), new PageRank<>());
        props = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix, CLUSTER.bootstrapServers(),
            graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), props).streams();
        int maxIterations = 3;
        GraphAlgorithmState<KTable<Long, Tuple2<Double, Double>>> ranks = algorithm.run(maxIterations);
        ranks.result().get();

        Thread.sleep(2000);

        Map<Long, Tuple2<Double, Double>> map = StreamUtils.mapFromStore(ranks.streams(), "solutionSetStore-" +
            suffix);
        List<Double> list = map.values().stream().map(Tuple2::_1).sorted().collect(Collectors.toList());

        log.debug("result: {}", map);

        List<Double> expectedResult = new ArrayList<>();
        expectedResult.add(0.0);
        expectedResult.add(0.0);
        expectedResult.add(0.0);
        expectedResult.add(0.0);
        expectedResult.add(0.0);
        expectedResult.add(0.0);
        expectedResult.add(0.6141249999999999);
        expectedResult.add(0.7224999999999999);
        expectedResult.add(0.85);
        expectedResult.add(1.0);
        assertEquals(expectedResult, list);
    }

    @Test
    public void testCompletePageRank() throws Exception {
        String suffix = "complete";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), IntegerSerializer.class,
            IntegerSerializer.class, new Properties()
        );
        KGraph<Long, Long, Long> completeGraph = GraphGenerators.completeGraph(builder, producerConfig, 25);
        KTable<Long, Double> initialVertices = completeGraph.vertices().mapValues(v -> 1.0);
        KTable<Edge<Long>, Double> initialEdges = completeGraph.edges().mapValues(v -> 1.0);
        KGraph<Long, Double, Double> initialGraph =
            new KGraph<>(initialVertices, initialEdges, GraphSerialized.with(Serdes.Long(), Serdes.Double(), Serdes
                .Double()));
        KTable<Long, Long> vertexOutDegrees = initialGraph.outDegrees();
        KGraph<Long, Double, Double> joinedGraph = initialGraph
            .joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());
        KTable<Long, Tuple2<Double, Double>> vertices =
            joinedGraph.vertices().mapValues((k, v) -> new Tuple2<>(0.0, 0.0));
        KGraph<Long, Tuple2<Double, Double>, Double> graph =
            new KGraph<>(vertices, joinedGraph.edges(), GraphSerialized.with(joinedGraph.keySerde(), new KryoSerde<>
                (), Serdes.Double()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix, CLUSTER
                .bootstrapServers(),
            graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Void> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 50, (short) 1);
        state.get();

        Thread.sleep(2000);

        double resetProb = 0.15;
        double tol = 0.0001;
        Map<String, Object> configs = new HashMap<>();
        configs.put(PageRank.RESET_PROBABILITY, resetProb);
        configs.put(PageRank.TOLERANCE, tol);
        Optional<Double> initMsg = Optional.of(resetProb / (1.0 - resetProb));
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 50, (short) 1,
                configs, initMsg, new PageRank<>());
        props = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix, CLUSTER.bootstrapServers(),
            graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), props).streams();
        int maxIterations = 30;  //Integer.MAX_VALUE;
        GraphAlgorithmState<KTable<Long, Tuple2<Double, Double>>> ranks = algorithm.run(maxIterations);
        ranks.result().get();

        Thread.sleep(2000);

        Map<Long, Tuple2<Double, Double>> map = StreamUtils.mapFromStore(ranks.streams(), "solutionSetStore-" +
            suffix);

        log.debug("result: {}", map);

        for (long i = 0; i < 25; i++) {
            assertEquals(new Tuple2<>(0.9935138543444264, 0.0011446139392183863), map.get(i));
        }
    }

    @Test
    public void testGridPageRank() throws Exception {
        String suffix = "grid";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), IntegerSerializer.class,
            IntegerSerializer.class, new Properties()
        );
        KGraph<Long, Tuple2<Long, Long>, Long> gridGraph = GraphGenerators.gridGraph(builder, producerConfig, 10, 10);
        KTable<Edge<Long>, Double> initialEdges = gridGraph.edges().mapValues(v -> 1.0);
        KGraph<Long, Tuple2<Long, Long>, Double> initialGraph =
            new KGraph<>(gridGraph.vertices(), initialEdges,
                GraphSerialized.with(Serdes.Long(), new KryoSerde<>(), Serdes.Double()));
        KTable<Long, Long> vertexOutDegrees = initialGraph.outDegrees();
        KGraph<Long, Tuple2<Long, Long>, Double> joinedGraph = initialGraph
            .joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());
        KTable<Long, Tuple2<Double, Double>> vertices =
            joinedGraph.vertices().mapValues((k, v) -> new Tuple2<>(0.0, 0.0));
        KGraph<Long, Tuple2<Double, Double>, Double> graph =
            new KGraph<>(vertices, joinedGraph.edges(), GraphSerialized.with(joinedGraph.keySerde(), new KryoSerde<>
                (), Serdes.Double()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix, CLUSTER
                .bootstrapServers(),
            graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Void> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 50, (short) 1);
        state.get();

        Thread.sleep(2000);

        double resetProb = 0.15;
        double tol = 0.0001;
        Map<String, Object> configs = new HashMap<>();
        configs.put(PageRank.RESET_PROBABILITY, resetProb);
        configs.put(PageRank.TOLERANCE, tol);
        Optional<Double> initMsg = Optional.of(resetProb / (1.0 - resetProb));
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 50, (short) 1,
                configs, initMsg, new PageRank<>());
        props = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix, CLUSTER.bootstrapServers(),
            graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), props).streams();
        int maxIterations = 50;  //Integer.MAX_VALUE;
        GraphAlgorithmState<KTable<Long, Tuple2<Double, Double>>> ranks = algorithm.run(maxIterations);
        ranks.result().get();

        Thread.sleep(2000);

        Map<Long, Tuple2<Double, Double>> map = StreamUtils.mapFromStore(ranks.streams(), "solutionSetStore-" +
            suffix);

        log.debug("result: {}", map);

        String result = "{0=(0.15, 0.15), 1=(0.21375, 0.06375), 2=(0.24084375, 0.02709375), 3=(0.25235859374999997, " +
            "0.011514843749999976), 4=(0.25725240234374996, 0.004893808593749993), 5=(0.2593322709960937, " +
            "0.0020798686523437193), 6=(0.26021621517333976, 8.839441772460765E-4), 7=(0.26059189144866934, " +
            "3.756762753295839E-4), 8=(0.26075155386568444, 1.5966241701509398E-4), 9=(0.26081941039291584, " +
            "6.785652723140245E-5), 10=(0.21375, 0.06375), 11=(0.33168749999999997, 0.0541875), 12=" +
            "(0.39332578124999995, 0.03454453125000001), 13=(0.4244158593749999, 0.019575234374999972), 14=" +
            "(0.4397090112304687, 0.010399343261718763), 15=(0.447092544946289, 0.00530366506347657), 16=" +
            "(0.4506062230508422, 0.002629733927307143), 17=(0.45225919866229236, 0.0012772993361205853), 18=" +
            "(0.45302956982439013, 6.107087450826776E-4), 19=(0.5641763879611975, 2.595512166601033E-4), 20=" +
            "(0.24084375, 0.02709375), 21=(0.39332578124999995, 0.03454453125000001), 22=(0.48432691406249995, " +
            "0.029362851562500014), 23=(0.5362156787109374, 0.020798686523437526), 24=(0.5647679932250975, " +
            "0.013259162658691426), 25=(0.5800407287228392, 0.007889201781921429), 26=(0.5880249545038145, " +
            "0.004470547676422187), 27=(0.5921207650955954, 0.0024428349803307103), 28=(0.5941888923409938, " +
            "0.0012977560833007384), 29=(0.8820802090119402, 7.72164869563885E-4), 30=(0.25235859374999997, " +
            "0.011514843749999976), 31=(0.4244158593749999, 0.019575234374999972), 32=(0.5362156787109374, " +
            "0.020798686523437526), 33=(0.6057833269042968, 0.017678883544921864), 34=(0.6474843110549925, " +
            "0.013148669636535604), 35=(0.6716981419055784, 0.008941095352844264), 36=(0.685382315973992, " +
            "0.00569994828743825), 37=(0.6929388094545746, 0.003460682888801858), 38=(0.6970292732631166, " +
            "0.0020223365631436563), 39=(1.1960056187969734, 0.0015158331784652646), 40=(0.25725240234374996, " +
            "0.004893808593749993), 41=(0.4397090112304687, 0.010399343261718763), 42=(0.5647679932250975, " +
            "0.013259162658691426), 43=(0.6474843110549925, 0.013148669636535604), 44=(0.7003616643967435, " +
            "0.011176369191055247), 45=(0.7331254176784868, 0.008549922431157264), 46=(0.7528657868023035, " +
            "0.006056195055403113), 47=(0.7644669534091731, 0.0040446731262870905), 48=(0.771135896335723, " +
            "0.0025784791180080147), 49=(1.4943375319201095, 0.0023843118268489505), 50=(0.2593322709960937, " +
            "0.0020798686523437193), 51=(0.447092544946289, 0.00530366506347657), 52=(0.5800407287228392, " +
            "0.007889201781921429), 53=(0.6716981419055784, 0.008941095352844264), 54=(0.7331254176784868, " +
            "0.008549922431157264), 55=(0.7731566050267139, 0.007267434066483669), 56=(0.7985595165273324, " +
            "0.00566254237680186), 57=(0.8142862497230148, 0.004125566588812801), 58=(0.8238044120749636, " +
            "0.002849219425398841), 59=(1.7703037772639525, 0.00323758330861601), 60=(0.26021621517333976, " +
            "8.839441772460765E-4), 61=(0.4506062230508422, 0.002629733927307143), 62=(0.5880249545038145, " +
            "0.004470547676422187), 63=(0.685382315973992, 0.00569994828743825), 64=(0.7528657868023035, " +
            "0.006056195055403113), 65=(0.7985595165273324, 0.00566254237680186), 66=(0.8287755890482327, " +
            "0.00481316102028162), 67=(0.8483012814777801, 0.0037989592338650846), 68=(0.8606449197599159, " +
            "0.0028254759301871157), 69=(2.020532301572323, 0.003952773082652961), 70=(0.26059189144866934, " +
            "3.756762753295839E-4), 71=(0.45225919866229236, 0.0012772993361205853), 72=(0.5921207650955954, " +
            "0.0024428349803307103), 73=(0.6929388094545746, 0.003460682888801858), 74=(0.7644669534091731, " +
            "0.0040446731262870905), 75=(0.8142862497230148, 0.004125566588812801), 76=(0.8483012814777801, " +
            "0.0037989592338650846), 77=(0.8710560892561131, 0.0032291153487853386), 78=(0.8859729288318124, " +
            "0.0025732012935633097), 79=(2.2439909510899945, 0.004453467670019418), 80=(0.26075155386568444, " +
            "1.5966241701509398E-4), 81=(0.45302956982439013, 6.107087450826776E-4), 82=(0.5941888923409938, " +
            "0.0012977560833007384), 83=(0.6970292732631166, 0.0020223365631436563), 84=(0.771135896335723, " +
            "0.0025784791180080147), 85=(0.8238044120749636, 0.002849219425398841), 86=(0.8606449197599159, " +
            "0.0028254759301871157), 87=(0.8859729288318124, 0.0025732012935633097), 88=(0.9030769895070407, " +
            "0.0021872210995288466), 89=(2.4412000289669873, 0.004715016486816381), 90=(0.26081941039291584, " +
            "6.785652723140245E-5), 91=(0.5641763879611975, 2.595512166601033E-4), 92=(0.8820802090119402, " +
            "7.72164869563885E-4), 93=(1.1960056187969734, 0.0015158331784652646), 94=(1.4943375319201095, " +
            "0.0023843118268489505), 95=(1.7703037772639525, 0.00323758330861601), 96=(2.020532301572323, " +
            "0.003952773082652961), 97=(2.2439909510899945, 0.004453467670019418), 98=(2.4412000289669873, " +
            "0.004715016486816381), 99=(4.300040049243878, 0.008015528027588203)}";

        assertEquals(result, map.toString());
    }

    @Test
    public void testStarPageRank() throws Exception {
        String suffix = "star";
        StreamsBuilder builder = new StreamsBuilder();

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), IntegerSerializer.class,
            IntegerSerializer.class, new Properties()
        );
        KGraph<Long, Long, Long> starGraph = GraphGenerators.starGraph(builder, producerConfig, 100);
        KTable<Long, Double> initialVertices = starGraph.vertices().mapValues(v -> 1.0);
        KTable<Edge<Long>, Double> initialEdges = starGraph.edges().mapValues(v -> 1.0);
        KGraph<Long, Double, Double> initialGraph =
            new KGraph<>(initialVertices, initialEdges, GraphSerialized.with(Serdes.Long(), Serdes.Double(), Serdes
                .Double()));
        KTable<Long, Long> vertexOutDegrees = initialGraph.outDegrees();
        KGraph<Long, Double, Double> joinedGraph = initialGraph
            .joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());
        KTable<Long, Tuple2<Double, Double>> vertices =
            joinedGraph.vertices().mapValues((k, v) -> new Tuple2<>(0.0, 0.0));
        KGraph<Long, Tuple2<Double, Double>, Double> graph =
            new KGraph<>(vertices, joinedGraph.edges(), GraphSerialized.with(joinedGraph.keySerde(), new KryoSerde<>
                (), Serdes.Double()));

        Properties props = ClientUtils.streamsConfig("prepare-" + suffix, "prepare-client-" + suffix, CLUSTER
                .bootstrapServers(),
            graph.keySerde().getClass(), graph.vertexValueSerde().getClass());
        CompletableFuture<Void> state = GraphUtils.groupEdgesBySourceAndRepartition(builder, props, graph, "vertices-" + suffix, "edgesGroupedBySource-" + suffix, 50, (short) 1);
        state.get();

        Thread.sleep(2000);

        double resetProb = 0.15;
        double tol = 0.0001;
        Map<String, Object> configs = new HashMap<>();
        configs.put(PageRank.RESET_PROBABILITY, resetProb);
        configs.put(PageRank.TOLERANCE, tol);
        Optional<Double> initMsg = Optional.of(resetProb / (1.0 - resetProb));
        algorithm =
            new PregelGraphAlgorithm<>(null, "run-" + suffix, CLUSTER.bootstrapServers(),
                CLUSTER.zKConnectString(), "vertices-" + suffix, "edgesGroupedBySource-" + suffix, graph.serialized(),
                "solutionSet-" + suffix, "solutionSetStore-" + suffix, "workSet-" + suffix, 50, (short) 1,
                configs, initMsg, new PageRank<>());
        props = ClientUtils.streamsConfig("run-" + suffix, "run-client-" + suffix, CLUSTER.bootstrapServers(),
            graph.keySerde().getClass(), KryoSerde.class);
        KafkaStreams streams = algorithm.configure(new StreamsBuilder(), props).streams();
        int maxIterations = 2;  //Integer.MAX_VALUE;
        GraphAlgorithmState<KTable<Long, Tuple2<Double, Double>>> ranks = algorithm.run(maxIterations);
        ranks.result().get();

        Thread.sleep(2000);

        Map<Long, Tuple2<Double, Double>> map = StreamUtils.mapFromStore(ranks.streams(), "solutionSetStore-" +
            suffix);

        log.debug("result: {}", map);

        assertEquals(new Tuple2<>(12.77250000000002, 12.62250000000002), map.get(0L));
        for (long i = 1; i < 100; i++) {
            assertEquals(new Tuple2<>(0.15, 0.15), map.get(i));
        }
    }

    @After
    public void tearDown() throws Exception {
        algorithm.close();
    }

    private static final class InitVertices implements ValueMapper<Long, Double> {
        @Override
        public Double apply(Long id) {
            return Double.POSITIVE_INFINITY;
        }
    }

    private static final class InitWeights implements EdgeJoinFunction<Double, Long> {
        public Double edgeJoin(Double edgeValue, Long inputValue) {
            //return edgeValue / (double) inputValue;
            // TOOD fix
            return 1.0 / (double) inputValue;
        }
    }
}
