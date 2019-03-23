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

package io.kgraph.rest.server.graph;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.utils.ZKPaths;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.reactive.context.ReactiveWebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.codec.multipart.FormFieldPart;
import org.springframework.http.codec.multipart.Part;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ResponseStatusException;

import io.kgraph.GraphAlgorithmState;
import io.kgraph.GraphSerialized;
import io.kgraph.library.BreadthFirstSearch;
import io.kgraph.library.ConnectedComponents;
import io.kgraph.library.LabelPropagation;
import io.kgraph.library.LocalClusteringCoefficient;
import io.kgraph.library.MultipleSourceShortestPaths;
import io.kgraph.library.PageRank;
import io.kgraph.library.SingleSourceShortestPaths;
import io.kgraph.pregel.ComputeFunction;
import io.kgraph.pregel.PregelGraphAlgorithm;
import io.kgraph.pregel.ZKUtils;
import io.kgraph.rest.server.KafkaGraphsProperties;
import io.kgraph.tools.importer.GraphImporter;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.GraphUtils;
import io.kgraph.utils.KryoSerde;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@EnableConfigurationProperties(KafkaGraphsProperties.class)
public class GraphAlgorithmHandler<EV> implements ApplicationListener<ReactiveWebServerInitializedEvent> {

    private static final Logger log = LoggerFactory.getLogger(GraphAlgorithmHandler.class);

    private static final String X_KGRAPH_APPID = "X-KGraph-AppId";

    private static final Flux<Long> INTERVAL = Flux.interval(Duration.ofMillis(100), Duration.ofSeconds(2));

    private final KafkaGraphsProperties props;
    private final CuratorFramework curator;
    private final String host;
    private int port;
    private GroupMember group;
    private final ConcurrentMap<String, PregelGraphAlgorithm<Long, ?, ?, ?>> algorithms = new ConcurrentHashMap<>();

    public GraphAlgorithmHandler(KafkaGraphsProperties props, CuratorFramework curator) {
        this.props = props;
        this.curator = curator;
        this.host = getHostAddress();
    }

    @Override
    public void onApplicationEvent(final ReactiveWebServerInitializedEvent event) {
        this.port = event.getWebServer().getPort();
        this.group = new GroupMember(curator, ZKPaths.makePath(ZKUtils.GRAPHS_PATH, ZKUtils.GROUP), getHostAndPort());
        group.start();
    }

    public Mono<ServerResponse> importGraph(ServerRequest request) {
        return request.body(BodyExtractors.toMultipartData()).flatMap(parts -> {
            try {
                Map<String, Part> map = parts.toSingleValueMap();

                FilePart verticesFilePart = (FilePart) map.get("verticesFile");
                File verticesFile = new File(ClientUtils.tempDirectory(), verticesFilePart.filename());
                verticesFilePart.transferTo(verticesFile);

                FilePart edgesFilePart = (FilePart) map.get("edgesFile");
                File edgesFile = new File(ClientUtils.tempDirectory(), edgesFilePart.filename());
                edgesFilePart.transferTo(edgesFile);

                FormFieldPart verticesTopicPart = (FormFieldPart) map.get("verticesTopic");
                String verticesTopic = verticesTopicPart.value();

                FormFieldPart edgesTopicPart = (FormFieldPart) map.get("edgesTopic");
                String edgesTopic = edgesTopicPart.value();

                FormFieldPart useDoublePart = (FormFieldPart) map.get("useDouble");
                boolean useDouble = Boolean.parseBoolean(useDoublePart.value());

                FormFieldPart numPartitionsPart = (FormFieldPart) map.get("numPartitions");
                int numPartitions = Integer.parseInt(numPartitionsPart.value());

                FormFieldPart replicatorFactorPart = (FormFieldPart) map.get("replicationFactor");
                short replicationFactor = Short.parseShort(replicatorFactorPart.value());

                GraphImporter importer = new GraphImporter(props.getBootstrapServers(), verticesFile, edgesFile,
                    verticesTopic, edgesTopic, useDouble, numPartitions, replicationFactor
                );
                importer.call();
            } catch (NumberFormatException e) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid number", e);
            } catch (Exception e) {
                throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR);
            }

            return ServerResponse.ok().build();
        });
    }

    public Mono<ServerResponse> prepareGraph(ServerRequest request) {
        String appId = ClientUtils.generateRandomString(8);
        return request.bodyToMono(GroupEdgesBySourceRequest.class)
            .doOnNext(input -> {
                try {
                    StreamsBuilder builder = new StreamsBuilder();
                    Properties streamsConfig = streamsConfig(appId, props.getBootstrapServers(),
                        input.isValuesOfTypeDouble() ? Serdes.Double() : Serdes.Long()
                    );
                    CompletableFuture<Map<TopicPartition, Long>> future;
                    if (input.isValuesOfTypeDouble()) {
                        future = GraphUtils.groupEdgesBySourceAndRepartition(builder,
                            streamsConfig, input.getInitialVerticesTopic(), input.getInitialEdgesTopic(),
                            GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Double()),
                            input.getVerticesTopic(), input.getEdgesGroupedBySourceTopic(),
                            input.getNumPartitions(), input.getReplicationFactor()
                        );
                    } else {
                        future = GraphUtils.groupEdgesBySourceAndRepartition(builder,
                            streamsConfig, input.getInitialVerticesTopic(), input.getInitialEdgesTopic(),
                            GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long()),
                            input.getVerticesTopic(), input.getEdgesGroupedBySourceTopic(),
                            input.getNumPartitions(), input.getReplicationFactor()
                        );
                    }
                    if (!input.isAsync()) future.get();
                } catch (Exception e) {
                    throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR);
                }
            }).then(ServerResponse.ok().build());
    }

    public Mono<ServerResponse> configure(ServerRequest request) {
        List<String> appIdHeaders = request.headers().header(X_KGRAPH_APPID);
        String appId = appIdHeaders.isEmpty() ? ClientUtils.generateRandomString(8) : appIdHeaders.iterator().next();
        return request.bodyToMono(GraphAlgorithmCreateRequest.class)
            .doOnNext(input -> {
                PregelGraphAlgorithm<Long, ?, ?, ?> algorithm = getAlgorithm(appId, input);
                StreamsBuilder builder = new StreamsBuilder();
                Properties streamsConfig = streamsConfig(appId, props.getBootstrapServers(), new KryoSerde<>());
                algorithm.configure(builder, streamsConfig);
                algorithms.put(appId, algorithm);
            })
            .flatMapMany(input -> proxyConfigure(appIdHeaders.isEmpty()
                ? group.getCurrentMembers().keySet() : Collections.emptySet(), appId, input))
            .then(ServerResponse.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(new GraphAlgorithmId(appId)), GraphAlgorithmId.class));
    }

    @SuppressWarnings("unchecked")
    private PregelGraphAlgorithm<Long, ?, ?, ?> getAlgorithm(String appId, GraphAlgorithmCreateRequest input) {
        try {
            long srcVertexId;
            ComputeFunction<Long, ?, ?, ?> cf;
            Map<String, Object> configs = new HashMap<>();
            Optional<?> initMsg = Optional.empty();
            GraphSerialized<Long, ?, ?> graphSerialized;
            switch (input.getAlgorithm()) {
                case bfs:
                    cf = new BreadthFirstSearch<>();
                    srcVertexId = Long.parseLong(getParam(input.getParams(), "srcVertexId", true));
                    configs.put(BreadthFirstSearch.SRC_VERTEX_ID, srcVertexId);
                    if (input.isValuesOfTypeDouble()) {
                        graphSerialized = GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Double());
                    } else {
                        graphSerialized = GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long());
                    }
                    break;
                case wcc:
                    cf = new ConnectedComponents<>();
                    if (input.isValuesOfTypeDouble()) {
                        graphSerialized = GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Double());
                    } else {
                        graphSerialized = GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long());
                    }
                    break;
                case lcc:
                    cf = new LocalClusteringCoefficient();
                    graphSerialized = GraphSerialized.with(Serdes.Long(), Serdes.Double(), Serdes.Double());
                    break;
                case lp:
                    cf = new LabelPropagation<>();
                    if (input.isValuesOfTypeDouble()) {
                        graphSerialized = GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Double());
                    } else {
                        graphSerialized = GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long());
                    }
                    break;
                case mssp:
                    cf = new MultipleSourceShortestPaths();
                    String[] values = getParam(input.getParams(), "landmarkVertexIds", true).split(",");
                    Set<Long> landmarkVertexIds = Arrays.stream(values).map((Long::parseLong)).collect(Collectors.toSet());
                    configs.put(MultipleSourceShortestPaths.LANDMARK_VERTEX_IDS, landmarkVertexIds);
                    graphSerialized = GraphSerialized.with(Serdes.Long(), new KryoSerde<>(), Serdes.Double());
                    break;
                case pagerank:
                    cf = new PageRank<>();
                    double tolerance = Double.parseDouble(getParam(input.getParams(), "tolerance", true));
                    double resetProbability = Double.parseDouble(getParam(input.getParams(), "resetProbability", true));
                    String srcVertexIdStr = getParam(input.getParams(), "srcVertexId", false);
                    configs.put(PageRank.TOLERANCE, tolerance);
                    configs.put(PageRank.RESET_PROBABILITY, resetProbability);
                    if (srcVertexIdStr != null) {
                        configs.put(PageRank.SRC_VERTEX_ID, Long.parseLong(srcVertexIdStr));
                        initMsg = Optional.of(0.0);
                    } else {
                        initMsg = Optional.of(resetProbability / (1.0 - resetProbability));
                    }
                    graphSerialized = GraphSerialized.with(Serdes.Long(), new KryoSerde<>(), Serdes.Double());
                    break;
                case sssp:
                    cf = new SingleSourceShortestPaths();
                    srcVertexId = Long.parseLong(getParam(input.getParams(), "srcVertexId", true));
                    configs.put(SingleSourceShortestPaths.SRC_VERTEX_ID, srcVertexId);
                    graphSerialized = GraphSerialized.with(Serdes.Long(), Serdes.Double(), Serdes.Double());
                    break;
                default:
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid algorithm: " + input.getAlgorithm());
            }
            return new PregelGraphAlgorithm<>(
                getHostAndPort(),
                appId,
                props.getBootstrapServers(),
                curator,
                input.getVerticesTopic(),
                input.getEdgesGroupedBySourceTopic(),
                Collections.emptyMap(),
                (GraphSerialized<Long, Object, Object>) graphSerialized,
                input.getNumPartitions(),
                input.getReplicationFactor(),
                configs,
                (Optional<Object>) initMsg,
                (ComputeFunction<Long, Object, Object, Object>) cf);
        } catch (NumberFormatException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid number", e);
        }
    }

    private String getParam(Map<String, String> params, String key, boolean isRequired) {
        String value = params.get(key);
        if (isRequired && value == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Missing param: " + key);
        }
        return value;
    }

    private Flux<GraphAlgorithmId> proxyConfigure(Set<String> groupMembers, String appId, GraphAlgorithmCreateRequest input) {
        Flux<GraphAlgorithmId> flux = Flux.fromIterable(groupMembers)
            .filter(s -> !s.equals(getHostAndPort()))
            .flatMap(s -> {
                log.debug("proxy configure to {}", s);
                WebClient client = WebClient.create("http://" + s);
                return client.post().uri("/pregel")
                    .accept(MediaType.APPLICATION_JSON)
                    .header(X_KGRAPH_APPID, appId)
                    .body(Mono.just(input), GraphAlgorithmCreateRequest.class)
                    .retrieve()
                    .bodyToMono(GraphAlgorithmId.class);
            });
        return flux;
    }

    public Mono<ServerResponse> state(ServerRequest request) {
        String appId = request.pathVariable("id");
        PregelGraphAlgorithm<Long, ?, ?, ?> algorithm = algorithms.get(appId);
        return ServerResponse.ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(Mono.just(new GraphAlgorithmStatus(algorithm.state())), GraphAlgorithmStatus.class);
    }

    public Mono<ServerResponse> run(ServerRequest request) {
        List<String> appIdHeaders = request.headers().header(X_KGRAPH_APPID);
        String appId = request.pathVariable("id");
        return request.bodyToMono(GraphAlgorithmRunRequest.class)
            .flatMapMany(input -> {
                log.debug("num iterations: {}", input.getNumIterations());
                PregelGraphAlgorithm<Long, ?, ?, ?> algorithm = algorithms.get(appId);
                GraphAlgorithmState state = algorithm.run(input.getNumIterations());
                GraphAlgorithmStatus status = new GraphAlgorithmStatus(state);
                Flux<GraphAlgorithmStatus> states =
                    proxyRun(appIdHeaders.isEmpty() ? group.getCurrentMembers().keySet() : Collections.emptySet(), appId, input);
                return Mono.just(status).mergeWith(states);
            })
            .reduce((state1, state2) -> state1)
            .flatMap(state ->
                ServerResponse.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Mono.just(state), GraphAlgorithmStatus.class)
            );

    }

    private Flux<GraphAlgorithmStatus> proxyRun(Set<String> groupMembers, String appId, GraphAlgorithmRunRequest input) {
        Flux<GraphAlgorithmStatus> flux = Flux.fromIterable(groupMembers)
            .filter(s -> !s.equals(getHostAndPort()))
            .flatMap(s -> {
                log.debug("proxy run to {}", s);
                WebClient client = WebClient.create("http://" + s);
                return client.post().uri("/pregel/" + appId)
                    .accept(MediaType.APPLICATION_JSON)
                    .header(X_KGRAPH_APPID, appId)
                    .body(Mono.just(input), GraphAlgorithmRunRequest.class)
                    .retrieve()
                    .bodyToMono(GraphAlgorithmStatus.class);
            });
        return flux;
    }

    public Mono<ServerResponse> result(ServerRequest request) {
        List<String> appIdHeaders = request.headers().header(X_KGRAPH_APPID);
        String appId = request.pathVariable("id");
        PregelGraphAlgorithm<Long, ?, ?, ?> algorithm = algorithms.get(appId);
        Flux<String> body = Flux.fromIterable(algorithm.result()).map(kv -> {
            log.trace("result: ({}, {})", kv.key, kv.value);
            return kv.key + " " + kv.value;
        });
        body = proxyResult(appIdHeaders.isEmpty() ? group.getCurrentMembers().keySet() : Collections.emptySet(), appId, body);
        return ServerResponse.ok()
            .contentType(MediaType.TEXT_EVENT_STREAM)
            .body(BodyInserters.fromPublisher(body, String.class));
    }

    private Flux<String> proxyResult(Set<String> groupMembers, String appId, Flux<String> body) {
        Flux<String> flux = groupMembers.stream()
            .filter(s -> !s.equals(getHostAndPort()))
            .map(s -> {
                log.debug("proxy result to {}", s);
                WebClient client = WebClient.create("http://" + s);
                return client.get().uri("/pregel/" + appId + "/result")
                    .accept(MediaType.TEXT_EVENT_STREAM)
                    .header(X_KGRAPH_APPID, appId)
                    .retrieve()
                    .bodyToFlux(String.class);
            })
            .reduce(body, Flux::mergeWith);
        return flux;
    }

    public Mono<ServerResponse> delete(ServerRequest request) {
        List<String> appIdHeaders = request.headers().header(X_KGRAPH_APPID);
        String appId = request.pathVariable("id");
        PregelGraphAlgorithm<Long, ?, ?, ?> algorithm = algorithms.remove(appId);
        algorithm.close();
        return proxyDelete(appIdHeaders.isEmpty() ? group.getCurrentMembers().keySet() : Collections.emptySet(), appId)
            .then(ServerResponse.noContent().build());
    }

    private Flux<Void> proxyDelete(Set<String> groupMembers, String appId) {
        Flux<Void> flux = Flux.fromIterable(groupMembers)
            .filter(s -> !s.equals(getHostAndPort()))
            .flatMap(s -> {
                log.debug("proxy delete to {}", s);
                WebClient client = WebClient.create("http://" + s);
                return client.delete().uri("/pregel/" + appId)
                    .accept(MediaType.APPLICATION_JSON)
                    .header(X_KGRAPH_APPID, appId)
                    .retrieve()
                    .bodyToMono(Void.class);
            });
        return flux;
    }

    public static Properties streamsConfig(String appId, String bootstrapServers, Serde<?> valueSerde) {
        final Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfig.put(StreamsConfig.CLIENT_ID_CONFIG, appId + "-client");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde.getClass().getName());
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, ClientUtils.tempDirectory().getAbsolutePath());
        return streamsConfig;
    }

    public String getHostAndPort() {
        return host + ":" + port;
    }

    public String getHostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public int getPort() {
        return port;
    }
}
