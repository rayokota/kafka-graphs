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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.FluxExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.util.MultiValueMap;

import io.kgraph.GraphAlgorithmState;
import io.kgraph.library.GraphAlgorithmType;
import io.kgraph.library.cf.CfLongId;
import io.kgraph.library.cf.EdgeCfLongIdFloatValueParser;
import io.kgraph.library.cf.Svdpp;
import io.kgraph.rest.server.KafkaGraphsApplication;
import io.kgraph.rest.server.utils.EdgeLongIdLongValueParser;
import io.kgraph.rest.server.utils.VertexLongIdLongValueParser;

@RunWith(SpringRunner.class)
@AutoConfigureWebTestClient(timeout = "36000")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = KafkaGraphsApplication.class)
public class GraphIntegrationTest {

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1) {
        @Override
        public void start() throws IOException, InterruptedException {
            super.start();
            System.setProperty("spring.embedded.kafka.brokers", bootstrapServers());
            System.setProperty("spring.embedded.zookeeper.connect", zKConnectString());
        }
    };

    @Autowired
    private WebTestClient webTestClient;

    @Test
    public void testConnectedComponents() {
        webTestClient
            .post()
            .uri("/import")
            .syncBody(generateCCBody())
            .exchange()
            .expectStatus().isOk()
            .expectBody(Void.class);

        GroupEdgesBySourceRequest prepareRequest = new GroupEdgesBySourceRequest();
        prepareRequest.setAlgorithm(GraphAlgorithmType.wcc);
        prepareRequest.setInitialVerticesTopic("initial-cc-vertices");
        prepareRequest.setInitialEdgesTopic("initial-cc-edges");
        prepareRequest.setVerticesTopic("new-cc-vertices");
        prepareRequest.setEdgesGroupedBySourceTopic("new-cc-edges");
        prepareRequest.setAsync(false);

        webTestClient
            .post()
            .uri("/prepare")
            .contentType(MediaType.APPLICATION_JSON)
            .syncBody(prepareRequest)
            .exchange()
            .expectStatus().isOk()
            .expectBody(Void.class);

        GraphAlgorithmCreateRequest createRequest = new GraphAlgorithmCreateRequest();
        createRequest.setAlgorithm(GraphAlgorithmType.wcc);
        createRequest.setVerticesTopic("new-cc-vertices");
        createRequest.setEdgesGroupedBySourceTopic("new-cc-edges");

        EntityExchangeResult<GraphAlgorithmId> createResponse = webTestClient
            .post()
            .uri("/pregel")
            .contentType(MediaType.APPLICATION_JSON)
            .syncBody(createRequest)
            .exchange()
            .expectStatus().isOk()
            .expectBody(GraphAlgorithmId.class)
            .returnResult();
        String id = createResponse.getResponseBody().getId();

        GraphAlgorithmRunRequest runRequest = new GraphAlgorithmRunRequest();

        EntityExchangeResult<GraphAlgorithmStatus> runResponse = webTestClient
            .post()
            .uri("/pregel/{id}", id)
            .contentType(MediaType.APPLICATION_JSON)
            .syncBody(runRequest)
            .exchange()
            .expectStatus().isOk()
            .expectBody(GraphAlgorithmStatus.class)
            .returnResult();

        GraphAlgorithmState.State state = GraphAlgorithmState.State.RUNNING;
        while (state == GraphAlgorithmState.State.RUNNING) {
            EntityExchangeResult<GraphAlgorithmStatus> statusResponse = webTestClient
                .get()
                .uri("/pregel/{id}", id)
                .exchange()
                .expectStatus().isOk()
                .expectBody(GraphAlgorithmStatus.class)
                .returnResult();
            state = statusResponse.getResponseBody().getState();
        }

        FluxExchangeResult<KeyValue> result = webTestClient
            .get()
            .uri("/pregel/{id}/result", id)
            .accept(MediaType.TEXT_EVENT_STREAM)
            .exchange()
            .expectStatus().isOk()
            .returnResult(KeyValue.class);

        Map<String, String> map = result.getResponseBody().collectMap(KeyValue::getKey, KeyValue::getValue).block();
        for (int i = 0; i < 10; i++) {
            assertEquals("0", map.get(String.valueOf(i)));
        }
        for (int i = 10; i < 21; i++) {
            assertEquals("10", map.get(String.valueOf(i)));
        }
    }

    private MultiValueMap<String, HttpEntity<?>> generateCCBody() {
        MultipartBodyBuilder builder = new MultipartBodyBuilder();
        builder.part("verticesTopic", "initial-cc-vertices");
        builder.part("edgesTopic", "initial-cc-edges");
        builder.part("vertexFile", new ClassPathResource("vertices_simple.txt"));
        builder.part("edgeFile", new ClassPathResource("edges_simple.txt"));
        builder.part("vertexParser", VertexLongIdLongValueParser.class.getName());
        builder.part("edgeParser", EdgeLongIdLongValueParser.class.getName());
        builder.part("keySerializer", LongSerializer.class.getName());
        builder.part("vertexValueSerializer", LongSerializer.class.getName());
        builder.part("edgeValueSerializer", LongSerializer.class.getName());
        builder.part("numPartitions", "50");
        builder.part("replicationFactor", "1");
        return builder.build();
    }

    @Test
    public void testSvdpp() {
        webTestClient
            .post()
            .uri("/import")
            .syncBody(generateSvdppBody())
            .exchange()
            .expectStatus().isOk()
            .expectBody(Void.class);

        GroupEdgesBySourceRequest prepareRequest = new GroupEdgesBySourceRequest();
        prepareRequest.setAlgorithm(GraphAlgorithmType.svdpp);
        prepareRequest.setInitialEdgesTopic("initial-svdpp-edges");
        prepareRequest.setVerticesTopic("new-svdpp-vertices");
        prepareRequest.setEdgesGroupedBySourceTopic("new-svdpp-edges");
        prepareRequest.setAsync(false);

        webTestClient
            .post()
            .uri("/prepare")
            .contentType(MediaType.APPLICATION_JSON)
            .syncBody(prepareRequest)
            .exchange()
            .expectStatus().isOk()
            .expectBody(Void.class);

        Map<String, String> params = new HashMap<>();
        params.put(Svdpp.RANDOM_SEED, "0");
        params.put(Svdpp.ITERATIONS, "3");
        GraphAlgorithmCreateRequest createRequest = new GraphAlgorithmCreateRequest();
        createRequest.setConfigs(params);
        createRequest.setAlgorithm(GraphAlgorithmType.svdpp);
        createRequest.setVerticesTopic("new-svdpp-vertices");
        createRequest.setEdgesGroupedBySourceTopic("new-svdpp-edges");

        EntityExchangeResult<GraphAlgorithmId> createResponse = webTestClient
            .post()
            .uri("/pregel")
            .contentType(MediaType.APPLICATION_JSON)
            .syncBody(createRequest)
            .exchange()
            .expectStatus().isOk()
            .expectBody(GraphAlgorithmId.class)
            .returnResult();
        String id = createResponse.getResponseBody().getId();

        GraphAlgorithmRunRequest runRequest = new GraphAlgorithmRunRequest();

        EntityExchangeResult<GraphAlgorithmStatus> runResponse = webTestClient
            .post()
            .uri("/pregel/{id}", id)
            .contentType(MediaType.APPLICATION_JSON)
            .syncBody(runRequest)
            .exchange()
            .expectStatus().isOk()
            .expectBody(GraphAlgorithmStatus.class)
            .returnResult();

        GraphAlgorithmState.State state = GraphAlgorithmState.State.RUNNING;
        while (state == GraphAlgorithmState.State.RUNNING) {
            EntityExchangeResult<GraphAlgorithmStatus> statusResponse = webTestClient
                .get()
                .uri("/pregel/{id}", id)
                .exchange()
                .expectStatus().isOk()
                .expectBody(GraphAlgorithmStatus.class)
                .returnResult();
            state = statusResponse.getResponseBody().getState();
        }

        FluxExchangeResult<KeyValue> result = webTestClient
            .get()
            .uri("/pregel/{id}/result", id)
            .accept(MediaType.TEXT_EVENT_STREAM)
            .exchange()
            .expectStatus().isOk()
            .returnResult(KeyValue.class);

        NavigableMap<CfLongId, String> map = (NavigableMap<CfLongId, String>) result.getResponseBody().collectMap(
            kv -> new CfLongId(kv.getKey()),
            KeyValue::getValue,
            TreeMap::new
        ).block();
        assertEquals("(1, 0)=(0.11611404, [0.006397, 0.008010])", map.firstEntry().toString());
        assertEquals("(20, 1)=(0.6374174, [0.007310, 0.002405])", map.lastEntry().toString());
    }

    private MultiValueMap<String, HttpEntity<?>> generateSvdppBody() {
        MultipartBodyBuilder builder = new MultipartBodyBuilder();
        builder.part("edgesTopic", "initial-svdpp-edges");
        builder.part("edgeFile", new ClassPathResource("ratings_simple.txt"));
        builder.part("edgeParser", EdgeCfLongIdFloatValueParser.class.getName());
        builder.part("edgeValueSerializer", FloatSerializer.class.getName());
        builder.part("numPartitions", "50");
        builder.part("replicationFactor", "1");
        return builder.build();
    }
}
