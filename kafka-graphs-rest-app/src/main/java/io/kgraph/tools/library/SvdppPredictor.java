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

package io.kgraph.tools.library;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.jblas.FloatMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import io.kgraph.library.basic.EdgeCount;
import io.kgraph.library.cf.CfLongId;
import io.kgraph.library.cf.Svdpp;
import io.kgraph.rest.server.graph.GraphAlgorithmResultRequest;
import io.kgraph.rest.server.graph.GraphAlgorithmStatus;
import io.kgraph.rest.server.graph.KeyValue;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import reactor.core.publisher.Mono;

@CommandLine.Command(description = "Predicts a rating for a given user and item.",
    name = "svdpp-predict", mixinStandardHelpOptions = true, version = "svdpp-predict 1.0")
public class SvdppPredictor implements Callable<Void> {

    private static final Logger log = LoggerFactory.getLogger(SvdppPredictor.class);

    @Parameters(index = "0", description = "Rest app server.")
    private String restAppServer;

    @Parameters(index = "1", description = "Pregel graph ID.")
    private String id;

    @Option(names = {"-u", "--user"}, description = "The user id.")
    private Long user;

    @Option(names = {"-i", "--item"}, description = "The item id.")
    private Long item;

    public SvdppPredictor() {
    }

    public SvdppPredictor(String restAppServer,
                          Long user,
                          Long item) {
        this.restAppServer = restAppServer;
        this.user = user;
        this.item = item;
    }

    @Override
    public Void call() {
        try {
            String baseUrl = restAppServer;
            if (!baseUrl.startsWith("http://")) {
                baseUrl = "http://" + baseUrl;
            }
            WebClient client = WebClient.create(baseUrl);

            GraphAlgorithmStatus status = client
                .get()
                .uri("/pregel/{id}", id)
                .retrieve()
                .bodyToMono(GraphAlgorithmStatus.class)
                .block();
            if (status == null) {
                log.error("Error: no status found");
                return null;
            }

            double overallRating = Double.parseDouble(status.getAggregates().get(Svdpp.OVERALL_RATING_AGGREGATOR));
            long numEdges = Long.parseLong(status.getAggregates().get(EdgeCount.EDGE_COUNT_AGGREGATOR));
            double meanRating =  overallRating / (numEdges * 2);

            List<Float> userFloats = getFloats(client, (byte) 0, user);
            Float userBaseline = userFloats.remove(0);
            FloatMatrix userFactors = new FloatMatrix(userFloats);

            List<Float> itemFloats = getFloats(client, (byte) 1, item);
            Float itemBaseline = itemFloats.remove(0);
            FloatMatrix itemFactors = new FloatMatrix(itemFloats);

            double predictedRating = meanRating + userBaseline + itemBaseline +
                itemFactors.dot(userFactors);
            log.info("Predicted rating: " + predictedRating);
            System.out.println("Predicted rating: " + predictedRating);
        } catch (WebClientResponseException e) {
            log.error("Error: " + e.getMessage());
        }

        return null;
    }

    private List<Float> getFloats(WebClient client, byte type, long id) {
        GraphAlgorithmResultRequest key = new GraphAlgorithmResultRequest();
        key.setKey(new CfLongId(type, id).toString());
        KeyValue result = client
            .post()
            .uri("/pregel/{id}/result", this.id)
            .accept(MediaType.TEXT_EVENT_STREAM)
            .body(Mono.just(key), GraphAlgorithmResultRequest.class)
            .retrieve()
            .bodyToFlux(KeyValue.class)
            .next()
            .block();
        if (result == null) {
            return Collections.emptyList();
        }

        String[] values = result.getValue().split("(\\(|\\)|\\[|\\]|,\\s)");
        return Arrays.stream(values)
            .filter(s -> !s.isEmpty())
            .map(Float::parseFloat)
            .collect(Collectors.toList());
    }

    public static void main(String[] args) {
        CommandLine.call(new SvdppPredictor(), args);
    }
}
