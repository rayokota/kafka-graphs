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

package io.kgraph.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kgraph.Edge;
import io.kgraph.EdgeWithValue;
import io.kgraph.GraphSerialized;
import io.kgraph.KGraph;

public class GraphUtils {
    private static final Logger log = LoggerFactory.getLogger(GraphUtils.class);

    public static <T extends Number> void verticesToTopic(
        InputStream inputStream,
        Function<String, T> valueParser,
        Serializer<T> valueSerializer,
        Properties props,
        String topic,
        int numPartitions,
        short replicationFactor
    ) throws IOException {
        ClientUtils.createTopic(topic, numPartitions, replicationFactor, props);
        try (BufferedReader reader =
                 new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
             Producer<Long, T> producer = new KafkaProducer<>(props, new LongSerializer(), valueSerializer)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.trim().split("\\s");
                long id = Long.parseLong(tokens[0]);
                log.trace("read vertex: {}", id);
                T value = tokens.length > 1 ? valueParser.apply(tokens[1]) : null;
                ProducerRecord<Long, T> producerRecord = new ProducerRecord<>(topic, id, value);
                producer.send(producerRecord);
            }
            producer.flush();
        }
    }

    public static <T extends Number> void edgesToTopic(
        InputStream inputStream,
        Function<String, T> valueParser,
        Serializer<T> valueSerializer,
        Properties props,
        String topic,
        int numPartitions,
        short replicationFactor
    ) throws IOException {
        ClientUtils.createTopic(topic, numPartitions, replicationFactor, props);
        try (BufferedReader reader =
                 new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
             Producer<Edge<Long>, T> producer = new KafkaProducer<>(props, new KryoSerializer<>(), valueSerializer)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.trim().split("\\s");
                long sourceId = Long.parseLong(tokens[0]);
                long targetId = Long.parseLong(tokens[1]);
                log.trace("read edge: ({}, {})", sourceId, targetId);
                T value = tokens.length > 2 ? valueParser.apply(tokens[2]) : null;
                ProducerRecord<Edge<Long>, T> producerRecord = new ProducerRecord<>(topic, new Edge<>(sourceId, targetId), value);
                producer.send(producerRecord);
            }
            producer.flush();
        }
    }

    public static <T extends Number> void verticesToFile(
        KTable<Long, T> vertices,
        String fileName) {
        vertices.toStream().print(Printed.<Long, T>toFile(fileName).withKeyValueMapper((k, v) -> String.format("%d %f", k , v)));
    }

    public static <K, VV, EV> CompletableFuture<Void> groupEdgesBySourceAndRepartition(StreamsBuilder builder,
                                                                                       Properties streamsConfig,
                                                                                       String initialVerticesTopic,
                                                                                       String initialEdgesTopic,
                                                                                       GraphSerialized<K, VV, EV> serialized,
                                                                                       String verticesTopic,
                                                                                       String edgesGroupedBySourceTopic,
                                                                                       int numPartitions,
                                                                                       short replicationFactor) {
        KGraph<K, VV, EV> graph = new KGraph<>(
            builder.table(initialVerticesTopic, Consumed.with(serialized.keySerde(), serialized.vertexValueSerde())),
            builder.table(initialEdgesTopic, Consumed.with(new KryoSerde<>(), serialized.edgeValueSerde())),
            serialized);
        return groupEdgesBySourceAndRepartition(builder, streamsConfig, graph, verticesTopic, edgesGroupedBySourceTopic, numPartitions, replicationFactor);
    }

    public static <K, VV, EV> CompletableFuture<Void> groupEdgesBySourceAndRepartition(StreamsBuilder builder,
                                                                                       Properties streamsConfig,
                                                                                       KGraph<K, VV, EV> graph,
                                                                                       String verticesTopic,
                                                                                       String edgesGroupedBySourceTopic,
                                                                                       int numPartitions,
                                                                                       short replicationFactor) {
        log.debug("Started loading graph");

        ClientUtils.createTopic(verticesTopic, numPartitions, replicationFactor, streamsConfig);
        ClientUtils.createTopic(edgesGroupedBySourceTopic, numPartitions, replicationFactor, streamsConfig);

        CompletableFuture<Boolean> verticesFuture = new CompletableFuture<>();
        CompletableFuture<Boolean> edgesFuture = new CompletableFuture<>();

        graph.vertices()
            .toStream()
            .transformValues(() -> new EndOfBatchCheck<>(verticesFuture))
            .to(verticesTopic, Produced.with(graph.keySerde(), graph.vertexValueSerde()));
        graph.edgesGroupedBySource()
            .toStream()
            .mapValues(v -> StreamSupport.stream(v.spliterator(), false)
                .collect(Collectors.toMap(EdgeWithValue::target, EdgeWithValue::value)))
            .transformValues(() -> new EndOfBatchCheck<>(edgesFuture))
            .to(edgesGroupedBySourceTopic, Produced.with(graph.keySerde(), new KryoSerde<>()));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();

        return CompletableFuture.allOf(verticesFuture, edgesFuture).thenRun(() -> {
            log.debug("Finished loading graph");
            streams.close();
        });
    }

    private static final class EndOfBatchCheck<V> implements ValueTransformer<V, V> {

        private final CompletableFuture<Boolean> future;
        private ProcessorContext context;
        private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        private ScheduledFuture<Boolean> scheduledFuture = null;

        public EndOfBatchCheck(CompletableFuture<Boolean> future) {
            this.future = future;
        }

        @Override
        public void init(final ProcessorContext context) {
            this.context = context;

            // TODO make interval configurable
            this.context.schedule(Duration.ofMillis(500), PunctuationType.STREAM_TIME, (timestamp) -> {
                if (scheduledFuture != null) {
                    scheduledFuture.cancel(false);
                }
                // Assume stream is done if no activity after 5 seconds
                scheduledFuture = executor.schedule(() -> {
                    this.context.commit();
                    return future.complete(true);
                }, 5000, TimeUnit.MILLISECONDS);
            });
        }

        @Override
        public V transform(final V value) {
            return value;
        }


        @Override
        public void close() {
        }
    }
}
