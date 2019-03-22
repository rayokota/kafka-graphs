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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.Processor;
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

    public static <K, VV, EV> CompletableFuture<Map<TopicPartition, Long>> groupEdgesBySourceAndRepartition(
        StreamsBuilder builder,
        Properties streamsConfig,
        String initialVerticesTopic,
        String initialEdgesTopic,
        GraphSerialized<K, VV, EV> serialized,
        String verticesTopic,
        String edgesGroupedBySourceTopic,
        int numPartitions,
        short replicationFactor
    ) {
        KGraph<K, VV, EV> graph = new KGraph<>(
            builder.table(initialVerticesTopic, Consumed.with(serialized.keySerde(), serialized.vertexValueSerde())),
            builder.table(initialEdgesTopic, Consumed.with(new KryoSerde<>(), serialized.edgeValueSerde())),
            serialized);
        return groupEdgesBySourceAndRepartition(builder, streamsConfig, graph, verticesTopic, edgesGroupedBySourceTopic, numPartitions, replicationFactor);
    }

    public static <K, VV, EV> CompletableFuture<Map<TopicPartition, Long>> groupEdgesBySourceAndRepartition(
        StreamsBuilder builder,
        Properties streamsConfig,
        KGraph<K, VV, EV> graph,
        String verticesTopic,
        String edgesGroupedBySourceTopic,
        int numPartitions,
        short replicationFactor
    ) {
        log.info("Started loading graph");

        ClientUtils.createTopic(verticesTopic, numPartitions, replicationFactor, streamsConfig);
        ClientUtils.createTopic(edgesGroupedBySourceTopic, numPartitions, replicationFactor, streamsConfig);

        Map<TopicPartition, Long> lastWrittenOffsets = new ConcurrentHashMap<>();
        AtomicLong lastWriteMs = new AtomicLong(0);

        graph.vertices()
            .toStream()
            .process(() -> new SendMessages<K, VV>(verticesTopic, graph.keySerde(),
                graph.vertexValueSerde(), streamsConfig, lastWrittenOffsets, lastWriteMs));
        graph.edgesGroupedBySource()
            .toStream()
            .mapValues(v -> StreamSupport.stream(v.spliterator(), false)
                .collect(Collectors.toMap(EdgeWithValue::target, EdgeWithValue::value)))
            .process(() -> new SendMessages<K, Map<K, EV>>(edgesGroupedBySourceTopic,
                graph.keySerde(), new KryoSerde<>(), streamsConfig, lastWrittenOffsets, lastWriteMs));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();

        CompletableFuture<Map<TopicPartition, Long>> future = new CompletableFuture<>();
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        // TODO make interval configurable
        ScheduledFuture scheduledFuture = executor.scheduleWithFixedDelay(() -> {
            long lastWrite = lastWriteMs.get();
            if (lastWrite > 0 && System.currentTimeMillis() - lastWrite > 10000) {
                //System.out.println("Complt " + lastWrite + " " + System.currentTimeMillis());
                streams.close();  // will flush/close all producers
                log.info("Last written " + lastWrittenOffsets);
                future.complete(lastWrittenOffsets);
                log.info("Finished loading graph");
            } else {
                //System.out.println("Cancel " + lastWrite + " " + System.currentTimeMillis());
            }
        }, 0, 500, TimeUnit.MILLISECONDS);

        return future.whenCompleteAsync((v, t) -> {
            scheduledFuture.cancel(true);
            executor.shutdown();
        });
    }

    private static final class SendMessages<K, V> implements Processor<K, V> {

        private final String topic;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;
        private final Properties streamsConfig;
        private final Map<TopicPartition, Long> lastWrittenOffsets;
        private final AtomicLong lastWriteMs;
        private Producer<K, V> producer;

        public SendMessages(String topic, Serde<K> keySerde,
                            Serde<V> valueSerde, Properties streamsConfig,
                            Map<TopicPartition, Long> lastWrittenOffsets,
                            AtomicLong lastWriteMs
        ) {
            this.topic = topic;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.streamsConfig = streamsConfig;
            this.lastWrittenOffsets = lastWrittenOffsets;
            this.lastWriteMs = lastWriteMs;
        }

        @Override
        public void init(final ProcessorContext context) {
            Properties producerConfig = ClientUtils.producerConfig(
                streamsConfig.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG),
                keySerde.serializer().getClass(), valueSerde.serializer().getClass(),
                streamsConfig
            );
            String clientId = "pregel-" + context.taskId();
            producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId + "-producer");
            this.producer = new KafkaProducer<>(producerConfig);
        }

        @Override
        public void process(final K readOnlyKey, final V value) {
            try {
                ProducerRecord<K, V> producerRecord =
                    new ProducerRecord<>(topic, readOnlyKey, value);
                producer.send(producerRecord, (metadata, error) -> {
                    if (error == null) {
                        try {
                            lastWrittenOffsets.put(
                                new TopicPartition(metadata.topic(), metadata.partition()),
                                metadata.offset()
                            );
                        } catch (Exception e) {
                            throw toRuntimeException(e);
                        }
                    }
                }).get();
                lastWriteMs.set(System.currentTimeMillis());
            } catch (Exception e) {
                throw toRuntimeException(e);
            }
        }

        @Override
        public void close() {
            producer.close();
        }
    }

    private static RuntimeException toRuntimeException(Exception e) {
        return e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
    }
}
