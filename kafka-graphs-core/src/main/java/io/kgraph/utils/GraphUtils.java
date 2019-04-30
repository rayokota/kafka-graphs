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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kgraph.Edge;
import io.kgraph.EdgeWithValue;
import io.kgraph.GraphSerialized;
import io.kgraph.KGraph;
import io.kgraph.VertexWithValue;

public class GraphUtils {
    private static final Logger log = LoggerFactory.getLogger(GraphUtils.class);

    public static <K, V> void verticesToTopic(
        InputStream inputStream,
        Parser<K> keyParser,
        Parser<V> valueParser,
        Serializer<K> keySerializer,
        Serializer<V> valueSerializer,
        Properties props,
        String topic,
        int numPartitions,
        short replicationFactor
    ) throws IOException {
        verticesToTopic(inputStream, new Parsers.VertexParser<>(keyParser, valueParser),
            keySerializer, valueSerializer, props, topic, numPartitions, replicationFactor);
    }

    public static <K, V> void verticesToTopic(
        InputStream inputStream,
        Parser<VertexWithValue<K, V>> vertexParser,
        Serializer<K> keySerializer,
        Serializer<V> valueSerializer,
        Properties props,
        String topic,
        int numPartitions,
        short replicationFactor
    ) throws IOException {
        ClientUtils.createTopic(topic, numPartitions, replicationFactor, props);
        try (BufferedReader reader =
                 new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
             Producer<K, V> producer = new KafkaProducer<>(props, keySerializer, valueSerializer)) {
            String line;
            while ((line = reader.readLine()) != null) {
                VertexWithValue<K, V> vertex = vertexParser.parse(line);
                log.trace("read vertex: {}", vertex.id());
                ProducerRecord<K, V> producerRecord =
                    new ProducerRecord<>(topic, vertex.id(), vertex.value());
                producer.send(producerRecord);
            }
            producer.flush();
        }
    }

    public static <K, V> void edgesToTopic(
        InputStream inputStream,
        Parser<K> sourceVertexIdParser,
        Parser<K> targetVertexIdParser,
        Parser<V> valueParser,
        Serializer<V> valueSerializer,
        Properties props,
        String topic,
        int numPartitions,
        short replicationFactor
    ) throws IOException {
        edgesToTopic(inputStream, new Parsers.EdgeParser<>(sourceVertexIdParser, targetVertexIdParser, valueParser),
            valueSerializer, props, topic, numPartitions, replicationFactor);
    }

    public static <K, V> void edgesToTopic(
        InputStream inputStream,
        Parser<EdgeWithValue<K, V>> edgeParser,
        Serializer<V> valueSerializer,
        Properties props,
        String topic,
        int numPartitions,
        short replicationFactor
    ) throws IOException {
        ClientUtils.createTopic(topic, numPartitions, replicationFactor, props);
        try (BufferedReader reader =
                 new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
             Producer<Edge<K>, V> producer = new KafkaProducer<>(props, new KryoSerializer<>(), valueSerializer)) {
            String line;
            while ((line = reader.readLine()) != null) {
                EdgeWithValue<K, V> edge = edgeParser.parse(line);
                log.trace("read edge: ({}, {})", edge.source(), edge.target());
                ProducerRecord<Edge<K>, V> producerRecord =
                    new ProducerRecord<>(topic, new Edge<>(edge.source(), edge.target()), edge.value());
                producer.send(producerRecord);
            }
            producer.flush();
        }
    }

    public static <V> void verticesToFile(
        KTable<Long, V> vertices,
        String fileName) {
        vertices.toStream().print(Printed.<Long, V>toFile(fileName).withKeyValueMapper((k, v) -> String.format("%s %s", k, v)));
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
        AtomicInteger vertexCount = new AtomicInteger(0);
        AtomicInteger edgeCount = new AtomicInteger(0);
        AtomicLong lastWriteMs = new AtomicLong(0);

        Properties vertexProducerConfig = ClientUtils.producerConfig(
            streamsConfig.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG),
            graph.keySerde().serializer().getClass(), graph.vertexValueSerde().serializer().getClass(),
            streamsConfig
        );
        vertexProducerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "pregel-vertex-producer");
        Producer<K, VV> vertexProducer = new KafkaProducer<>(vertexProducerConfig);

        Properties edgeProducerConfig = ClientUtils.producerConfig(
            streamsConfig.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG),
            graph.keySerde().serializer().getClass(), KryoSerializer.class,
            streamsConfig
        );
        edgeProducerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "pregel-edge-producer");
        Producer<K, Map<K, EV>> edgeProducer = new KafkaProducer<>(edgeProducerConfig);

        graph.vertices()
            .toStream()
            .peek((k, v) -> {
                vertexCount.incrementAndGet();
                lastWriteMs.set(System.currentTimeMillis());
            })
            .process(() -> new SendMessages<>(verticesTopic, vertexProducer, lastWrittenOffsets));
        graph.edgesGroupedBySource()
            .toStream()
            .peek((k, v) -> {
                edgeCount.incrementAndGet();
                lastWriteMs.set(System.currentTimeMillis());
            })
            .mapValues(v -> StreamSupport.stream(v.spliterator(), false)
                .collect(Collectors.toMap(EdgeWithValue::target, EdgeWithValue::value)))
            .process(() -> new SendMessages<>(edgesGroupedBySourceTopic, edgeProducer, lastWrittenOffsets));

        Topology topology = builder.build();
        log.debug("Graph description {}", topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
        streams.start();

        CompletableFuture<Map<TopicPartition, Long>> future = new CompletableFuture<>();
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        // TODO make interval configurable
        ScheduledFuture scheduledFuture = executor.scheduleWithFixedDelay(() -> {
            long lastWrite = lastWriteMs.get();
            if (lastWrite > 0 && System.currentTimeMillis() - lastWrite > 10000) {
                try {
                    vertexProducer.close();
                    edgeProducer.close();
                    streams.close();
                } finally {
                    future.complete(lastWrittenOffsets);
                }
                log.info("Finished loading graph: {} vertices, {} edges", vertexCount.get(), edgeCount.get());
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);

        return future.whenCompleteAsync((v, t) -> {
            scheduledFuture.cancel(true);
            executor.shutdown();
        });
    }

    private static final class SendMessages<K, V> implements Processor<K, V> {

        private final String topic;
        private final Producer<K, V> producer;
        private final Map<TopicPartition, Long> lastWrittenOffsets;

        public SendMessages(String topic,
                            Producer<K, V> producer,
                            Map<TopicPartition, Long> lastWrittenOffsets
        ) {
            this.topic = topic;
            this.producer = producer;
            this.lastWrittenOffsets = lastWrittenOffsets;
        }

        @Override
        public void init(ProcessorContext context) {
        }

        @Override
        public void process(final K readOnlyKey, final V value) {
            try {
                ProducerRecord<K, V> producerRecord =
                    new ProducerRecord<>(topic, readOnlyKey, value);
                producer.send(producerRecord, (metadata, error) -> {
                    if (error == null) {
                        try {
                            lastWrittenOffsets.merge(
                                new TopicPartition(metadata.topic(), metadata.partition()),
                                metadata.offset(),
                                Math::max
                            );
                        } catch (Exception e) {
                            throw toRuntimeException(e);
                        }
                    } else {
                        log.error("Failed to send record to {}: {}", topic, error);
                    }
                });
                producer.flush();
            } catch (Exception e) {
                throw toRuntimeException(e);
            }
        }

        @Override
        public void close() {
        }
    }

    private static RuntimeException toRuntimeException(Exception e) {
        return e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
    }
}
