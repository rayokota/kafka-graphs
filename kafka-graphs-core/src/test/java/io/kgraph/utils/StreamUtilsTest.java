/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kgraph.utils;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import io.kgraph.AbstractIntegrationTest;

public class StreamUtilsTest extends AbstractIntegrationTest {
    private static final String LEFT_INPUT_TOPIC = "left-input-topic";
    private static final String RIGHT_INPUT_TOPIC = "right-input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String AGG_OUTPUT_TOPIC = "agg-output-topic";

    private static String[] topics() {
        return new String[]{LEFT_INPUT_TOPIC, RIGHT_INPUT_TOPIC, OUTPUT_TOPIC};
    }

    private static final List<Integer> LEFT_INPUT = Arrays.asList(
        1, 2, 2, 3, 4, 4, 5
    );

    private static final List<Integer> RIGHT_INPUT = Arrays.asList(
        2, 3, 3, 4, 5, 5, 6
    );

    private static Properties PRODUCER_CONFIG;

    @BeforeClass
    public static void setupConfigsAndUtils() {
        PRODUCER_CONFIG = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), IntegerSerializer.class,
            IntegerSerializer.class, new Properties()
        );
    }

    private static <K, V> List<KeyValue<K, V>> consumeData(
        String topic,
        Class keyDeserializer,
        Class valueDeserializer,
        int expectedNumMessages,
        long resultsPollMaxTimeMs) {

        List<KeyValue<K, V>> result = new ArrayList<>();

        Properties consumerConfig = ClientUtils.consumerConfig(CLUSTER.bootstrapServers(), "testgroup",
            keyDeserializer, valueDeserializer, new Properties());
        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig)) {

            consumer.subscribe(Collections.singleton(topic));
            long pollStart = System.currentTimeMillis();
            long pollEnd = pollStart + resultsPollMaxTimeMs;
            while (System.currentTimeMillis() < pollEnd &&
                continueConsuming(result.size(), expectedNumMessages)) {
                for (ConsumerRecord<K, V> record :
                    consumer.poll(Math.max(1, pollEnd - System.currentTimeMillis()))) {
                    if (record.value() != null) {
                        result.add(new KeyValue<>(record.key(), record.value()));
                    }
                }
            }
        }
        return result;
    }

    private static boolean continueConsuming(int messagesConsumed, int maxMessages) {
        return maxMessages < 0 || messagesConsumed < maxMessages;
    }

    @Test
    public void testCollectionToStream() throws Exception {
        Collection<KeyValue<Integer, Integer>> input = new ArrayList<>();
        for (Integer i : LEFT_INPUT) {
            input.add(new KeyValue<>(i, i));
        }
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, Integer> stream = StreamUtils.streamFromCollection(
            builder, PRODUCER_CONFIG, LEFT_INPUT_TOPIC, 50, (short) 1,
            Serdes.Integer(), Serdes.Integer(),
            input);
        stream.to(OUTPUT_TOPIC);

        startStreams(builder, Serdes.Integer(), Serdes.Integer());

        Thread.sleep(1000);

        List<KeyValue<Integer, Integer>> records = consumeData(
            OUTPUT_TOPIC, IntegerDeserializer.class, IntegerDeserializer.class, 26, 10000L);
        for (KeyValue<Integer, Integer> record : records) {
            assertEquals(record.key, record.value);
        }

        streams.close();
    }

    @After
    public void cleanup() throws Exception {
        CLUSTER.deleteTopicsAndWait(120000, topics());
    }
}
