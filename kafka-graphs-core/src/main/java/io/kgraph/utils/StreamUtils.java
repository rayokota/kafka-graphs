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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamUtils {
    private static final Logger log = LoggerFactory.getLogger(StreamUtils.class);

    public static <K, V> KStream<K, V> streamFromCollection(
        StreamsBuilder builder,
        Properties props,
        Serde<K> keySerde,
        Serde<V> valueSerde,
        Collection<KeyValue<K, V>> values) {
        return streamFromCollection(builder, props, "temp-" + UUID.randomUUID(), 50, (short) 1, keySerde, valueSerde,
            values);
    }

    public static <K, V> KStream<K, V> streamFromCollection(
        StreamsBuilder builder,
        Properties props,
        String topic,
        int numPartitions,
        short replicationFactor,
        Serde<K> keySerde,
        Serde<V> valueSerde,
        Collection<KeyValue<K, V>> values) {

        ClientUtils.createTopic(topic, numPartitions, replicationFactor, props);
        try (Producer<K, V> producer = new KafkaProducer<>(props, keySerde.serializer(), valueSerde.serializer())) {
            for (KeyValue<K, V> value : values) {
                ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, value.key, value.value);
                producer.send(producerRecord);
            }
            producer.flush();
        }
        return builder.stream(topic, Consumed.with(keySerde, valueSerde));
    }

    public static <K, V> KTable<K, V> tableFromCollection(
        StreamsBuilder builder,
        Properties props,
        Serde<K> keySerde,
        Serde<V> valueSerde,
        Collection<KeyValue<K, V>> values) {

        return tableFromCollection(builder, props, "temp-" + UUID.randomUUID(), 50, (short) 1, keySerde, valueSerde,
            values);
    }

    public static <K, V> KTable<K, V> tableFromCollection(
        StreamsBuilder builder,
        Properties props,
        String topic,
        int numPartitions,
        short replicationFactor,
        Serde<K> keySerde,
        Serde<V> valueSerde,
        Collection<KeyValue<K, V>> values) {

        ClientUtils.createTopic(topic, numPartitions, replicationFactor, props);
        try (Producer<K, V> producer = new KafkaProducer<>(props, keySerde.serializer(), valueSerde.serializer())) {
            for (KeyValue<K, V> value : values) {
                ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, value.key, value.value);
                producer.send(producerRecord);
            }
            producer.flush();
        }
        return builder.table(topic, Consumed.with(keySerde, valueSerde), Materialized.with(keySerde, valueSerde));
    }

    public static <K, V> List<KeyValue<K, V>> listFromTable(KafkaStreams streams, KTable<K, V> table) {
        return listFromStore(streams, table.queryableStoreName());
    }

    public static <K, V> List<KeyValue<K, V>> listFromStore(KafkaStreams streams, String storeName) {
        final ReadOnlyKeyValueStore<K, V> store = streams.store(
            storeName, QueryableStoreTypes.keyValueStore());

        try (final KeyValueIterator<K, V> all = store.all()) {
            List<KeyValue<K, V>> result = new ArrayList<>();
            while (all.hasNext()) {
                result.add(all.next());
            }
            return result;
        }
    }

    public static <K, V> Map<K, V> mapFromTable(KafkaStreams streams, KTable<K, V> table) {
        return mapFromStore(streams, table.queryableStoreName());
    }

    public static <K, V> Map<K, V> mapFromStore(KafkaStreams streams, String storeName) {
        final ReadOnlyKeyValueStore<K, V> store = streams.store(
            storeName, QueryableStoreTypes.keyValueStore());

        try (final KeyValueIterator<K, V> all = store.all()) {
            Map<K, V> result = new TreeMap<>();
            while (all.hasNext()) {
                KeyValue<K, V> next = all.next();
                result.put(next.key, next.value);
            }
            return result;
        }
    }
}
