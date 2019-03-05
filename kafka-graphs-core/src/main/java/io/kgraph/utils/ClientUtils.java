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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientUtils {
    private static final Logger log = LoggerFactory.getLogger(ClientUtils.class);

    /**
     * Create a temporary relative directory in the default temporary-file directory with the given
     * prefix.
     *
     * @param prefix The prefix of the temporary directory, if null using "kafka-" as default prefix
     */
    public static File tempDirectory(final String prefix) {
        return tempDirectory(null, prefix);
    }

    /**
     * Create a temporary relative directory in the default temporary-file directory with a
     * prefix of "kafka-"
     *
     * @return the temporary directory just created.
     */
    public static File tempDirectory() {
        return tempDirectory(null);
    }

    /**
     * Create a temporary relative directory in the specified parent directory with the given prefix.
     *
     * @param parent The parent folder path name, if null using the default temporary-file directory
     * @param prefix The prefix of the temporary directory, if null using "kafka-" as default prefix
     */
    public static File tempDirectory(final Path parent, String prefix) {
        final File file;
        prefix = prefix == null ? "kafka-" : prefix;
        try {
            file = parent == null
                ?
                Files.createTempDirectory(prefix).toFile()
                : Files.createTempDirectory(parent, prefix).toFile();
        } catch (final IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }
        file.deleteOnExit();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Utils.delete(file);
            } catch (IOException e) {
                log.error("Error deleting {}", file.getAbsolutePath(), e);
            }
        }));

        return file;
    }

    public static <T> Serde<T> getSerde(Class<Serde<T>> cls, Map<String, ?> configs) {
        try {
            final Serde<T> serde = getConfiguredInstance(cls, configs);
            serde.configure(configs, true);
            return serde;
        } catch (final Exception e) {
            throw new KafkaException(
                String.format("Failed to configure key serde %s", cls), e);
        }
    }

    public static <T> T getConfiguredInstance(Class<T> cls, Map<String, ?> configs) {
        if (cls == null) {
            return null;
        }
        Object o = Utils.newInstance(cls);
        if (o instanceof Configurable) {
            ((Configurable) o).configure(configs);
        }
        return cls.cast(o);
    }

    public static Properties producerConfig(
        final String bootstrapServers,
        final Class keySerializer,
        final Class valueSerializer,
        final Properties additional) {
        final Properties properties = new Properties();
        properties.putAll(additional);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return properties;
    }

    public static Properties consumerConfig(
        final String bootstrapServers,
        final String groupId,
        final Class keyDeserializer,
        final Class valueDeserializer,
        final Properties additional) {

        final Properties consumerConfig = new Properties();
        consumerConfig.putAll(additional);
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        return consumerConfig;
    }

    public static Properties streamsConfig(
        final String applicationId,
        final String clientId,
        final String bootstrapServers,
        final Class keySerde,
        final Class valueSerde) {

        return streamsConfig(applicationId, clientId, bootstrapServers, keySerde, valueSerde, new Properties());
    }

    public static Properties streamsConfig(
        final String applicationId,
        final String clientId,
        final String bootstrapServers,
        final Class keySerde,
        final Class valueSerde,
        final Properties additional) {

        final Properties streamsConfig = new Properties();
        streamsConfig.putAll(additional);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfig.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde.getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde.getName());
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, ClientUtils.tempDirectory().getAbsolutePath());
        return streamsConfig;
    }

    public static void createTopic(String topic, int numPartitions, short replicationFactor, Properties props) {
        NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
        AdminClient adminClient = AdminClient.create(props);
        adminClient.createTopics(Collections.singletonList(newTopic));
        adminClient.close();
    }

    public static String generateRandomString(int len) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            int randomLimitedInt = leftLimit + (int)
                (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        return buffer.toString();
    }
}
