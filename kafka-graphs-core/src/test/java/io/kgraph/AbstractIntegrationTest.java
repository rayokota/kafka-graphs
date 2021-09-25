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
package io.kgraph;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import io.kgraph.utils.ClientUtils;

public abstract class AbstractIntegrationTest {
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1, new Properties() {{
        setProperty("message.max.bytes", String.valueOf(100 * 1024 * 1024));
    }});

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    protected KafkaStreams streams;
    protected Properties streamsConfiguration;

    protected <K, V> void startStreams(StreamsBuilder builder, Serde<K> keySerde, Serde<V> valueSerde) {
        String id = UUID.randomUUID().toString();
        streamsConfiguration = ClientUtils.streamsConfig("test-" + id, "test-client-" + id, CLUSTER.bootstrapServers(),
            keySerde.getClass(), valueSerde.getClass());
        streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();
        while (streams.state() != KafkaStreams.State.RUNNING) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    @After
    public void cleanup() throws Exception {
        if (streams != null) {
            streams.close();
        }
        if (streamsConfiguration != null) {
            IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
        }
    }
}
