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

package io.kgraph.pregel;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kgraph.GraphAlgorithm;
import io.kgraph.GraphAlgorithmState;
import io.kgraph.GraphSerialized;
import io.kgraph.utils.ClientUtils;

public class PregelGraphAlgorithm<K, VV, EV, Message>
    implements GraphAlgorithm<K, VV, EV, KTable<K, VV>> {

    private static final Logger log = LoggerFactory.getLogger(PregelGraphAlgorithm.class);

    protected final String hostAndPort;
    protected final String applicationId;
    protected final String bootstrapServers;
    protected final String zookeeperConnect;
    protected final CuratorFramework curator;
    protected final String verticesTopic;
    protected final String edgesGroupedBySourceTopic;
    protected final Map<TopicPartition, Long> graphOffsets;
    protected final GraphSerialized<K, VV, EV> serialized;
    protected final String solutionSetTopic;
    protected final String solutionSetStore;
    protected final String workSetTopic;
    protected final int numPartitions;
    protected final short replicationFactor;
    protected final PregelComputation<K, VV, EV, Message> computation;

    protected KafkaStreams streams;

    public PregelGraphAlgorithm(String hostAndPort,
                                String applicationId,
                                String bootstrapServers,
                                CuratorFramework curator,
                                String verticesTopic,
                                String edgesGroupedBySourceTopic,
                                Map<TopicPartition, Long> graphOffsets,
                                GraphSerialized<K, VV, EV> serialized,
                                int numPartitions,
                                short replicationFactor,
                                Map<String, ?> configs,
                                Optional<Message> initialMessage,
                                ComputeFunction<K, VV, EV, Message> cf) {
        this(hostAndPort,
            applicationId,
            bootstrapServers,
            curator,
            verticesTopic,
            edgesGroupedBySourceTopic,
            graphOffsets,
            serialized,
            "solutionSet-" + applicationId,
            "solutionSetStore-" + applicationId,
            "workSet-" + applicationId,
            numPartitions,
            replicationFactor,
            configs,
            initialMessage,
            cf);
    }

    // visible for testing
    public PregelGraphAlgorithm(String hostAndPort,
                                String applicationId,
                                String bootstrapServers,
                                String zookeeperConnect,
                                String verticesTopic,
                                String edgesGroupedBySourceTopic,
                                Map<TopicPartition, Long> graphOffsets,
                                GraphSerialized<K, VV, EV> serialized,
                                String solutionSetTopic,
                                String solutionSetStore,
                                String workSetTopic,
                                int numPartitions,
                                short replicationFactor,
                                Map<String, ?> configs,
                                Optional<Message> initialMessage,
                                ComputeFunction<K, VV, EV, Message> cf) {
        this(hostAndPort,
            applicationId,
            bootstrapServers,
            ZKUtils.createCurator(zookeeperConnect),
            verticesTopic,
            edgesGroupedBySourceTopic,
            graphOffsets,
            serialized,
            solutionSetTopic,
            solutionSetStore,
            workSetTopic,
            numPartitions,
            replicationFactor,
            configs,
            initialMessage,
            cf);
    }

    public PregelGraphAlgorithm(String hostAndPort,
                                String applicationId,
                                String bootstrapServers,
                                CuratorFramework curator,
                                String verticesTopic,
                                String edgesGroupedBySourceTopic,
                                Map<TopicPartition, Long> graphOffsets,
                                GraphSerialized<K, VV, EV> serialized,
                                String solutionSetTopic,
                                String solutionSetStore,
                                String workSetTopic,
                                int numPartitions,
                                short replicationFactor,
                                Map<String, ?> configs,
                                Optional<Message> initialMessage,
                                ComputeFunction<K, VV, EV, Message> cf) {
        this.hostAndPort = hostAndPort;
        this.applicationId = applicationId;
        this.bootstrapServers = bootstrapServers;
        this.zookeeperConnect = null;
        this.curator = curator;
        this.verticesTopic = verticesTopic;
        this.edgesGroupedBySourceTopic = edgesGroupedBySourceTopic;
        this.graphOffsets = graphOffsets;
        this.serialized = serialized;
        this.solutionSetTopic = solutionSetTopic;
        this.solutionSetStore = solutionSetStore;
        this.workSetTopic = workSetTopic;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;

        this.computation = new PregelComputation<>(hostAndPort, applicationId,
            bootstrapServers, curator, verticesTopic, edgesGroupedBySourceTopic, graphOffsets,
            serialized, solutionSetTopic, solutionSetStore, workSetTopic, numPartitions,
            configs, initialMessage, cf);
    }

    public GraphSerialized<K, VV, EV> serialized() {
        return serialized;
    }

    @Override
    public GraphAlgorithmState<Void> configure(StreamsBuilder builder, Properties streamsConfig) {
        ClientUtils.createTopic(solutionSetTopic, numPartitions, replicationFactor, streamsConfig);
        ClientUtils.createTopic(workSetTopic, numPartitions, replicationFactor, streamsConfig);

        computation.prepare(builder, streamsConfig);

        Topology topology = builder.build();
        log.info("Topology description {}", topology.describe());
        streams = new KafkaStreams(topology, streamsConfig, new PregelClientSupplier());
        streams.start();

        return new GraphAlgorithmState<>(streams, GraphAlgorithmState.State.CREATED, 0,
            0L, Collections.emptyMap(), null);
    }

    @Override
    public GraphAlgorithmState<KTable<K, VV>> run(int maxIterations) {
        CompletableFuture<KTable<K, VV>> futureResult = new CompletableFuture<>();

        PregelState state = computation.run(maxIterations, futureResult);

        return new GraphAlgorithmState<>(streams, state.state(), state.superstep(),
            state.runningTime(), computation.previousAggregates(state.superstep()), futureResult);
    }

    @Override
    public GraphAlgorithmState<KTable<K, VV>> state() {
        PregelState state = computation.state();
        CompletableFuture<KTable<K, VV>> futureResult = computation.futureResult();

        return new GraphAlgorithmState<>(streams, state.state(), state.superstep(),
            state.runningTime(), computation.previousAggregates(state.superstep()), futureResult);
    }

    @Override
    public Map<String, ?> configs() {
        return computation.configs();
    }

    @Override
    public Iterable<KeyValue<K, VV>> result() {
        return () -> streams.store(StoreQueryParameters.fromNameAndType(
            solutionSetStore, QueryableStoreTypes.<K, VV>keyValueStore())).all();
    }

    @Override
    public void close() {
        streams.close();
        // Clean up ZK after all ZK clients are closed
        computation.close();
    }
}
