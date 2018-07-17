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

package io.kgraph.library;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kgraph.EdgeWithValue;
import io.kgraph.GraphSerialized;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.ComputeFunction;
import io.kgraph.pregel.PregelGraphAlgorithm;

public class MultipleSourceShortestPaths extends PregelGraphAlgorithm<Long, Map<Long, Double>, Double, Map<Long, Double>> {
    private static final Logger log = LoggerFactory.getLogger(MultipleSourceShortestPaths.class);

    private final Set<Long> landmarkVertexIds;

    public MultipleSourceShortestPaths(String hostAndPort,
                                       String applicationId,
                                       String bootstrapServers,
                                       CuratorFramework curator,
                                       String verticesTopic,
                                       String edgesGroupedBySourceTopic,
                                       GraphSerialized<Long, Map<Long, Double>, Double> serialized,
                                       int numPartitions,
                                       short replicationFactor,
                                       Set<Long> landmarkVertexIds) {
        super(hostAndPort, applicationId, bootstrapServers, curator, verticesTopic, edgesGroupedBySourceTopic, serialized,
            numPartitions, replicationFactor, Optional.empty());
        this.landmarkVertexIds = landmarkVertexIds;
    }

    public MultipleSourceShortestPaths(String hostAndPort,
                                       String applicationId,
                                       String bootstrapServers,
                                       String zookeeperConnect,
                                       String verticesTopic,
                                       String edgesGroupedBySourceTopic,
                                       GraphSerialized<Long, Map<Long, Double>, Double> serialized,
                                       String solutionSetTopic,
                                       String solutionSetStore,
                                       String workSetTopic,
                                       int numPartitions,
                                       short replicationFactor,
                                       Set<Long> landmarkVertexIds) {
        super(hostAndPort, applicationId, bootstrapServers, zookeeperConnect, verticesTopic, edgesGroupedBySourceTopic, serialized,
            solutionSetTopic, solutionSetStore, workSetTopic, numPartitions, replicationFactor, Optional.empty());
        this.landmarkVertexIds = landmarkVertexIds;
    }

    @Override
    protected ComputeFunction<Long, Map<Long, Double>, Double, Map<Long, Double>> computeFunction() {
        return new MSSPComputeFunction();
    }

    public final class MSSPComputeFunction implements ComputeFunction<Long, Map<Long, Double>, Double,
        Map<Long, Double>> {

        public void compute(
            int superstep,
            VertexWithValue<Long, Map<Long, Double>> vertex,
            Map<Long, Map<Long, Double>> messages,
            Iterable<EdgeWithValue<Long, Double>> edges,
            Callback<Long, Map<Long, Double>, Map<Long, Double>> cb) {

            Map<Long, Double> minDistance = landmarkVertexIds.stream().collect(Collectors.toMap(id -> id,
                id -> vertex.id().equals(id) ? 0d : Double.POSITIVE_INFINITY));

            for (Map<Long, Double> message : messages.values()) {
                message.forEach((k, v) -> minDistance.merge(k, v, Math::min));
            }
            vertex.value().forEach((k, v) -> minDistance.merge(k, v, Math::min));

            log.debug(">>> Vertex {} got minDist = {} vertex value = {}", vertex.id(), minDistance, vertex.value());

            if (!minDistance.equals(vertex.value())) {
                cb.setNewVertexValue(minDistance);
                for (EdgeWithValue<Long, Double> edge : edges) {
                    Map<Long, Double> distance = minDistance.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() + edge.value()));
                    log.debug(">>> Vertex {} sent to {} = {}", vertex.id(), edge.target(), distance);
                    cb.sendMessageTo(edge.target(), distance);
                }
            }
        }
    }
}
