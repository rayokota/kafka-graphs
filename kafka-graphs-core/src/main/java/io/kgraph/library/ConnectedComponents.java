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

import java.util.Optional;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kgraph.EdgeWithValue;
import io.kgraph.GraphSerialized;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.ComputeFunction;
import io.kgraph.pregel.PregelGraphAlgorithm;

public class ConnectedComponents<EV> extends PregelGraphAlgorithm<Long, Long, EV, Long> {
    private static final Logger log = LoggerFactory.getLogger(ConnectedComponents.class);

    public ConnectedComponents(String hostAndPort,
                               String applicationId,
                               String bootstrapServers,
                               CuratorFramework curator,
                               String verticesTopic,
                               String edgesGroupedBySourceTopic,
                               GraphSerialized<Long, Long, EV> serialized,
                               int numPartitions,
                               short replicationFactor) {
        super(hostAndPort, applicationId, bootstrapServers, curator, verticesTopic, edgesGroupedBySourceTopic, serialized,
            numPartitions, replicationFactor, Optional.empty());
    }

    public ConnectedComponents(String hostAndPort,
                               String applicationId,
                               String bootstrapServers,
                               CuratorFramework curator,
                               String verticesTopic,
                               String edgesGroupedBySourceTopic,
                               GraphSerialized<Long, Long, EV> serialized,
                               String solutionSetTopic,
                               String solutionSetStore,
                               String workSetTopic,
                               int numPartitions,
                               short replicationFactor) {
        super(hostAndPort, applicationId, bootstrapServers, curator, verticesTopic, edgesGroupedBySourceTopic, serialized,
            solutionSetTopic, solutionSetStore, workSetTopic, numPartitions, replicationFactor, Optional.empty());
    }

    public ConnectedComponents(String hostAndPort,
                               String applicationId,
                               String bootstrapServers,
                               String zookeeperConnect,
                               String verticesTopic,
                               String edgesGroupedBySourceTopic,
                               GraphSerialized<Long, Long, EV> serialized,
                               String solutionSetTopic,
                               String solutionSetStore,
                               String workSetTopic,
                               int numPartitions,
                               short replicationFactor) {
        super(hostAndPort, applicationId, bootstrapServers, zookeeperConnect, verticesTopic, edgesGroupedBySourceTopic, serialized,
            solutionSetTopic, solutionSetStore, workSetTopic, numPartitions, replicationFactor, Optional.empty());
    }

    @Override
    protected ComputeFunction<Long, Long, EV, Long> computeFunction() {
        return new CCComputeFunction();
    }

    public final class CCComputeFunction implements ComputeFunction<Long, Long, EV, Long> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<Long, Long> vertex,
            Iterable<Long> messages,
            Iterable<EdgeWithValue<Long, EV>> edges,
            Callback<Long, Long, Long> cb) {

            Long currentValue = vertex.value();

            for (Long message : messages) {
                currentValue = Math.min(currentValue, message);
            }

            if (currentValue < vertex.value()) {
                log.debug(">>> Vertex {} has new value {}", vertex.id(), currentValue);
                cb.setNewVertexValue(currentValue);
            }

            for (EdgeWithValue<Long, EV> e : edges) {
                if (currentValue < e.target()) {
                    log.debug(">>> Vertex {} sent to {} = {}", vertex.id(), e.target(), currentValue);
                    cb.sendMessageTo(e.target(), currentValue);
                } else if (currentValue > e.target()) {
                    log.debug(">>> Vertex {} sent to {} = {}", vertex.id(), currentValue, e.target());
                    cb.sendMessageTo(currentValue, e.target());
                }
            }
        }
    }
}
