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

package io.kgraph.library.basic;

import java.util.Optional;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kgraph.EdgeWithValue;
import io.kgraph.GraphSerialized;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.ComputeFunction;
import io.kgraph.pregel.PregelGraphAlgorithm;

public class ReverseEdges<K, VV, EV> extends PregelGraphAlgorithm<K, VV, EV, EdgeWithValue<K, EV>> {
    private static final Logger log = LoggerFactory.getLogger(ReverseEdges.class);

    public ReverseEdges(
        String hostAndPort,
        String applicationId,
        String bootstrapServers,
        CuratorFramework curator,
        String verticesTopic,
        String edgesGroupedBySourceTopic,
        GraphSerialized<K, VV, EV> serialized,
        int numPartitions,
        short replicationFactor
    ) {
        super(hostAndPort, applicationId, bootstrapServers, curator, verticesTopic, edgesGroupedBySourceTopic, serialized,
            numPartitions, replicationFactor, Optional.empty()
        );
    }

    public ReverseEdges(
        String hostAndPort,
        String applicationId,
        String bootstrapServers,
        String zookeeperConnect,
        String verticesTopic,
        String edgesGroupedBySourceTopic,
        GraphSerialized<K, VV, EV> serialized,
        String solutionSetTopic,
        String solutionSetStore,
        String workSetTopic,
        int numPartitions,
        short replicationFactor
    ) {
        super(hostAndPort, applicationId, bootstrapServers, zookeeperConnect, verticesTopic, edgesGroupedBySourceTopic, serialized,
            solutionSetTopic, solutionSetStore, workSetTopic, numPartitions, replicationFactor, Optional.empty()
        );
    }

    @Override
    protected ComputeFunction<K, VV, EV, EdgeWithValue<K, EV>> computeFunction() {
        return new ReverseEdgesComputeFunction();
    }

    public final class ReverseEdgesComputeFunction implements ComputeFunction<K, VV, EV, EdgeWithValue<K, EV>> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<K, VV> vertex,
            Iterable<EdgeWithValue<K, EV>> messages,
            Iterable<EdgeWithValue<K, EV>> edges,
            Callback<K, VV, EV, EdgeWithValue<K, EV>> cb
        ) {
            if (superstep == 0) {
                for (EdgeWithValue<K, EV> edge : edges) {
                    cb.sendMessageTo(edge.target(), edge);
                }
            } else if (superstep == 1) {
                for (EdgeWithValue<K, EV> msg : messages) {
                    boolean hasEdge = false;
                    for (EdgeWithValue<K, EV> edge : edges) {
                        if (edge.target().equals(msg.source())) {
                            hasEdge = true;
                            break;
                        }
                    }
                    if (!hasEdge) {
                        cb.addEdge(msg.source(), msg.value());
                    }
                }
            }

            cb.voteToHalt();
        }
    }
}
