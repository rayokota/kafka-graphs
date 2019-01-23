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
import io.vavr.Tuple2;

public class PageRank<K> extends PregelGraphAlgorithm<K, Tuple2<Double, Double>, Double, Double> {
    private static final Logger log = LoggerFactory.getLogger(PageRank.class);

    private final double tolerance;
    private final double resetProbability;
    private final K srcVertexId;

    public PageRank(String hostAndPort,
                    String applicationId,
                    String bootstrapServers,
                    CuratorFramework curator,
                    String verticesTopic,
                    String edgesGroupedBySourceTopic,
                    GraphSerialized<K, Tuple2<Double, Double>, Double> serialized,
                    int numPartitions,
                    short replicationFactor,
                    double tolerance,
                    double resetProbability,
                    Optional<K> srcVertexId) {
        super(hostAndPort, applicationId, bootstrapServers, curator, verticesTopic, edgesGroupedBySourceTopic, serialized,
            numPartitions, replicationFactor, Optional.empty());
        this.tolerance = tolerance;
        this.resetProbability = resetProbability;
        this.srcVertexId = srcVertexId.orElse(null);
    }

    public PageRank(String hostAndPort,
                    String applicationId,
                    String bootstrapServers,
                    String zookeeperConnect,
                    String verticesTopic,
                    String edgesGroupedBySourceTopic,
                    GraphSerialized<K, Tuple2<Double, Double>, Double> serialized,
                    String solutionSetTopic,
                    String solutionSetStore,
                    String workSetTopic,
                    int numPartitions,
                    short replicationFactor,
                    double tolerance,
                    double resetProbability,
                    Optional<K> srcVertexId) {
        super(hostAndPort, applicationId, bootstrapServers, zookeeperConnect, verticesTopic, edgesGroupedBySourceTopic, serialized,
            solutionSetTopic, solutionSetStore, workSetTopic, numPartitions, replicationFactor,
            Optional.of(srcVertexId.isPresent() ? 0.0 : resetProbability / (1.0 - resetProbability)));
        this.tolerance = tolerance;
        this.resetProbability = resetProbability;
        this.srcVertexId = srcVertexId.orElse(null);
    }

    @Override
    protected ComputeFunction<K, Tuple2<Double, Double>, Double, Double> computeFunction() {
        return new PageRankComputeFunction();
    }

    public final class PageRankComputeFunction implements ComputeFunction<K, Tuple2<Double, Double>,
        Double, Double> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<K, Tuple2<Double, Double>> vertex,
            Iterable<Double> messages,
            Iterable<EdgeWithValue<K, Double>> edges,
            Callback<K, Tuple2<Double, Double>, Double> cb) {

            double oldPageRank = vertex.value()._1;
            double oldDelta = vertex.value()._2;

            double messageSum = 0.0;
            for (Double message : messages) {
                messageSum += message;
            }

            boolean isPersonalized = srcVertexId != null;
            double newPageRank = isPersonalized && oldDelta == Double.NEGATIVE_INFINITY
                ? 1.0 : oldPageRank + (1.0 - resetProbability) * messageSum;
            double newDelta = newPageRank - oldPageRank;

            log.debug("step {} vertex {} sum {}", superstep, vertex.id(), messageSum);
            log.debug("old ({},{})", oldPageRank, oldDelta);
            log.debug("new ({},{})", newPageRank, newDelta);
            log.debug("msgs {}", messages);

            cb.setNewVertexValue(new Tuple2<>(newPageRank, newDelta));

            for (EdgeWithValue<K, Double> edge : edges) {
                if (newDelta > tolerance) {
                    log.debug("sending to target {} edge {} msg {}", edge.target(), edge.value(), newDelta * edge.value());
                    cb.sendMessageTo(edge.target(), newDelta * edge.value());
                }
            }

            cb.voteToHalt();
        }
    }
}
