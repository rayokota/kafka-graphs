/*
 * Copyright 2014 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.Objects;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.library.basic.VertexCount;
import io.kgraph.pregel.ComputeFunction;
import io.kgraph.pregel.aggregators.LongSumAggregator;

/**
 * This is an implementation of the SybilRank algorithm. In fact, this is an
 * extension of the original SybilRank algorithm published by Cao et al at
 * NSDI'12. This version of the algorithm assumes a weighted graph. The modified
 * algorithm has been developed by Boshmaf et al.
 *
 * @author dl
 */
public class SybilRank implements ComputeFunction<Long, SybilRank.VertexValue, Double, Double> {
    /**
     * Property name for the total trust.
     */
    public static final String TOTAL_TRUST = "sybilrank.total.trust";

    /**
     * Property name for the iteration multiplier.
     */
    public static final String ITERATION_MULTIPLIER = "sybilrank.iteration.multiplier";

    /**
     * Default multiplier for the iterations.
     */
    public static final int ITERATION_MULTIPLIER_DEFAULT = 1;

    /**
     * Name of aggregator used to calculate the total number of trusted nodes.
     */
    public static final String AGGREGATOR_NUM_TRUSTED = "AGG_NUM_TRUSTED";

    /**
     * This method computes the degree of a vertex as the sum of its edge weights.
     *
     * @param edges
     * @return
     */
    public static double computeDegree(Iterable<EdgeWithValue<Long, Double>> edges
    ) {
        double degree = 0.0;
        for (EdgeWithValue<Long, Double> edge : edges) {
            degree += edge.value();
        }
        return degree;
    }

    /**
     * This computation class is used to calculate the aggregate number of
     * trusted nodes. This value is necessary to initialize the rank of the nodes
     * before the power iterations starts.
     *
     * @author dl
     */
    public static class TrustAggregation implements
        ComputeFunction<Long, VertexValue, Double, Double> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<Long, VertexValue> vertex,
            Iterable<Double> messages,
            Iterable<EdgeWithValue<Long, Double>> edges,
            Callback<Long, VertexValue, Double, Double> cb
        ) {
            if (vertex.value().isTrusted()) {
                cb.aggregate(AGGREGATOR_NUM_TRUSTED, 1L);
            }
        }
    }

    /**
     * This class is used only to initialize the rank of the vertices. It assumes
     * that the trust aggregation computations has occurred in the previous step.
     * <p>
     * After the initialization it also distributes the rank of every vertex to
     * it friends, so that the power iterations start.
     *
     * @author dl
     */
    public class Initializer implements ComputeFunction<Long, VertexValue, Double, Double> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<Long, VertexValue> vertex,
            Iterable<Double> messages,
            Iterable<EdgeWithValue<Long, Double>> edges,
            Callback<Long, VertexValue, Double, Double> cb
        ) {
            double totalTrust = totalTrustParameter != null
                ? totalTrustParameter
                : getTotalNumVertices(cb);

            if (vertex.value().isTrusted()) {
                vertex.value().setRank(
                    totalTrust / (Long) cb.getAggregatedValue(AGGREGATOR_NUM_TRUSTED));
            } else {
                vertex.value().setRank(0.0);
            }
            cb.setNewVertexValue(vertex.value());

            double degree = computeDegree(edges);

            // Distribute rank to edges proportionally to the edge weights
            for (EdgeWithValue<Long, Double> edge : edges) {
                double distRank = vertex.value().getRank() * (edge.value() / degree);
                cb.sendMessageTo(edge.target(), distRank);
            }
        }
    }

    /**
     * This class implements the main part of the SybilRank algorithms, that is,
     * the power iterations.
     *
     * @author dl
     */
    public static class SybilRankComputation implements ComputeFunction<Long, VertexValue, Double, Double> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<Long, VertexValue> vertex,
            Iterable<Double> messages,
            Iterable<EdgeWithValue<Long, Double>> edges,
            Callback<Long, VertexValue, Double, Double> cb
        ) {
            // Aggregate rank from friends.
            double newRank = 0.0;
            for (Double message : messages) {
                newRank += message;
            }

            double degree = computeDegree(edges);

            // Distribute rank to edges proportionally to the edge weights
            for (EdgeWithValue<Long, Double> edge : edges) {
                double distRank = newRank * (edge.value() / degree);
                cb.sendMessageTo(edge.target(), distRank);
            }

            // The final value of the rank is normalized by the degree of the vertex.
            vertex.value().setRank(newRank / degree);
            cb.setNewVertexValue(vertex.value());
        }
    }

    private int iterationMultiplier;
    private Double totalTrustParameter;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, ?> configs, InitCallback cb) {
        Map<String, Object> c = (Map<String, Object>) configs;
        iterationMultiplier = (Integer) c.getOrDefault(ITERATION_MULTIPLIER, ITERATION_MULTIPLIER_DEFAULT);
        totalTrustParameter = (Double) c.get(TOTAL_TRUST);

        // Register the aggregator that will be used to count the number of
        // trusted nodes.
        cb.registerAggregator(AGGREGATOR_NUM_TRUSTED, LongSumAggregator.class, true);
        cb.registerAggregator(VertexCount.VERTEX_COUNT_AGGREGATOR, LongSumAggregator.class, true);
    }

    @Override
    public void masterCompute(int superstep, MasterCallback cb) {
        // The number of power iterations we execute is equal to c*log10(N), where
        // N is the number of vertices in the graph and c is the iteration
        // multiplier.
        if (superstep > 0) {
            int maxPowerIterations = (int) Math.ceil(
                iterationMultiplier * Math.log10((double) getTotalNumVertices(cb)));
            // Before the power iterations, we execute 2 initial supersteps, so we
            // count those in when deciding to stop.
            if (superstep >= 2 + maxPowerIterations) {
                cb.haltComputation();
            }
        }
    }

    @Override
    public void compute(
        int superstep,
        VertexWithValue<Long, VertexValue> vertex,
        Iterable<Double> messages,
        Iterable<EdgeWithValue<Long, Double>> edges,
        Callback<Long, VertexValue, Double, Double> cb
    ) {
        if (superstep == 0) {
            new TrustAggregation().compute(superstep, vertex, messages, edges, cb);
            new VertexCount<Long, VertexValue, Double, Double>()
                .compute(superstep, vertex, messages, edges, cb);
        } else if (superstep == 1) {
            new Initializer().compute(superstep, vertex, messages, edges, cb);
        } else {
            new SybilRankComputation().compute(superstep, vertex, messages, edges, cb);
        }
    }

    /**
     * Represents the state of a vertex for this algorithm. This state indicates
     * the current rank of the vertex and whether this vertex is considered
     * trusted or not.
     * <p>
     * Unless explicitly set, a vertex is initialized to be untrusted.
     *
     * @author dl
     */
    public static class VertexValue {
        // Indicates whether this vertex is considered trusted.
        private final boolean isTrusted;
        // This holds the current rank of the vertex.
        private double rank;

        public VertexValue(double rank, boolean isTrusted) {
            this.rank = rank;
            this.isTrusted = isTrusted;
        }

        public void setRank(double rank) {
            this.rank = rank;
        }

        public double getRank() {
            return rank;
        }

        public boolean isTrusted() {
            return isTrusted;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VertexValue that = (VertexValue) o;
            return isTrusted == that.isTrusted &&
                Double.compare(that.rank, rank) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(isTrusted, rank);
        }

        @Override
        public String toString() {
            return String.valueOf(rank);
        }
    }

    protected long getTotalNumVertices(ReadAggregators aggregators) {
        return aggregators.getAggregatedValue(VertexCount.VERTEX_COUNT_AGGREGATOR);
    }
}
