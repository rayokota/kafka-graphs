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
package io.kgraph.library.cf;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.jblas.FloatMatrix;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.library.basic.EdgeCount;
import io.kgraph.pregel.ComputeFunction;
import io.kgraph.pregel.aggregators.DoubleSumAggregator;
import io.kgraph.pregel.aggregators.LongSumAggregator;

/**
 * Stochastic Gradient Descent (SGD) implementation.
 */
public class Sgd implements ComputeFunction<CfLongId, FloatMatrix, Float, FloatMatrixMessage> {

    /**
     * Keyword for RMSE aggregator tolerance.
     */
    public static final String RMSE_TARGET = "rmse";
    /**
     * Default value for parameter enabling the RMSE aggregator.
     */
    public static final float RMSE_TARGET_DEFAULT = -1f;
    /**
     * Keyword for parameter setting the convergence tolerance
     */
    public static final String TOLERANCE = "tolerance";
    /**
     * Default value for TOLERANCE.
     */
    public static final float TOLERANCE_DEFAULT = -1f;
    /**
     * Keyword for parameter setting the number of iterations.
     */
    public static final String ITERATIONS = "iterations";
    /**
     * Default value for ITERATIONS.
     */
    public static final int ITERATIONS_DEFAULT = 10;
    /**
     * Keyword for parameter setting the regularization parameter LAMBDA.
     */
    public static final String LAMBDA = "lambda";
    /**
     * Default value for LABDA.
     */
    public static final float LAMBDA_DEFAULT = 0.01f;
    /**
     * Keyword for parameter setting the learning rate GAMMA.
     */
    public static final String GAMMA = "gamma";
    /**
     * Default value for GAMMA.
     */
    public static final float GAMMA_DEFAULT = 0.005f;
    /**
     * Keyword for parameter setting the Latent Vector Size.
     */
    public static final String VECTOR_SIZE = "dim";
    /**
     * Default value for GAMMA.
     */
    public static final int VECTOR_SIZE_DEFAULT = 50;
    /**
     * Max rating.
     */
    public static final String MAX_RATING = "max.rating";
    /**
     * Default maximum rating
     */
    public static final float MAX_RATING_DEFAULT = 5.0f;
    /**
     * Min rating.
     */
    public static final String MIN_RATING = "min.rating";
    /**
     * Default minimum rating
     */
    public static final float MIN_RATING_DEFAULT = 0.0f;

    /**
     * Aggregator used to compute the RMSE
     */
    public static final String RMSE_AGGREGATOR = "sgd.rmse.aggregator";

    private float tolerance;
    private float lambda;
    private float gamma;
    protected float minRating;
    protected float maxRating;
    private FloatMatrix oldValue;

    private Map<String, Object> configs;

    @Override
    public void preSuperstep(int superstep, Aggregators aggregators) {
        lambda = (Float) configs.getOrDefault(LAMBDA, LAMBDA_DEFAULT);
        gamma = (Float) configs.getOrDefault(GAMMA, GAMMA_DEFAULT);
        tolerance = (Float) configs.getOrDefault(TOLERANCE, TOLERANCE_DEFAULT);
        minRating = (Float) configs.getOrDefault(MIN_RATING, MIN_RATING_DEFAULT);
        maxRating = (Float) configs.getOrDefault(MAX_RATING, MAX_RATING_DEFAULT);
    }

    /**
     * Main SGD compute method.
     *
     * @param messages Messages received
     */
    public void superstepCompute(
        int superstep,
        VertexWithValue<CfLongId, FloatMatrix> vertex,
        Iterable<FloatMatrixMessage> messages,
        Iterable<EdgeWithValue<CfLongId, Float>> edges,
        Callback<CfLongId, FloatMatrix, Float, FloatMatrixMessage> cb
    ) {
        double rmsePartialSum = 0d;
        float l2norm = 0f;

        if (tolerance > 0) {
            // Create new object because we're going to operate on the old one.
            oldValue = new FloatMatrix(vertex.value().getRows(),
                vertex.value().getColumns(), vertex.value().data
            );
        }

        Map<CfLongId, Float> edgeValues = new HashMap<>();
        for (EdgeWithValue<CfLongId, Float> edge : edges) {
            edgeValues.put(edge.target(), edge.value());
        }
        for (FloatMatrixMessage msg : messages) {
            // Get rating for the item that this message came from
            float rating = edgeValues.get(msg.getSenderId());

            // Update the factors
            updateValue(vertex.value(), msg.getFactors(), rating,
                minRating, maxRating, lambda, gamma
            );
        }

        // Calculate new error for RMSE calculation
        for (FloatMatrixMessage msg : messages) {
            float predicted = vertex.value().dot(msg.getFactors());
            float rating = edgeValues.get(msg.getSenderId());
            predicted = Math.min(predicted, maxRating);
            predicted = Math.max(predicted, minRating);
            float err = predicted - rating;
            rmsePartialSum += (err * err);
        }

        cb.aggregate(RMSE_AGGREGATOR, rmsePartialSum);

        // Calculate difference with previous value
        if (tolerance > 0) {
            l2norm = vertex.value().distance2(oldValue);
        }

        // Broadcast the new vector
        if (tolerance < 0 || (tolerance > 0 && l2norm > tolerance)) {
            for (EdgeWithValue<CfLongId, Float> edge : edges) {
                cb.sendMessageTo(
                    edge.target(),
                    new FloatMatrixMessage(vertex.id(), vertex.value(), 0.0f)
                );
            }
        }

        cb.setNewVertexValue(vertex.value());
        cb.voteToHalt();
    }

    /**
     * Applies the SGD update logic in the provided vector. It does the update
     * in-place.
     * <p>
     * The update is performed according to the following formula:
     * <p>
     * v = v - gamma*(lambda*v + error*u)
     *
     * @param value     The vector to update
     * @param update    The vector used to update
     * @param rating
     * @param minRating
     * @param maxRating
     * @param lambda
     * @param gamma
     */
    protected final void updateValue(
        FloatMatrix value, FloatMatrix update, final float rating, final float minRating,
        final float maxRating, final float lambda, final float gamma
    ) {
        float predicted = value.dot(update);

        // Correct the predicted rating
        predicted = Math.min(predicted, maxRating);
        predicted = Math.max(predicted, minRating);

        float err = predicted - rating;

        FloatMatrix part1 = value.mul(lambda);
        FloatMatrix part2 = update.mul(err);
        FloatMatrix part3 = (part1.add(part2)).mul(-gamma);
        value.addi(part3);
    }

    /**
     * This computation class is used to initialize the factors of the user nodes
     * in the very first superstep, and send the first updates to the item nodes.
     *
     * @author dl
     */
    public class InitUsersComputation implements
        ComputeFunction<CfLongId, FloatMatrix, Float, FloatMatrixMessage> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<CfLongId, FloatMatrix> vertex,
            Iterable<FloatMatrixMessage> messages,
            Iterable<EdgeWithValue<CfLongId, Float>> edges,
            Callback<CfLongId, FloatMatrix, Float, FloatMatrixMessage> cb
        ) {
            FloatMatrix vector = new FloatMatrix((Integer) configs.getOrDefault(VECTOR_SIZE, VECTOR_SIZE_DEFAULT));
            Random randGen = new Random(0);
            for (int i = 0; i < vector.length; i++) {
                vector.put(i, 0.01f * randGen.nextFloat());
            }
            cb.setNewVertexValue(vector);

            for (EdgeWithValue<CfLongId, Float> edge : edges) {
                FloatMatrixMessage msg = new FloatMatrixMessage(vertex.id(), vector, edge.value());
                cb.sendMessageTo(edge.target(), msg);
            }

            cb.voteToHalt();
        }
    }

    /**
     * This computation class is used to initialize the factors of the item nodes
     * in the second superstep. Every item also creates the edges that point to
     * the users that have rated the item.
     *
     * @author dl
     */
    public class InitItemsComputation implements
        ComputeFunction<CfLongId, FloatMatrix, Float, FloatMatrixMessage> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<CfLongId, FloatMatrix> vertex,
            Iterable<FloatMatrixMessage> messages,
            Iterable<EdgeWithValue<CfLongId, Float>> edges,
            Callback<CfLongId, FloatMatrix, Float, FloatMatrixMessage> cb
        ) {
            FloatMatrix vector = new FloatMatrix((Integer) configs.getOrDefault(VECTOR_SIZE, VECTOR_SIZE_DEFAULT));
            Random randGen = new Random(0);
            for (int i = 0; i < vector.length; i++) {
                vector.put(i, 0.01f * randGen.nextFloat());
            }
            cb.setNewVertexValue(vector);

            for (FloatMatrixMessage msg : messages) {
                cb.addEdge(msg.getSenderId(), msg.getScore());
            }

            // The score does not matter at this point.
            for (EdgeWithValue<CfLongId, Float> edge : edges) {
                cb.sendMessageTo(edge.target(), new FloatMatrixMessage(vertex.id(), vector, 0.0f));
            }

            cb.voteToHalt();
        }
    }

    private int maxIterations;
    private float rmseTarget;

    @SuppressWarnings("unchecked")
    @Override
    public final void init(Map<String, ?> configs, InitCallback cb) {
        this.configs = (Map<String, Object>) configs;
        maxIterations = (Integer) this.configs.getOrDefault(ITERATIONS, ITERATIONS_DEFAULT);
        rmseTarget = (Float) this.configs.getOrDefault(RMSE_TARGET, RMSE_TARGET_DEFAULT);

        cb.registerAggregator(RMSE_AGGREGATOR, DoubleSumAggregator.class);
        cb.registerAggregator(EdgeCount.EDGE_COUNT_AGGREGATOR, LongSumAggregator.class, true);
    }

    @Override
    public final void masterCompute(int superstep, MasterCallback cb) {
        long numRatings = getTotalNumEdges(cb);
        double rmse = Math.sqrt(((Double) cb.getAggregatedValue(RMSE_AGGREGATOR)) / numRatings);

        if (rmseTarget > 0f && rmse < rmseTarget) {
            cb.haltComputation();
        } else if (superstep > maxIterations) {
            cb.haltComputation();
        }
    }

    @Override
    public void compute(
        int superstep,
        VertexWithValue<CfLongId, FloatMatrix> vertex,
        Iterable<FloatMatrixMessage> messages,
        Iterable<EdgeWithValue<CfLongId, Float>> edges,
        Callback<CfLongId, FloatMatrix, Float, FloatMatrixMessage> cb
    ) {
        if (superstep == 0) {
            new EdgeCount<CfLongId, FloatMatrix, Float, FloatMatrixMessage>()
                .compute(superstep, vertex, messages, edges, cb);
        } else if (superstep == 1) {
            new InitUsersComputation().compute(superstep, vertex, messages, edges, cb);
        } else if (superstep == 2) {
            new InitItemsComputation().compute(superstep, vertex, messages, edges, cb);
        } else {
            superstepCompute(superstep, vertex, messages, edges, cb);
        }
    }

    // Returns the total number of edges before adding reverse edges
    protected long getTotalNumEdges(ReadAggregators aggregators) {
        return aggregators.getAggregatedValue(EdgeCount.EDGE_COUNT_AGGREGATOR);
    }
}
