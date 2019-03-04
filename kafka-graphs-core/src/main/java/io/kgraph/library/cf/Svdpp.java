/**
 * Copyright 2014 Grafos.ml
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kgraph.library.cf;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import org.jblas.FloatMatrix;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.ComputeFunction;
import io.kgraph.pregel.aggregators.DoubleSumAggregator;
import io.kgraph.pregel.aggregators.LongSumAggregator;

public class Svdpp implements ComputeFunction<CfLongId,
    Svdpp.SvdppValue, Float, FloatMatrixMessage> {

    /**
     * Name of aggregator that aggregates all ratings.
     */
    public static final String OVERALL_RATING_AGGREGATOR =
        "svd.overall.rating.aggregator";
    /**
     * RMSE target
     */
    public static final String RMSE_TARGET = "rmse";
    /**
     * Default value for parameter enabling the RMSE aggregator.
     */
    public static final float RMSE_TARGET_DEFAULT = -1f;
    /**
     * Maximum number of iterations.
     */
    public static final String ITERATIONS = "iterations";
    /**
     * Default value for ITERATIONS.
     */
    public static final int ITERATIONS_DEFAULT = 10;
    /**
     * Factor regularization parameter.
     */
    public static final String FACTOR_LAMBDA = "lambda.factor";
    /**
     * Default value for factor regularization parameter
     */
    public static final float FACTOR_LAMBDA_DEFAULT = 0.01f;
    /**
     * Factor learning rate parameter
     */
    public static final String FACTOR_GAMMA = "gamma.factor";
    /**
     * Default value for factor learning rate
     */
    public static final float FACTOR_GAMMA_DEFAULT = 0.005f;
    /**
     * Bias regularization parameter.
     */
    public static final String BIAS_LAMBDA = "lambda.bias";
    /**
     * Default value for bias regularization parameter
     */
    public static final float BIAS_LAMBDA_DEFAULT = 0.01f;
    /**
     * Bias learning rate parameter
     */
    public static final String BIAS_GAMMA = "gamma.bias";
    /**
     * Default value for bias learning rate
     */
    public static final float BIAS_GAMMA_DEFAULT = 0.005f;
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
     * Latent vector size.
     */
    public static final String VECTOR_SIZE = "dim";
    /**
     * Default latent vector size
     */
    public static final int VECTOR_SIZE_DEFAULT = 50;

    /**
     * Aggregator for the computation of RMSE
     */
    public static final String RMSE_AGGREGATOR = "svd.rmse.aggregator";

    /**
     * Aggregator for the number of edges
     */
    public static final String EDGE_COUNT_AGGREGATOR = "edge.count.aggregator";

    private Map<String, Object> configs;

    /**
     * Computes the predicted rating r between a user and an item based on the
     * formula:
     * r = b + q^T * (p + (1/sqrt(N) * sum(y_i)))
     * <p>
     * where
     * b: the baseline estimate of the user for the item
     * q: the item vector
     * p: the user vector
     * N: number of ratings of the user
     * y_i: the weight vector
     *
     * @param meanRating
     * @param userBaseline
     * @param itemBaseline
     * @param user
     * @param item
     * @param numRatings
     * @param sumWeights
     * @param maxRating
     * @param minRating
     */
    protected static float computePredictedRating(
        final float meanRating,
        final float userBaseline, final float itemBaseline, FloatMatrix user,
        FloatMatrix item, final int numRatings, FloatMatrix sumWeights,
        final float minRating, final float maxRating
    ) {

        float predicted = meanRating + userBaseline + itemBaseline +
            item.dot(user.add(sumWeights.mul(1.0f / (float) (Math.sqrt(numRatings)))));

        // Correct the predicted rating to be between the min and max ratings
        predicted = Math.min(predicted, maxRating);
        predicted = Math.max(predicted, minRating);

        return predicted;
    }

    /**
     * Computes the updated baseline based on the formula:
     * <p>
     * b := b + gamma * (error - lambda * b)
     *
     * @param baseline
     * @param predictedRating
     * @param observedRating
     * @param gamma
     * @param lambda
     */
    protected static float computeUpdatedBaseLine(
        final float baseline,
        final float predictedRating, final float observedRating,
        final float gamma, final float lambda
    ) {

        return baseline +
            gamma * ((predictedRating - observedRating) - lambda * baseline);
    }

    /**
     * Increments a scalar value according to the formula:
     * <p>
     * v:= v + step - gamma*lambda*v;
     *
     * @param baseline
     * @param step
     * @param gamma
     * @param lambda
     * @return
     */
    protected static float incrementValue(
        final float baseline,
        final float step, final float gamma, final float lambda
    ) {
        return baseline + step - gamma * lambda * baseline;
    }

    /**
     * Increments a vector according to the formula
     * <p>
     * v:= v + step - gamma*lambda*v
     *
     * @param value
     * @param step
     * @param gamma
     * @param lambda
     */
    protected static void incrementValue(
        FloatMatrix value, FloatMatrix step,
        final float gamma, final float lambda
    ) {
        value.addi(value.mul(-gamma * lambda).addi(step));
    }

    /**
     * A value in the Svdpp algorithm consists of (i) the baseline estimate, (ii)
     * the latent vector, and (iii) the weight vector.
     *
     * @author dl
     */
    public static class SvdppValue {
        private float baseline;
        private FloatMatrix factors;
        private FloatMatrix weight;

        public SvdppValue() {
        }

        public float getBaseline() {
            return baseline;
        }

        public FloatMatrix getFactors() {
            return factors;
        }

        public FloatMatrix getWeight() {
            return weight;
        }

        public SvdppValue(
            float baseline, FloatMatrix factors,
            FloatMatrix weight
        ) {
            this.baseline = baseline;
            this.factors = factors;
            this.weight = weight;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SvdppValue that = (SvdppValue) o;
            return Float.compare(that.baseline, baseline) == 0 &&
                Objects.equals(factors, that.factors) &&
                Objects.equals(weight, that.weight);
        }

        @Override
        public int hashCode() {
            return Objects.hash(baseline, factors, weight);
        }

        @Override
        public String toString() {
            return factors.toString();
        }
    }

    /**
     * This computation class is used to initialize the factors of the user nodes
     * in the very first superstep, and send the first updates to the item nodes.
     *
     * @author dl
     */
    public class InitUsersComputation implements ComputeFunction<CfLongId,
        SvdppValue, Float, FloatMatrixMessage> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<CfLongId, SvdppValue> vertex,
            Iterable<FloatMatrixMessage> messages,
            Iterable<EdgeWithValue<CfLongId, Float>> edges,
            Callback<CfLongId, SvdppValue, Float, FloatMatrixMessage> cb
        ) {

            // Aggregate ratings. Necessary to compute the mean rating.
            double sum = 0;
            for (EdgeWithValue<CfLongId, Float> edge : edges) {
                sum += edge.value();
            }
            cb.aggregate(OVERALL_RATING_AGGREGATOR, sum);

            // Initialize the baseline estimate and the factor vector.

            int vectorSize = (Integer) configs.getOrDefault(VECTOR_SIZE, VECTOR_SIZE_DEFAULT);

            FloatMatrix factors = new FloatMatrix(1, vectorSize);

            Random randGen = new Random(0);
            for (int i = 0; i < factors.length; i++) {
                factors.put(i, 0.01f * randGen.nextFloat());
            }

            float baseline = randGen.nextFloat();

            cb.setNewVertexValue(new SvdppValue(baseline, factors, new FloatMatrix(0))); // The weights vector is empty for users

            // Send ratings to all items so that they can create the reverse edges.
            for (EdgeWithValue<CfLongId, Float> edge : edges) {
                FloatMatrixMessage msg = new FloatMatrixMessage(
                    vertex.id(),
                    new FloatMatrix(0), // the matrix of this message is empty
                    edge.value()
                );    // because we only need the rating
                cb.sendMessageTo(edge.target(), msg);
            }

            cb.voteToHalt();
        }
    }

    public class InitItemsComputation implements ComputeFunction<CfLongId,
        SvdppValue, Float, FloatMatrixMessage> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<CfLongId, SvdppValue> vertex,
            Iterable<FloatMatrixMessage> messages,
            Iterable<EdgeWithValue<CfLongId, Float>> edges,
            ComputeFunction.Callback<CfLongId, SvdppValue, Float, FloatMatrixMessage> cb
        ) {

            // Create the reverse edges
            for (FloatMatrixMessage msg : messages) {
                cb.addEdge(msg.getSenderId(), msg.getScore());
            }

            // Initialize baseline estimate and the factor and weight vectors

            int vectorSize = (Integer) configs.getOrDefault(VECTOR_SIZE, VECTOR_SIZE_DEFAULT);

            FloatMatrix factors = new FloatMatrix(1, vectorSize);
            FloatMatrix weight = new FloatMatrix(1, vectorSize);

            Random randGen = new Random(0);
            for (int i = 0; i < factors.length; i++) {
                factors.put(i, 0.01f * randGen.nextFloat());
                weight.put(i, 0.01f * randGen.nextFloat());
            }
            float baseline = randGen.nextFloat();

            cb.setNewVertexValue(new SvdppValue(baseline, factors, weight));

            // Start iterations by sending vectors to users
            FloatMatrix packedVectors = new FloatMatrix(2, vectorSize);
            packedVectors.putRow(0, factors);
            packedVectors.putRow(1, weight);

            for (EdgeWithValue<CfLongId, Float> edge : edges) {
                cb.sendMessageTo(edge.target(), new FloatMatrixMessage(vertex.id(), packedVectors, baseline));
            }

            cb.voteToHalt();
        }
    }

    public class UserComputation implements ComputeFunction<CfLongId,
        SvdppValue, Float, FloatMatrixMessage> {

        private float biasLambda;
        private float biasGamma;
        private float factorLambda;
        private float factorGamma;
        private float minRating;
        private float maxRating;
        private int vectorSize;
        private float meanRating;

        protected void updateValue(
            FloatMatrix user, FloatMatrix item,
            final float error, final float gamma, final float lambda
        ) {

            user.addi(user.mul(-lambda * gamma).addi(item.mul(error * gamma)));
        }

        @Override
        public void preSuperstep(int superstep, Aggregators aggregators) {
            factorLambda = (Float) configs.getOrDefault(
                FACTOR_LAMBDA,
                FACTOR_LAMBDA_DEFAULT
            );
            factorGamma = (Float) configs.getOrDefault(
                FACTOR_GAMMA,
                FACTOR_GAMMA_DEFAULT
            );
            biasLambda = (Float) configs.getOrDefault(
                BIAS_LAMBDA,
                BIAS_LAMBDA_DEFAULT
            );
            biasGamma = (Float) configs.getOrDefault(
                BIAS_GAMMA,
                BIAS_GAMMA_DEFAULT
            );
            minRating = (Float) configs.getOrDefault(
                MIN_RATING,
                MIN_RATING_DEFAULT
            );
            maxRating = (Float) configs.getOrDefault(
                MAX_RATING,
                MAX_RATING_DEFAULT
            );
            vectorSize = (Integer) configs.getOrDefault(
                VECTOR_SIZE,
                VECTOR_SIZE_DEFAULT
            );
            meanRating = (float) ((Double) aggregators.getAggregatedValue(
                OVERALL_RATING_AGGREGATOR) / (getTotalNumEdges(aggregators) * 2));
        }

        @Override
        public void compute(
            int superstep,
            VertexWithValue<CfLongId, SvdppValue> vertex,
            Iterable<FloatMatrixMessage> messages,
            Iterable<EdgeWithValue<CfLongId, Float>> edges,
            ComputeFunction.Callback<CfLongId, SvdppValue, Float, FloatMatrixMessage> cb
        ) {

            double rmsePartialSum = 0d;

            float userBaseline = vertex.value().getBaseline();
            int numRatings = 0;
            Map<CfLongId, Float> edgeValues = new HashMap<>();
            for (EdgeWithValue<CfLongId, Float> edge : edges) {
                numRatings++;
                edgeValues.put(edge.target(), edge.value());
            }
            FloatMatrix userFactors = vertex.value().getFactors();

            FloatMatrix sumWeights = new FloatMatrix(1, vectorSize);
            for (FloatMatrixMessage msg : messages) {
                // The weights are in the 2nd row of the matrix
                sumWeights.addi(msg.getFactors().getRow(1));
            }

            FloatMatrix itemWeightStep = new FloatMatrix(1, vectorSize);

            for (FloatMatrixMessage msg : messages) {
                // row 1 of the matrix in the message holds the item factors
                FloatMatrix itemFactors = msg.getFactors().getRow(0);
                // score holds the item baseline estimate
                float itemBaseline = msg.getScore();

                float observed = edgeValues.get(msg.getSenderId());
                float predicted = computePredictedRating(
                    meanRating, userBaseline, itemBaseline,
                    userFactors, itemFactors,
                    numRatings, sumWeights, minRating, maxRating
                );
                float error = predicted - observed;

                // Update baseline
                userBaseline = computeUpdatedBaseLine(userBaseline, predicted,
                    observed, biasGamma, biasLambda
                );

                // Update the value
                updateValue(userFactors, itemFactors, error, factorGamma, factorLambda);

                itemWeightStep.addi(itemFactors.mul(error));
            }

            cb.setNewVertexValue(new SvdppValue(userBaseline, vertex.value().factors, vertex.value().weight));

            itemWeightStep.muli(factorGamma / (float) Math.sqrt(numRatings));

            // Now we iterate again to get the new predictions and send the updates
            // to each item.
            for (FloatMatrixMessage msg : messages) {
                FloatMatrix itemFactors = msg.getFactors().getRow(0);
                float itemBaseline = msg.getScore();
                float observed = edgeValues.get(msg.getSenderId());
                float predicted = computePredictedRating(
                    meanRating, userBaseline, itemBaseline,
                    userFactors, itemFactors,
                    numRatings, sumWeights, minRating, maxRating
                );
                float error = predicted - observed;
                float itemBiasStep = biasGamma * error;
                FloatMatrix itemFactorStep =
                    sumWeights.mul(1f / (float) Math.sqrt(numRatings)).add(
                        userFactors).mul(factorGamma * error);

                FloatMatrix packedVectors = new FloatMatrix(2, vectorSize);
                packedVectors.putRow(0, itemFactorStep);
                packedVectors.putRow(1, itemWeightStep);

                rmsePartialSum += (error * error);

                cb.sendMessageTo(
                    msg.getSenderId(),
                    new FloatMatrixMessage(
                        vertex.id(), packedVectors, itemBiasStep)
                );
            }

            cb.aggregate(RMSE_AGGREGATOR, rmsePartialSum);

            cb.voteToHalt();
        }
    }

    public class ItemComputation implements ComputeFunction<CfLongId,
        SvdppValue, Float, FloatMatrixMessage> {

        private float biasLambda;
        private float biasGamma;
        private float factorLambda;
        private float factorGamma;
        private int vectorSize;

        @Override
        public void preSuperstep(int superstep, Aggregators aggregators) {
            biasLambda = (Float) configs.getOrDefault(
                BIAS_LAMBDA,
                BIAS_LAMBDA_DEFAULT
            );
            biasGamma = (Float) configs.getOrDefault(
                BIAS_GAMMA,
                BIAS_GAMMA_DEFAULT
            );
            factorLambda = (Float) configs.getOrDefault(
                FACTOR_LAMBDA,
                FACTOR_LAMBDA_DEFAULT
            );
            factorGamma = (Float) configs.getOrDefault(
                FACTOR_GAMMA,
                FACTOR_GAMMA_DEFAULT
            );
            vectorSize = (Integer) configs.getOrDefault(
                VECTOR_SIZE,
                VECTOR_SIZE_DEFAULT
            );
        }

        @Override
        public void compute(
            int superstep,
            VertexWithValue<CfLongId, SvdppValue> vertex,
            Iterable<FloatMatrixMessage> messages,
            Iterable<EdgeWithValue<CfLongId, Float>> edges,
            ComputeFunction.Callback<CfLongId, SvdppValue, Float, FloatMatrixMessage> cb
        ) {

            float itemBaseline = vertex.value().getBaseline();
            FloatMatrix itemFactors = vertex.value().getFactors();
            FloatMatrix itemWeights = vertex.value().getWeight();

            for (FloatMatrixMessage msg : messages) {
                float itemBiasStep = msg.getScore();
                FloatMatrix itemFactorStep = msg.getFactors().getRow(0);
                FloatMatrix itemWeightStep = msg.getFactors().getRow(1);

                itemBaseline = incrementValue(itemBaseline, itemBiasStep, biasGamma,
                    biasLambda
                );
                incrementValue(itemFactors, itemFactorStep, factorGamma, factorLambda);
                incrementValue(itemWeights, itemWeightStep, factorGamma, factorLambda);
            }

            FloatMatrix packedVectors = new FloatMatrix(2, vectorSize);
            packedVectors.putRow(0, itemFactors);
            packedVectors.putRow(1, itemWeights);

            for (EdgeWithValue<CfLongId, Float> edge : edges) {
                cb.sendMessageTo(edge.target(), new FloatMatrixMessage(vertex.id(), packedVectors, itemBaseline));
            }

            cb.setNewVertexValue(new SvdppValue(itemBaseline, vertex.value().factors, vertex.value().weight));
            cb.voteToHalt();
        }
    }

    public static class EdgeCount implements ComputeFunction<CfLongId,
          SvdppValue, Float, FloatMatrixMessage> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<CfLongId, SvdppValue> vertex,
            Iterable<FloatMatrixMessage> messages,
            Iterable<EdgeWithValue<CfLongId, Float>> edges,
            Callback<CfLongId, SvdppValue, Float, FloatMatrixMessage> cb
        ) {
            if (superstep == 0) {
                long count = 0L;
                for (EdgeWithValue<CfLongId, Float> edge : edges) {
                    count++;
                }
                cb.aggregate(EDGE_COUNT_AGGREGATOR, count);
            }
        }
    }

    private int maxIterations;
    private float rmseTarget;

    @Override
    @SuppressWarnings("unchecked")
    public final void init(Map<String, ?> configs, ComputeFunction.InitCallback cb) {
        this.configs = (Map<String, Object>) configs;

        cb.registerAggregator(EDGE_COUNT_AGGREGATOR, LongSumAggregator.class, true);
        cb.registerAggregator(RMSE_AGGREGATOR, DoubleSumAggregator.class);
        cb.registerAggregator(OVERALL_RATING_AGGREGATOR, DoubleSumAggregator.class, true);

        maxIterations = (Integer) this.configs.getOrDefault(
            ITERATIONS,
            ITERATIONS_DEFAULT
        );
        rmseTarget = (Float) this.configs.getOrDefault(
            RMSE_TARGET,
            RMSE_TARGET_DEFAULT
        );
    }

    @Override
    public final void masterCompute(int superstep, ComputeFunction.MasterCallback cb) {
        long numRatings = getTotalNumEdges(cb);
        double rmse = Math.sqrt(((Double) cb.getAggregatedValue(RMSE_AGGREGATOR)) / numRatings);

        if (rmseTarget > 0f && rmse < rmseTarget) {
            cb.haltComputation();
        } else if (superstep > maxIterations) {
            cb.haltComputation();
        }
    }

    private final UserComputation userComputation = new UserComputation();
    private final ItemComputation itemComputation = new ItemComputation();

    @Override
    public void preSuperstep(int superstep, Aggregators aggregators) {
        if (superstep == 0) {
        } else if (superstep == 1) {
        } else if (superstep == 2) {
        } else if (superstep % 2 != 0) {
            userComputation.preSuperstep(superstep, aggregators);
        } else {
            itemComputation.preSuperstep(superstep, aggregators);
        }
    }

    @Override
    public void compute(
        int superstep,
        VertexWithValue<CfLongId, SvdppValue> vertex,
        Iterable<FloatMatrixMessage> messages,
        Iterable<EdgeWithValue<CfLongId, Float>> edges,
        Callback<CfLongId, SvdppValue, Float, FloatMatrixMessage> cb
    ) {
        if (superstep == 0) {
            new EdgeCount().compute(superstep, vertex, messages, edges, cb);
        } else if (superstep == 1) {
            new InitUsersComputation().compute(superstep, vertex, messages, edges, cb);
        } else if (superstep == 2) {
            new InitItemsComputation().compute(superstep, vertex, messages, edges, cb);
        } else if (superstep % 2 != 0) {
            userComputation.compute(superstep, vertex, messages, edges, cb);
        } else {
            itemComputation.compute(superstep, vertex, messages, edges, cb);
        }
    }

    // Returns the total number of edges before adding reverse edges
    protected long getTotalNumEdges(ReadAggregators aggregators) {
        return aggregators.getAggregatedValue(EDGE_COUNT_AGGREGATOR);
    }
}
