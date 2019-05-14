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
import org.jblas.JavaBlas;
import org.jblas.Solve;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.library.basic.EdgeCount;
import io.kgraph.pregel.ComputeFunction;
import io.kgraph.pregel.aggregators.DoubleSumAggregator;
import io.kgraph.pregel.aggregators.LongSumAggregator;

/**
 * Alternating Least Squares (ALS) implementation.
 */
public class Als implements ComputeFunction<CfLongId, FloatMatrix, Float, FloatMatrixMessage> {

    /**
     * RMSE target to reach.
     */
    public static final String RMSE_TARGET = "rmse";
    /**
     * Default value of RMSE target.
     */
    public static final float RMSE_TARGET_DEFAULT = -1f;
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
     * Keyword for parameter setting the Latent Vector Size.
     */
    public static final String VECTOR_SIZE = "dim";
    /**
     * Default value for vector size.
     */
    public static final int VECTOR_SIZE_DEFAULT = 50;
    /**
     * Random seed.
     */
    public static final String RANDOM_SEED = "random.seed";
    /**
     * Default random seed
     */
    public static final Long RANDOM_SEED_DEFAULT = null;

    /**
     * Aggregator used to compute the RMSE
     */
    public static final String RMSE_AGGREGATOR = "als.rmse.aggregator";

    private float lambda;
    private int vectorSize;
    private Long randomSeed;

    private Map<String, Object> configs;

    @Override
    public void preSuperstep(int superstep, Aggregators aggregators) {
        lambda = (Float) configs.getOrDefault(LAMBDA, LAMBDA_DEFAULT);
        vectorSize = (Integer) configs.getOrDefault(VECTOR_SIZE, VECTOR_SIZE_DEFAULT);
        randomSeed = (Long) configs.getOrDefault(RANDOM_SEED, RANDOM_SEED_DEFAULT);
    }

    /**
     * Main ALS compute method.
     * <p>
     * It updates the current latent vector based on ALS:<br>
     * A = M * M^T + LAMBDA * N * E<br>
     * V = M * R<br>
     * A * U = V, then solve for U<br>
     * <br>
     * where<br>
     * R: column vector with ratings by this user<br>
     * M: item features for items rated by this user with dimensions |F|x|R|<br>
     * M^T: transpose of M with dimensions |R|x|F|<br>
     * N: number of ratings of this user<br>
     * E: identity matrix with dimensions |F|x|F|<br>
     *
     * @param superstep the count of the current superstep
     * @param vertex the current vertex with its value
     * @param messages a Map of the source vertex and the message sent from the previous superstep
     * @param edges the adjacent edges with their values
     * @param cb a callback for setting a new vertex value or sending messages to the next superstep
     */
    public void superstepCompute(
        int superstep,
        VertexWithValue<CfLongId, FloatMatrix> vertex,
        Iterable<FloatMatrixMessage> messages,
        Iterable<EdgeWithValue<CfLongId, Float>> edges,
        Callback<CfLongId, FloatMatrix, Float, FloatMatrixMessage> cb
    ) {
        int numEdges = 0;
        Map<CfLongId, Float> edgeValues = new HashMap<>();
        for (EdgeWithValue<CfLongId, Float> edge : edges) {
            numEdges++;
            edgeValues.put(edge.target(), edge.value());
        }
        FloatMatrix mat_M = new FloatMatrix(vectorSize, numEdges);
        FloatMatrix mat_R = new FloatMatrix(numEdges, 1);

        // Build the matrices of the linear system
        int i = 0;
        for (FloatMatrixMessage msg : messages) {
            mat_M.putColumn(i, msg.getFactors());
            mat_R.put(i, 0, edgeValues.get(msg.getSenderId()));
            i++;
        }

        updateValue(vertex.value(), mat_M, mat_R, lambda);

        // Calculate errors and add squares to the RMSE aggregator
        double rmsePartialSum = 0d;
        for (int j = 0; j < mat_M.columns; j++) {
            float prediction = vertex.value().dot(mat_M.getColumn(j));
            double error = prediction - mat_R.get(j, 0);
            rmsePartialSum += (error * error);
        }

        cb.aggregate(RMSE_AGGREGATOR, rmsePartialSum);

        // Propagate new value
        for (EdgeWithValue<CfLongId, Float> edge : edges) {
            cb.sendMessageTo(edge.target(), new FloatMatrixMessage(vertex.id(), vertex.value(), 0.0f));
        }

        cb.setNewVertexValue(vertex.value());
        cb.voteToHalt();
    }

    protected void updateValue(
        FloatMatrix value, FloatMatrix mat_M,
        FloatMatrix mat_R, final float lambda
    ) {

        FloatMatrix mat_V = mat_M.mmul(mat_R);
        FloatMatrix mat_A = mat_M.mmul(mat_M.transpose());
        mat_A.addi(FloatMatrix.eye(mat_M.rows).muli(lambda * mat_R.rows));

        FloatMatrix mat_U = Solve.solve(mat_A, mat_V);
        value.rows = mat_U.rows;
        value.columns = mat_U.columns;
        JavaBlas.rcopy(mat_U.length, mat_U.data, 0, 1, value.data, 0, 1);
    }

    /**
     * This computation class is used to initialize the factors of the user nodes
     * in the very first superstep, and send the first updates to the item nodes.
     *
     * @author dl
     */
    public class InitUsersComputation implements ComputeFunction<CfLongId,
        FloatMatrix, Float, FloatMatrixMessage> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<CfLongId, FloatMatrix> vertex,
            Iterable<FloatMatrixMessage> messages,
            Iterable<EdgeWithValue<CfLongId, Float>> edges,
            Callback<CfLongId, FloatMatrix, Float, FloatMatrixMessage> cb
        ) {
            FloatMatrix vector =
                new FloatMatrix((Integer) configs.getOrDefault(VECTOR_SIZE, VECTOR_SIZE_DEFAULT));
            Random randGen = randomSeed != null ? new Random(randomSeed) : new Random();
            for (int i = 0; i < vector.length; i++) {
                vector.put(i, 0.01f * randGen.nextFloat());
            }
            cb.setNewVertexValue(vector);

            for (EdgeWithValue<CfLongId, Float> edge : edges) {
                FloatMatrixMessage msg = new FloatMatrixMessage(
                    vertex.id(), vertex.value(), edge.value());
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
    public class InitItemsComputation implements ComputeFunction<CfLongId,
        FloatMatrix, Float, FloatMatrixMessage> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<CfLongId, FloatMatrix> vertex,
            Iterable<FloatMatrixMessage> messages,
            Iterable<EdgeWithValue<CfLongId, Float>> edges,
            Callback<CfLongId, FloatMatrix, Float, FloatMatrixMessage> cb
        ) {
            FloatMatrix vector =
                new FloatMatrix((Integer) configs.getOrDefault(VECTOR_SIZE, VECTOR_SIZE_DEFAULT));
            Random randGen = randomSeed != null ? new Random(randomSeed) : new Random();
            for (int i = 0; i < vector.length; i++) {
                vector.put(i, 0.01f * randGen.nextFloat());
            }
            cb.setNewVertexValue(vector);

            for (FloatMatrixMessage msg : messages) {
                cb.addEdge(msg.getSenderId(), msg.getScore());
            }

            // The score does not matter at this point.
            for (EdgeWithValue<CfLongId, Float> edge : edges) {
                FloatMatrixMessage msg = new FloatMatrixMessage(
                    vertex.id(), vertex.value(), 0.0f);
                cb.sendMessageTo(edge.target(), msg);
            }
            cb.voteToHalt();
        }
    }

    private int maxIterations;
    private float rmseTarget;

    @Override
    @SuppressWarnings("unchecked")
    public final void init(Map<String, ?> configs, InitCallback cb) {
        this.configs = (Map<String, Object>) configs;
        this.maxIterations = (Integer) this.configs.getOrDefault(ITERATIONS, ITERATIONS_DEFAULT);
        this.rmseTarget = (Float) this.configs.getOrDefault(RMSE_TARGET, RMSE_TARGET_DEFAULT);

        cb.registerAggregator(EdgeCount.EDGE_COUNT_AGGREGATOR, LongSumAggregator.class, true);
        cb.registerAggregator(RMSE_AGGREGATOR, DoubleSumAggregator.class);
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
