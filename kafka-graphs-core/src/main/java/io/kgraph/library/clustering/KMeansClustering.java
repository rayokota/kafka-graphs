/*
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
package io.kgraph.library.clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.ComputeFunction;
import io.kgraph.pregel.aggregators.LongSumAggregator;

/**
 * The k-means clustering algorithm partitions <code>N</code> data points (observations) into <code>k</code> clusters.
 * The input consists of data points with a <code>pointID</code> and a vector of coordinates.
 * <p>
 * The algorithm is iterative and works as follows.
 * In the initialization phase, <code>k</code> clusters are chosen from the input points at random.
 * <p>
 * In each iteration:
 * 1. each data point is assigned to the cluster center which is closest to it, by means of euclidean distance
 * 2. new cluster centers are recomputed, by calculating the arithmetic mean of the assigned points
 * <p>
 * Convergence is reached when the positions of the cluster centers do not change.
 * <p>
 * http://en.wikipedia.org/wiki/K-means_clustering
 */
public class KMeansClustering<EV, Message> implements ComputeFunction<Long, KMeansVertexValue, EV, Message> {

    /**
     * The prefix for the cluster center aggregators, used to store the cluster centers coordinates.
     * Each of them aggregates the vector coordinates of points assigned to the cluster center,
     * in order to compute the means as the coordinates of the new cluster centers.
     */
    public static final String CENTER_AGGR_PREFIX = "center.aggr.prefix";

    /**
     * The prefix for the aggregators used to store the number of points
     * assigned to each cluster center
     */
    public static final String ASSIGNED_POINTS_PREFIX = "assigned.points.prefix";

    /**
     * The initial centers aggregator
     */
    public static final String INITIAL_CENTERS = "kmeans.initial.centers";

    /**
     * Maximum number of iterations
     */
    public static final String MAX_ITERATIONS = "kmeans.iterations";
    /**
     * Default value for iterations
     */
    public static final int ITERATIONS_DEFAULT = 100;
    /**
     * Number of cluster centers
     */
    public static final String CLUSTER_CENTERS_COUNT = "kmeans.cluster.centers.count";
    /**
     * Default number of cluster centers
     */
    public static final int CLUSTER_CENTERS_COUNT_DEFAULT = 3;
    /**
     * Dimensions of the input points
     */
    public static final String DIMENSIONS = "kmeans.points.dimensions";
    /**
     * Total number of input points
     */
    public static final String POINTS_COUNT = "kmeans.points.count";
    /**
     * Parameter that enables printing the final centers coordinates
     **/
    public static final String PRINT_FINAL_CENTERS = "kmeans.print.final.centers";
    /**
     * False by default
     **/
    public static final boolean PRINT_FINAL_CENTERS_DEFAULT = false;
    /**
     * False by default
     **/
    public static final String TEST_INITIAL_CENTERS = "test.initial.centers";

    public class RandomCentersInitialization implements
        ComputeFunction<Long, KMeansVertexValue, EV, Message> {

        @Override
        public void compute(
            int superstep,
            VertexWithValue<Long, KMeansVertexValue> vertex,
            Iterable<Message> messages,
            Iterable<EdgeWithValue<Long, EV>> edges,
            Callback<Long, KMeansVertexValue, EV, Message> cb
        ) {
            if (configs.get(TEST_INITIAL_CENTERS) != null) {
                return;
            }
            List<List<Double>> value = new ArrayList<>();
            value.add(vertex.value().getPointCoordinates());
            cb.aggregate(INITIAL_CENTERS, value);
        }
    }

    public void superstepCompute(
        int superstep,
        VertexWithValue<Long, KMeansVertexValue> vertex,
        Iterable<Message> messages,
        Iterable<EdgeWithValue<Long, EV>> edges,
        Callback<Long, KMeansVertexValue, EV, Message> cb
    ) {
        KMeansVertexValue currentValue = vertex.value();
        final List<Double> pointCoordinates = currentValue.getPointCoordinates();
        // read the cluster centers coordinates
        List<Double>[] clusterCenters = readClusterCenters(cb, CENTER_AGGR_PREFIX);
        // find the closest center
        final int centerId = findClosestCenter(clusterCenters, currentValue.getPointCoordinates());
        // aggregate this point's coordinates to the cluster centers aggregator
        cb.aggregate(CENTER_AGGR_PREFIX + "C_" + centerId, pointCoordinates);
        // increase the count of assigned points for this cluster center
        cb.aggregate(ASSIGNED_POINTS_PREFIX + "C_" + centerId, 1L);
        // set the cluster id in the vertex value
        cb.setNewVertexValue(new KMeansVertexValue(vertex.value().getPointCoordinates(), centerId));
    }

    @SuppressWarnings("unchecked")
    private List<Double>[] readClusterCenters(Callback<Long, KMeansVertexValue, EV, Message> cb, String prefix) {
        List<Double>[] centers = new List[clustersCount];
        for (int i = 0; i < clustersCount; i++) {
            centers[i] = cb.getAggregatedValue(prefix + "C_" + i);
        }
        return centers;
    }

    /**
     * finds the closest center to the given point
     * by minimizing the Euclidean distance
     *
     * @param clusterCenters
     * @param value
     * @return the index of the cluster center in the clusterCenters vector
     */
    private int findClosestCenter(List<Double>[] clusterCenters, List<Double> value) {
        double minDistance = Double.MAX_VALUE;
        double distanceFromI;
        int clusterIndex = 0;
        for (int i = 0; i < clusterCenters.length; i++) {
            distanceFromI = euclideanDistance(clusterCenters[i], value,
                clusterCenters[i].size()
            );
            if (distanceFromI < minDistance) {
                minDistance = distanceFromI;
                clusterIndex = i;
            }
        }
        return clusterIndex;
    }

    /**
     * Calculates the Euclidean distance between two vectors of doubles
     *
     * @param v1
     * @param v2
     * @param dim
     * @return
     */
    private double euclideanDistance(List<Double> v1, List<Double> v2, int dim) {
        double distance = 0.0;
        for (int i = 0; i < dim; i++) {
            distance += Math.pow(v1.get(i) - v2.get(i), 2);
        }
        return Math.sqrt(distance);
    }

    /**
     * The Master calculates the new cluster centers
     * and also checks for convergence
     */
    private Map<String, Object> configs;
    private int maxIterations;
    private List<Double>[] currentClusterCenters;
    private int clustersCount;
    private int dimensions;

    @SuppressWarnings("unchecked")
    @Override
    public final void init(Map<String, ?> configs, InitCallback cb) {
        this.configs = (Map<String, Object>) configs;
        maxIterations = (Integer) this.configs.getOrDefault(
            MAX_ITERATIONS,
            ITERATIONS_DEFAULT
        );
        clustersCount = (Integer) this.configs.getOrDefault(
            CLUSTER_CENTERS_COUNT,
            CLUSTER_CENTERS_COUNT_DEFAULT
        );
        dimensions = (Integer) this.configs.getOrDefault(DIMENSIONS, 0);
        currentClusterCenters = new List[clustersCount];
        // register initial centers aggregator
        cb.registerAggregator(INITIAL_CENTERS, ListOfDoubleListAggregator.class);
        // register aggregators, one per center for the coordinates and
        // one per center for counts of assigned elements
        for (int i = 0; i < clustersCount; i++) {
            cb.registerAggregator(CENTER_AGGR_PREFIX + "C_" + i, DoubleListAggregator.class);
            cb.registerAggregator(ASSIGNED_POINTS_PREFIX + "C_" + i, LongSumAggregator.class);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public final void masterCompute(int superstep, MasterCallback cb) {
        if (superstep == 1) {
            // initialize the centers aggregators
            List<List<Double>> initialCenters = (List<List<Double>>) configs.get(TEST_INITIAL_CENTERS);
            if (initialCenters == null) {
                initialCenters = cb.getAggregatedValue(INITIAL_CENTERS);
            }
            for (int i = 0; i < clustersCount; i++) {
                cb.setAggregatedValue(CENTER_AGGR_PREFIX + "C_" + i, initialCenters.get(i));
                currentClusterCenters[i] = initialCenters.get(i);
            }
        } else if (superstep > 1) {
            // compute the new centers positions
            List<Double>[] newClusters = computeClusterCenters(cb);
            //check for convergence
            if ((superstep > maxIterations) || (clusterPositionsDiff(currentClusterCenters, newClusters))) {

                // if enabled, print the final centers coordinates
                if ((Boolean) configs.getOrDefault(PRINT_FINAL_CENTERS, PRINT_FINAL_CENTERS_DEFAULT)) {
                    printFinalCentersCoordinates();
                }

                cb.haltComputation();
            } else {
                // update the aggregators with the new cluster centers
                for (int i = 0; i < clustersCount; i++) {
                    cb.setAggregatedValue(CENTER_AGGR_PREFIX + "C_" + i, newClusters[i]);
                }
                currentClusterCenters = newClusters;
            }
        }
    }

    @Override
    public void compute(
        int superstep,
        VertexWithValue<Long, KMeansVertexValue> vertex,
        Iterable<Message> messages,
        Iterable<EdgeWithValue<Long, EV>> edges,
        Callback<Long, KMeansVertexValue, EV, Message> cb
    ) {
        if (superstep == 0) {
            new RandomCentersInitialization().compute(superstep, vertex, messages, edges, cb);
        } else {
            superstepCompute(superstep, vertex, messages, edges, cb);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Double>[] computeClusterCenters(MasterCallback cb) {
        List<Double>[] newClusterCenters = new List[clustersCount];
        List<Double> clusterCoordinates;
        long assignedPoints;
        for (int i = 0; i < clustersCount; i++) {
            clusterCoordinates = cb.getAggregatedValue(CENTER_AGGR_PREFIX + "C_" + i);
            assignedPoints = cb.getAggregatedValue(ASSIGNED_POINTS_PREFIX + "C_" + i);
            for (int j = 0; j < clusterCoordinates.size(); j++) {
                clusterCoordinates.set(j, clusterCoordinates.get(j) / assignedPoints);
            }
            newClusterCenters[i] = clusterCoordinates;
        }
        return newClusterCenters;
    }

    private boolean clusterPositionsDiff(
        List<Double>[] currentClusterCenters,
        List<Double>[] newClusters
    ) {
        final double E = 0.001f;
        double diff = 0;
        for (int i = 0; i < clustersCount; i++) {
            for (int j = 0; j < dimensions; j++) {
                diff += Math.abs(currentClusterCenters[i].get(j) - newClusters[i].get(j));
            }
        }
        return diff <= E;
    }

    private void printFinalCentersCoordinates() {
        System.out.println("Centers Coordinates: ");
        for (int i = 0; i < clustersCount; i++) {
            System.out.print("cluster id " + i + ": ");
            for (int j = 0; j < currentClusterCenters[i].size(); j++) {
                System.out.print(currentClusterCenters[i].get(j) + " ");
            }
            System.out.println();
        }
    }
}
