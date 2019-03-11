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
package io.kgraph.library.clustering;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.ComputeFunction;


/**
 * Implements the Semi-Clustering algorithm as presented in the Pregel paper
 * from SIGMOD'10.
 * <p>
 * The input to the algorithm is an undirected weighted graph and the output
 * is a set of clusters with each vertex potentially belonging to multiple
 * clusters.
 * <p>
 * A semi-cluster is assigned a score S=(I-f*B)/(V(V-1)/2), where I is the sum
 * of weights of all internal edges, B is the sum of weights of all boundary
 * edges, V is the number of vertices in the semi-cluster, f is a user-specified
 * boundary edge score factor with a value between 0 and 1.
 * <p>
 * Each vertex maintains a list containing a maximum number of  semi-clusters,
 * sorted by score. The lists gets greedily updated in an iterative manner.
 * <p>
 * The algorithm finishes when the semi-cluster lists don't change or after a
 * maximum number of iterations.
 */
public class SemiClustering implements ComputeFunction<Long, Set<SemiClustering.SemiCluster>, Double, Set<SemiClustering.SemiCluster>> {

    /**
     * Maximum number of iterations.
     */
    public static final String ITERATIONS = "iterations";
    /**
     * Default value for ITERATIONS.
     */
    public static final int ITERATIONS_DEFAULT = 10;
    /**
     * Maximum number of semi-clusters.
     */
    public static final String MAX_CLUSTERS = "max.clusters";
    /**
     * Default value for maximum number of semi-clusters.
     */
    public static final int MAX_CLUSTERS_DEFAULT = 2;
    /**
     * Maximum number of vertices in a semi-cluster.
     */
    public static final String CLUSTER_CAPACITY = "cluster.capacity";
    /**
     * Default value for cluster capacity.
     */
    public static final int CLUSTER_CAPACITY_DEFAULT = 4;
    /**
     * Boundary edge score factor.
     */
    public static final String SCORE_FACTOR = "score.factor";
    /**
     * Default value for Boundary Edge Score Factor.
     */
    public static final double SCORE_FACTOR_DEFAULT = 0.5d;
    /**
     * Comparator to sort clusters in the list based on their score.
     */
    private static final ClusterScoreComparator scoreComparator =
        new ClusterScoreComparator();

    private Map<String, Object> configs;

    @SuppressWarnings("unchecked")
    @Override
    public final void init(Map<String, ?> configs, InitCallback cb) {
        this.configs = (Map<String, Object>) configs;
    }

    /**
     * Compute method.
     *
     * @param messages Messages received
     */
    @Override
    public void compute(
        int superstep,
        VertexWithValue<Long, Set<SemiCluster>> vertex,
        Iterable<Set<SemiCluster>> messages,
        Iterable<EdgeWithValue<Long, Double>> edges,
        Callback<Long, Set<SemiCluster>, Double, Set<SemiCluster>> cb
    ) {
        int iterations = (Integer) configs.getOrDefault(ITERATIONS, ITERATIONS_DEFAULT);
        int maxClusters = (Integer) configs.getOrDefault(MAX_CLUSTERS, MAX_CLUSTERS_DEFAULT);
        int clusterCapacity = (Integer) configs.getOrDefault(CLUSTER_CAPACITY, CLUSTER_CAPACITY_DEFAULT);
        double scoreFactor = (Double) configs.getOrDefault(SCORE_FACTOR, SCORE_FACTOR_DEFAULT);

        // If this is the first superstep, initialize cluster list with a single
        // cluster that contains only the current vertex, and send it to all
        // neighbors.
        if (superstep == 0) {
            SemiCluster myCluster = new SemiCluster();
            myCluster.addVertex(vertex, edges, scoreFactor);

            Set<SemiCluster> clusterList = new TreeSet<>(scoreComparator);
            clusterList.add(myCluster);

            cb.setNewVertexValue(clusterList);
            for (EdgeWithValue<Long, Double> edge : edges) {
                cb.sendMessageTo(edge.target(), clusterList);
            }
            cb.voteToHalt();
            return;
        }

        if (superstep == iterations) {
            cb.voteToHalt();
            return;
        }

        // For every cluster list received from neighbors and for every cluster in
        // the list, add current vertex if it doesn't already exist in the cluster
        // and the cluster is not full.
        //
        // Sort clusters received and newly formed clusters and send the top to all
        // neighbors
        //
        // Furthermore, update this vertex's list with received and newly formed
        // clusters the contain this vertex, sort them, and keep the top ones.

        Set<SemiCluster> unionedClusterSet = new TreeSet<>(scoreComparator);
        Set<SemiCluster> newVertexValue = new TreeSet<>(scoreComparator);

        for (Set<SemiCluster> clusterSet : messages) {
            unionedClusterSet.addAll(clusterSet);

            for (SemiCluster cluster : clusterSet) {
                boolean contains = cluster.vertices.contains(vertex.id());
                if (!contains && cluster.vertices.size() < clusterCapacity) {
                    SemiCluster newCluster = new SemiCluster(cluster);
                    newCluster.addVertex(vertex, edges, scoreFactor);
                    unionedClusterSet.add(newCluster);
                    newVertexValue.add(newCluster);
                } else if (contains) {
                    newVertexValue.add(cluster);
                }
            }
        }

        // If we have more than a maximum number of clusters, then we remove the
        // ones with the lowest score.
        Iterator<SemiCluster> iterator = unionedClusterSet.iterator();
        while (unionedClusterSet.size() > maxClusters) {
            iterator.next();
            iterator.remove();
        }

        iterator = newVertexValue.iterator();
        while (newVertexValue.size() > maxClusters) {
            iterator.next();
            iterator.remove();
        }

        cb.setNewVertexValue(newVertexValue);
        for (EdgeWithValue<Long, Double> edge : edges) {
            cb.sendMessageTo(edge.target(), unionedClusterSet);
        }
        cb.voteToHalt();
    }

    /**
     * Comparator that sorts semi-clusters according to their score.
     */
    private static class ClusterScoreComparator implements Comparator<SemiCluster>, Serializable {

        private static final long serialVersionUID = 9205668712825966861L;

        /**
         * Compare two semi-clusters for order.
         *
         * @param o1 the first object
         * @param o2 the second object
         * @return -1 if score for object1 is smaller than score of object2,
         * 1 if score for object2 is smaller than score of object1
         * or 0 if both scores are the same
         */
        @Override
        public int compare(final SemiCluster o1, final SemiCluster o2) {
            int cmp = Double.compare(o1.score, o2.score);
            if (cmp != 0) {
                return cmp;
            } else {
                // We add this for consistency with the equals() method.
                if (!o1.equals(o2)) {
                    return 1;
                }
            }
            return 0;
        }
    }

    /**
     * This class represents a semi-cluster.
     */
    public static class SemiCluster implements Comparable<SemiCluster> {

        /**
         * List of vertices
         */
        private final Set<Long> vertices;
        /**
         * Score of current semi cluster.
         */
        private double score;
        /**
         * Inner Score.
         */
        private double innerScore;
        /**
         * Boundary Score.
         */
        private double boundaryScore;

        /**
         * Constructor: Create a new empty Cluster.
         */
        public SemiCluster() {
            vertices = new HashSet<>();
            score = 1d;
            innerScore = 0d;
            boundaryScore = 0d;
        }

        /**
         * Constructor: Initialize a new Cluster.
         *
         * @param cluster cluster object to initialize the new object
         */
        public SemiCluster(final SemiCluster cluster) {
            vertices = new HashSet<>();
            vertices.addAll(cluster.vertices);
            score = cluster.score;
            innerScore = cluster.innerScore;
            boundaryScore = cluster.boundaryScore;
        }

        /**
         * Adds a vertex to the cluster.
         * <p>
         * Every time a vertex is added we also update the inner and boundary score.
         * Because vertices are only added to a semi-cluster, we can save the inner
         * and boundary scores and update them incrementally.
         * <p>
         * Otherwise, in order to re-compute it from scratch we would need every
         * vertex to send a friends-of-friends list, which is very expensive.
         *
         * @param vertex      The new vertex to be added into the cluster
         * @param scoreFactor Boundary Edge Score Factor
         */
        public final void addVertex(
            final VertexWithValue<Long, Set<SemiCluster>> vertex,
            Iterable<EdgeWithValue<Long, Double>> edges,
            final double scoreFactor
        ) {
            long vertexId = vertex.id();

            if (vertices.add(vertexId)) {
                if (size() == 1) {
                    for (EdgeWithValue<Long, Double> edge : edges) {
                        boundaryScore += edge.value();
                    }
                    score = 0.0;
                } else {
                    for (EdgeWithValue<Long, Double> edge : edges) {
                        if (vertices.contains(edge.target())) {
                            innerScore += edge.value();
                            boundaryScore -= edge.value();
                        } else {
                            boundaryScore += edge.value();
                        }
                    }
                    score = (innerScore - scoreFactor * boundaryScore)
                        / (size() * (size() - 1) / 2.0);
                }
            }
        }

        /**
         * Returns size of semi cluster list.
         *
         * @return Number of semi clusters in the list
         */
        public final int size() {
            return vertices.size();
        }

        /**
         * Two semi clusters are the same when:
         * (i) they have the same number of vertices,
         * (ii) all their vertices are the same
         *
         * @param other Cluster to be compared with current cluster
         * @return 0 if two clusters are the same
         */
        @Override
        public final int compareTo(final SemiCluster other) {
            if (other == null) {
                return 1;
            }
            if (this.size() < other.size()) {
                return -1;
            }
            if (this.size() > other.size()) {
                return 1;
            }
            if (other.vertices.containsAll(vertices)) {
                return 0;
            }
            return -1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SemiCluster that = (SemiCluster) o;
            return Double.compare(that.score, score) == 0 &&
                Double.compare(that.innerScore, innerScore) == 0 &&
                Double.compare(that.boundaryScore, boundaryScore) == 0 &&
                Objects.equals(vertices, that.vertices);
        }

        @Override
        public int hashCode() {
            return Objects.hash(vertices, score, innerScore, boundaryScore);
        }

        @Override
        public final String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("[ ");
            for (Long v : this.vertices) {
                builder.append(v.toString());
                builder.append(" ");
            }
            builder.append(" | " + score + ", " + innerScore + ", "
                + boundaryScore + " ]");
            return builder.toString();
        }
    }
}