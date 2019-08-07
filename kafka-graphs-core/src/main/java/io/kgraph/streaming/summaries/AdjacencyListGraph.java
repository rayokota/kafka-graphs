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

package io.kgraph.streaming.summaries;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

/**
 * A simple, undirected adjacency list graph representation with methods for traversals.
 * Used in the Spanner library method.
 *
 * @param <K> the vertex id type
 */
public class AdjacencyListGraph<K extends Comparable<K>> {

    private final Map<K, Set<K>> adjacencyMap;
    private final int factorK;

    public AdjacencyListGraph(int factorK) {
        this.adjacencyMap = new HashMap<>();
        this.factorK = factorK;
    }

    public AdjacencyListGraph(AdjacencyListGraph<K> graph) {
        this.adjacencyMap = new HashMap<>();
        for (Map.Entry<K, Set<K>> entry : graph.adjacencyMap.entrySet()) {
            this.adjacencyMap.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        this.factorK = graph.factorK;
    }

    public AdjacencyListGraph(AdjacencyListGraph<K> graph, K src, K trg) {
        this(graph);
        if (src != null && trg != null) {
            addEdge(src, trg);
        }
    }

    protected Map<K, Set<K>> adjacencyMap() {
        return adjacencyMap;
    }

    public int size() {
        return adjacencyMap.size();
    }

    /**
     * Adds the edge to the current adjacency graph
     *
     * @param src the src id
     * @param trg the trg id
     */
    protected void addEdge(K src, K trg) {
        Set<K> neighbors = adjacencyMap.computeIfAbsent(src, k -> new HashSet<>());
        neighbors.add(trg);

        neighbors = adjacencyMap.computeIfAbsent(trg, k -> new HashSet<>());
        neighbors.add(src);
    }

    /**
     * Performs a bounded BFS on the adjacency graph to determine
     * whether the current distance between src and trg is greater than k.
     *
     * @param src the source
     * @param trg the target
     * @return true if the current distance is less than or equal to k
     * and false otherwise.
     */
    public boolean boundedBFS(K src, K trg) {
        if (!adjacencyMap.containsKey(src)) {
            // this is the first time we encounter this vertex
            return false;
        } else {
            Set<K> visited = new HashSet<>();
            Queue<Node> queue = new LinkedList<>();

            // add the src neighbors
            for (K neighbor : adjacencyMap.get(src)) {
                queue.add(new Node(neighbor, 1));
            }
            visited.add(src);

            while (!queue.isEmpty()) {
                Node current = queue.peek();
                if (current.id().equals(trg)) {
                    // we found the trg in <= k levels
                    return true;
                } else {
                    queue.remove();
                    visited.add(current.id());

                    // bound the BFS to k steps
                    if (current.level() < factorK) {
                        for (K neighbor : adjacencyMap.get(current.id())) {
                            if (!visited.contains(neighbor)) {
                                queue.add(new Node(neighbor, current.level() + 1));
                            }
                        }
                    }
                }
            }
            return false;
        }
    }

    public AdjacencyListGraph<K> merge(AdjacencyListGraph<K> graph) {
        AdjacencyListGraph<K> result = new AdjacencyListGraph<>(this);
        for (K src : graph.adjacencyMap.keySet()) {
            for (K trg : graph.adjacencyMap.get(src)) {
                if (!result.boundedBFS(src, trg)) {
                    // the current distance between src and trg is > k
                    result.addEdge(src, trg);
                }
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AdjacencyListGraph<?> that = (AdjacencyListGraph<?>) o;
        return adjacencyMap.equals(that.adjacencyMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(adjacencyMap);
    }

    @Override
    public String toString() {
        return adjacencyMap.toString();
    }

    public class Node {

        private final K id;
        private final int level;

        Node(K id, int level) {
            this.id = id;
            this.level = level;
        }

        public K id() {
            return id;
        }

        int level() {
            return level;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return level == node.level &&
                id.equals(node.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, level);
        }

        @Override
        public String toString() {
            return "(" + id + "," + level + ")";
        }
    }
}
