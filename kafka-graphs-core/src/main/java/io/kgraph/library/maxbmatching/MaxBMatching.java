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
package io.kgraph.library.maxbmatching;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.library.maxbmatching.MBMEdgeValue.State;
import io.kgraph.pregel.ComputeFunction;

/**
 * Greedy algorithm for the Maximum B-Matching problem as described in G. De Francisci Morales, A. Gionis, M. Sozio 
 * "Social Content Matching in MapReduce" in * PVLDB: Proceedings of the VLDB Endowment, 4(7):460-469, 2011.
 *
 * Given a weighted undirected graph, with integer capacities assigned to each vertex, the maximum b-matching problem
 * is to select a subgraph of maximum weight  * such that the number of edges incident to each vertex in the subgraph
 * does not exceed its capacity. This is a greedy algorithm that provides a 1/2-approximation guarantee.
 */
public class MaxBMatching implements ComputeFunction<Long, Integer, MBMEdgeValue, MBMMessage> {
    private static final Logger LOG = Logger.getLogger(MaxBMatching.class);

    @Override
    public void compute(
        int superstep,
        VertexWithValue<Long, Integer> vertex,
        Iterable<MBMMessage> messages,
        Iterable<EdgeWithValue<Long, MBMEdgeValue>> edges,
        Callback<Long, Integer, MBMEdgeValue, MBMMessage> cb
    ) {
        if (LOG.isDebugEnabled()) {
            debug(vertex, edges);
        }
        if (vertex.value() < 0) {
            throw new AssertionError("Capacity should never be negative: " + vertex);
        } else if (vertex.value() == 0) {
            // vote to halt if capacity reaches zero, tell neighbors to remove edges connecting to this vertex
            removeVertex(vertex, edges, cb);
            cb.voteToHalt();
        } else {
            VertexWithValue<Long, Integer> newVertex = vertex;
            if (superstep > 0) {
                // accept intersection between proposed and received
                newVertex = processUpdates(superstep, vertex, messages, edges, cb);
                cb.setNewVertexValue(newVertex.value());
            }
            if (newVertex.value() > 0) {
                // propose edges to neighbors in decreasing order by weight up to capacity
                sendUpdates(newVertex, edges, cb);
            }
        }
    }

    private void sendUpdates(
        VertexWithValue<Long, Integer> vertex,
        Iterable<EdgeWithValue<Long, MBMEdgeValue>> edges,
        Callback<Long, Integer, MBMEdgeValue, MBMMessage> cb
    ) {
        final MBMMessage proposeMsg = new MBMMessage(vertex.id(), State.PROPOSED);

        // get top-capacity available edges by weight
        final int capacity = vertex.value();
        NavigableSet<Entry<Long, MBMEdgeValue>> maxHeap = new TreeSet<>((o1, o2) -> {
            return -1 * Double.compare(o1.getValue().getWeight(), o2.getValue().getWeight()); // reverse comparator, largest weight first
        });

        // prepare list of available edges
        Map<Long, MBMEdgeValue> edgeValues = new HashMap<>();
        for (EdgeWithValue<Long, MBMEdgeValue> e : edges) {
            if (e.value().getState() == State.DEFAULT || e.value().getState() == State.PROPOSED) {
                maxHeap.add(new AbstractMap.SimpleImmutableEntry<>(e.target(), e.value()));
                if (maxHeap.size() > capacity) {
                    maxHeap.pollLast();
                }
            }
            edgeValues.put(e.target(), e.value());
        }

        if (maxHeap.isEmpty()) {
            // all remaining edges are INCLUDED, nothing else to do
            checkSolution(edges);
            cb.voteToHalt();
        } else {
            // propose up to capacity
            while (!maxHeap.isEmpty()) {
                Entry<Long, MBMEdgeValue> entry = maxHeap.pollFirst();
                MBMEdgeValue value = edgeValues.get(entry.getKey());
                cb.setNewEdgeValue(entry.getKey(), new MBMEdgeValue(value.getWeight(), State.PROPOSED));
                cb.sendMessageTo(entry.getKey(), proposeMsg);
            }
        }
    }

    private VertexWithValue<Long, Integer> processUpdates(
        int superstep,
        VertexWithValue<Long, Integer> vertex,
        Iterable<MBMMessage> messages,
        Iterable<EdgeWithValue<Long, MBMEdgeValue>> edges,
        Callback<Long, Integer, MBMEdgeValue, MBMMessage> cb
    ) {
        Set<Long> toRemove = new HashSet<>();
        int numIncluded = 0;

        Map<Long, MBMEdgeValue> edgeValues = new HashMap<>();
        for (EdgeWithValue<Long, MBMEdgeValue> e : edges) {
            edgeValues.put(e.target(), e.value());
        }
        for (MBMMessage msg : messages) {
            MBMEdgeValue edgeValue = edgeValues.get(msg.getId());
            if (edgeValue == null) {
                // edge has already been removed, do nothing
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Superstep %d Vertex %d: message for removed edge from vertex %d", superstep, vertex.id(), msg
                        .getId()));
                }
            } else {
                if (msg.getState() == State.PROPOSED && edgeValue.getState() == State.PROPOSED) {
                    cb.setNewEdgeValue(msg.getId(), new MBMEdgeValue(edgeValue.getWeight(), State.INCLUDED));
                    numIncluded++;
                } else if (msg.getState() == State.REMOVED) {
                    toRemove.add(msg.getId());
                }
            }
        }
        // update capacity
        VertexWithValue<Long, Integer> newVertex = new VertexWithValue<>(vertex.id(), vertex.value() - numIncluded);
        // remove edges locally
        for (Long e : toRemove) {
            cb.removeEdge(e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Superstep %d Vertex %d: included %d edges, removed %d edges", superstep, vertex.id(), numIncluded,
                toRemove.size()
            ));
        }

        return newVertex;
    }

    private void removeVertex(
        VertexWithValue<Long, Integer> vertex,
        Iterable<EdgeWithValue<Long, MBMEdgeValue>> edges,
        Callback<Long, Integer, MBMEdgeValue, MBMMessage> cb
    ) {
        Set<Long> toRemove = new HashSet<>();
        final MBMMessage removeMsg = new MBMMessage(vertex.id(), State.REMOVED);

        for (EdgeWithValue<Long, MBMEdgeValue> e : edges) {
            MBMEdgeValue edgeState = e.value();
            // remove remaining DEFAULT edges
            if (edgeState.getState() == State.DEFAULT) {
                cb.sendMessageTo(e.target(), removeMsg);
                toRemove.add(e.target());
            }
        }
        for (Long e : toRemove) {
            cb.removeEdge(e);
        }
    }

    private void checkSolution(Iterable<EdgeWithValue<Long, MBMEdgeValue>> collection) {
        for (EdgeWithValue<Long, MBMEdgeValue> e : collection) {
            if (e.value().getState() != State.INCLUDED) {
                throw new AssertionError(String.format("All the edges in the matching should be %d, %d was %d", State.INCLUDED, e, e.value().getState()));
            }
        }
    }

    private void debug(
        VertexWithValue<Long, Integer> vertex,
        Iterable<EdgeWithValue<Long, MBMEdgeValue>> edges
    ) {
        LOG.debug(vertex);
        for (EdgeWithValue<Long, MBMEdgeValue> e : edges) {
            LOG.debug(String.format("Edge(%d, %s)", e.target(), e.value().toString()));
        }
    }
}
