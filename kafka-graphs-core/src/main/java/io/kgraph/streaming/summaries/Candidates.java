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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import io.kgraph.streaming.utils.SignedVertex;

public class Candidates {

    private final boolean success;
    private final Map<Long, Map<Long, SignedVertex>> map;

    public Candidates(boolean success) {
        this.success = success;
        this.map = new TreeMap<>();
    }

    public Candidates(boolean success, long src, long trg) {
        this.success = success;
        this.map = new TreeMap<>();

        add(src, new SignedVertex(src, true));
        add(src, new SignedVertex(trg, false));
    }

    public Candidates(Candidates input) {
        this.success = input.success();
        this.map = new TreeMap<>();

        for (Map.Entry<Long, Map<Long, SignedVertex>> entry : input.map.entrySet()) {
            add(entry.getKey(), entry.getValue());
        }
    }

    public boolean success() {
        return success;
    }

    private boolean add(long component, Map<Long, SignedVertex> vertices) {
        for (SignedVertex vertex : vertices.values()) {
            if (!add(component, vertex)) {
                return false;
            }
        }
        return true;
    }

    private boolean add(long component, SignedVertex vertex) {
        Map<Long, SignedVertex> vertices = map.computeIfAbsent(component, k -> new TreeMap<>());
        SignedVertex storedVertex = vertices.get(vertex.vertex());
        if (storedVertex != null && storedVertex.sign() != vertex.sign()) {
            return false;
        }
        vertices.put(vertex.vertex(), vertex);
        return true;
    }

    public Candidates merge(Candidates input) {
        // Propagate failure
        if (!input.success() || !success()) {
            return fail();
        }

        Candidates result = new Candidates(this);

        // Compare each input component with each candidate component and merge accordingly
        for (Map.Entry<Long, Map<Long, SignedVertex>> inEntry : input.map.entrySet()) {

            List<Long> mergeWith = new ArrayList<>();

            for (Map.Entry<Long, Map<Long, SignedVertex>> selfEntry : result.map.entrySet()) {
                long selfKey = selfEntry.getKey();

                // If the two components are exactly the same, skip them
                if (inEntry.getValue().keySet().containsAll(selfEntry.getValue().keySet())
                    && selfEntry.getValue().keySet().containsAll(inEntry.getValue().keySet())) {
                    continue;
                }

                // Find vertices of input component in the candidate component
                for (long inVertex : inEntry.getValue().keySet()) {
                    if (selfEntry.getValue().containsKey(inVertex)) {
                        if (!mergeWith.contains(selfKey)) {
                            mergeWith.add(selfKey);
                            break;
                        }
                    }
                }
            }

            if (mergeWith.isEmpty()) {
                // If the input component is disjoint from all components of the candidate,
                // simply add that component
                result.add(inEntry.getKey(), inEntry.getValue());
            } else {
                // Merge the input with the lowest id component in candidate
                Collections.sort(mergeWith);
                long firstKey = mergeWith.get(0);
                boolean success;

                success = result.merge(input, inEntry.getKey(), firstKey);
                if (!success) {
                    return fail();
                }

                firstKey = Math.min(inEntry.getKey(), firstKey);

                // Merge other components of candidate into the lowest id component
                for (int i = 1; i < mergeWith.size(); ++i) {

                    success = result.merge(result, mergeWith.get(i), firstKey);
                    if (!success) {
                        return fail();
                    }

                    result.map.remove(mergeWith.get(i));
                }
            }
        }

        return result;
    }

    private boolean merge(Candidates input, long inputKey, long selfKey) {
        Map<Long, SignedVertex> inputComponent = input.map.get(inputKey);
        Map<Long, SignedVertex> selfComponent = map.get(selfKey);

        // Find the vertices to merge along
        List<Long> mergeBy = new ArrayList<>();

        for (long inputVertex : inputComponent.keySet()) {
            if (selfComponent.containsKey(inputVertex)) {
                mergeBy.add(inputVertex);
            }
        }

        // Determine if the merge should be with reversed signs or not
        boolean inputSign = inputComponent.get(mergeBy.get(0)).sign();
        boolean selfSign = selfComponent.get(mergeBy.get(0)).sign();
        boolean reversed = inputSign != selfSign;

        // Evaluate the merge
        boolean success = true;
        for (long mergeVertex : mergeBy) {
            inputSign = inputComponent.get(mergeVertex).sign();
            selfSign = selfComponent.get(mergeVertex).sign();
            if (reversed) {
                success = inputSign != selfSign;
            } else {
                success = inputSign == selfSign;
            }
            if (!success) {
                return false;
            }
        }

        // Execute the merge
        long commonKey = Math.min(inputKey, selfKey);

        // Merge input vertices
        for (SignedVertex inputVertex : inputComponent.values()) {

            if (reversed) {
                success = add(commonKey, inputVertex.reverse());
            } else {
                success = add(commonKey, inputVertex);
            }
            if (!success) {
                return false;
            }
        }

        return true;
    }

    private Candidates fail() {
        return new Candidates(false);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Candidates that = (Candidates) o;
        return success == that.success &&
            map.equals(that.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(success, map);
    }

    @Override
    public String toString() {
        return "(" + success + "," + map + ")";
    }
}
