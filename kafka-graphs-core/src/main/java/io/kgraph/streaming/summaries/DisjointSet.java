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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DisjointSet<R> {

    private final Map<R, R> matches;
    private final Map<R, Integer> ranks;

    public DisjointSet() {
        this(Collections.emptySet());
    }

    public DisjointSet(Set<R> elements) {
        matches = new HashMap<>();
        ranks = new HashMap<>();
        for (R element : elements) {
            matches.put(element, element);
            ranks.put(element, 0);
        }
    }

    public DisjointSet(DisjointSet<R> elements) {
        matches = new HashMap<>();
        ranks = new HashMap<>();
        for (Map.Entry<R, R> entry : elements.matches.entrySet()) {
            matches.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<R, Integer> entry : elements.ranks.entrySet()) {
            ranks.put(entry.getKey(), entry.getValue());
        }
    }

    public DisjointSet(DisjointSet<R> elements, R e1, R e2) {
        this(elements);
        union(e1, e2);
    }

    protected Map<R, R> matches() {
        return matches;
    }

    public int size() {
        return matches.size();
    }

    /**
     * Creates a new disjoined set solely with e
     *
     * @param e the element
     */
    private void makeSet(R e) {
        matches.put(e, e);
        ranks.put(e, 0);
    }

    /**
     * Find returns the root of the disjoint set e belongs in.
     * It implements path compression, flattening the tree whenever used, attaching nodes directly to the disjoint
     * set root if not already.
     *
     * @param e the element
     * @return the root of the connected component
     */
    protected R find(R e) {
        R parent = matches.get(e);
        if (parent == null) {
            return null;
        }

        if (!parent.equals(e)) {
            R tmp = find(parent);
            if (!parent.equals(tmp)) {
                parent = tmp;
                matches.put(e, parent);
            }
        }
        return parent;
    }

    /**
     * Union combines the two possibly disjoint sets where e1 and e2 belong in.
     * Optimizations:
     * <p/>
     * - In case e1 or e2 do not exist they are being added directly in the same disjoint set.
     * - Union by Rank to minimize lookup depth
     *
     * @param e1 the first element
     * @param e2 the second element
     */
    protected void union(R e1, R e2) {

        if (!matches.containsKey(e1)) {
            makeSet(e1);
        }
        if (!matches.containsKey(e2)) {
            makeSet(e2);
        }

        R root1 = find(e1);
        R root2 = find(e2);

        if (root1.equals(root2)) {
            return;
        }

        int dist1 = ranks.get(root1);
        int dist2 = ranks.get(root2);
        if (dist1 > dist2) {
            matches.put(root2, root1);
        } else if (dist1 < dist2) {
            matches.put(root1, root2);
        } else {
            matches.put(root2, root1);
            ranks.put(root1, dist1 + 1);
        }
    }

    /**
     * Merge works in a similar fashion to a naive symmetric hash join.
     * We keep the current disjoint sets and attach all nodes of 'other' incrementally
     * There is certainly room for further optimisations...
     *
     * @param other the disjoint set to be merged
     */
    public DisjointSet<R> merge(DisjointSet<R> other) {
        DisjointSet<R> result = new DisjointSet<>(this);
        for (Map.Entry<R, R> entry : other.matches.entrySet()) {
            result.union(entry.getKey(), entry.getValue());
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DisjointSet<?> that = (DisjointSet<?>) o;
        return matches.equals(that.matches) &&
            ranks.equals(that.ranks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(matches, ranks);
    }

    @Override
    public String toString() {
        Map<R, List<R>> comps = new HashMap<>();

        for (R vertex : matches.keySet()) {
            R parent = find(vertex);
            List<R> cc = comps.computeIfAbsent(parent, k -> new ArrayList<>());
            cc.add(vertex);
        }
        return comps.toString();
    }
}

