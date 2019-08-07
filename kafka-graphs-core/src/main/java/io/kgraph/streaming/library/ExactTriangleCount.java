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

package io.kgraph.streaming.library;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

import io.kgraph.Edge;
import io.kgraph.KGraph;
import io.kgraph.streaming.KGraphStream;

/**
 * Single-pass, insertion-only exact Triangle Local and Global Count algorithm.
 * <p>
 * Based on http://www.kdd.org/kdd2016/papers/files/rfp0465-de-stefaniA.pdf.
 */
public class ExactTriangleCount {

    public static KTable<Long, Long> countTriangles(KGraphStream<Long, Void, Void> graph) {
        return graph.buildNeighborhood(false)
                .map(new ExactTriangleCount.ProjectCanonicalEdges())
                .flatMap(new ExactTriangleCount.IntersectNeighborhoods())
                .mapValues(new ExactTriangleCount.SumAndEmitCounters())
                .groupByKey()
                .reduce(Math::max, Materialized.as(KGraph.generateStoreName()));
    }

    // *** Transformation Methods *** //

    /**
     * Receives 2 tuples from the same edge (src + target) and intersects the attached neighborhoods.
     * For each common neighbor, increase local and global counters.
     */
    public static final class IntersectNeighborhoods implements
        KeyValueMapper<Edge<Long>, Set<Long>, Iterable<KeyValue<Long, Long>>> {

        private final Map<Edge<Long>, Set<Long>> neighborhoods = new HashMap<>();

        @Override
        public Iterable<KeyValue<Long, Long>> apply(Edge<Long> key, Set<Long> t) {
            //intersect neighborhoods and emit local and global counters
            List<KeyValue<Long, Long>> result = new ArrayList<>();
            Set<Long> t1 = neighborhoods.remove(key);
            if (t1 != null) {
                // this is the 2nd neighborhood => intersect
                Set<Long> t2 = t;
                long counter = 0;
                if (t1.size() < t2.size()) {
                    // iterate t1 and search t2
                    for (long i : t1) {
                        if (t2.contains(i)) {
                            counter++;
                            result.add(new KeyValue<>(i, 1L));
                        }
                    }
                } else {
                    // iterate t2 and search t1
                    for (long i : t2) {
                        if (t1.contains(i)) {
                            counter++;
                            result.add(new KeyValue<>(i, 1L));
                        }
                    }
                }
                if (counter > 0) {
                    //emit counter for srcID, trgID, and total
                    result.add(new KeyValue<>(key.source(), counter));
                    result.add(new KeyValue<>(key.target(), counter));
                    // -1 signals the total counter
                    result.add(new KeyValue<>(-1L, counter));
                }
            } else {
                // first neighborhood for this edge: store and wait for next
                neighborhoods.put(key, t);
            }
            return result;
        }
    }

    /**
     * Sums up and emits local and global counters.
     */
    public static final class SumAndEmitCounters implements ValueMapperWithKey<Long, Long, Long> {
        private final Map<Long, Long> counts = new HashMap<>();

        @Override
        public Long apply(Long key, Long value) {
            return counts.compute(key, (k, v) -> (v == null) ? value : v + value);
        }
    }

    public static final class ProjectCanonicalEdges implements
        KeyValueMapper<Edge<Long>, Set<Long>, KeyValue<Edge<Long>, Set<Long>>> {
        @Override
        public KeyValue<Edge<Long>, Set<Long>> apply(Edge<Long> key, Set<Long> value) {
            long source = Math.min(key.source(), key.target());
            long trg = Math.max(key.source(), key.target());
            return new KeyValue<>(new Edge<>(source, trg), value);
        }
    }
}
