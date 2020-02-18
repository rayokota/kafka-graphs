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

import org.apache.kafka.streams.kstream.Reducer;

import io.kgraph.streaming.EdgeFoldFunction;
import io.kgraph.streaming.SummaryBulkAggregation;
import io.kgraph.streaming.summaries.AdjacencyListGraph;

/**
 * The Spanner library method continuously computes a k-Spanner of an insertion-only edge stream.
 * The user-defined parameter k defines the distance estimation error,
 * i.e. a k-spanner preserves all distances with a factor of up to k.
 * <p>
 * This is a single-pass implementation, which uses a {@link SummaryBulkAggregation} to periodically merge
 * the partitioned state.
 *
 * @param <K>  the vertex ID type
 * @param <EV> the edge value type
 */
public class Spanner<K extends Comparable<K>, EV> extends SummaryBulkAggregation<K, EV, AdjacencyListGraph<K>, AdjacencyListGraph<K>> {

    /**
     * Creates a Spanner instance.
     *
     * @param mergeWindowTime Window time in millisec for the merger.
     * @param k               the distance error factor
     */
    public Spanner(long mergeWindowTime, int k) {
        super(new UpdateLocal<>(), new CombineSpanners<>(), new AdjacencyListGraph<>(k), mergeWindowTime, false);
    }

    /**
     * Decide to add or remove an edge to the local spanner in the current window.
     * If the current distance between the edge endpoints is &lt;= k then the edge is dropped,
     * otherwise it is added to the local spanner.
     *
     * @param <K> the vertex ID type
     */
    public static final class UpdateLocal<K extends Comparable<K>, EV> implements EdgeFoldFunction<K, EV, AdjacencyListGraph<K>> {

        public UpdateLocal() {
        }

        @Override
        public AdjacencyListGraph<K> foldEdges(AdjacencyListGraph<K> g, K src, K trg, EV value) {
            if (!g.boundedBFS(src, trg)) {
                // the current distance between src and trg is > k
                g = new AdjacencyListGraph<>(g, src, trg);
            }
            return g;
        }
    }

    /**
     * Merge the local spanners of each partition into the global spanner.
     */
    public static class CombineSpanners<K extends Comparable<K>> implements Reducer<AdjacencyListGraph<K>> {

        public CombineSpanners() {
        }

        @Override
        public AdjacencyListGraph<K> apply(AdjacencyListGraph<K> g1, AdjacencyListGraph<K> g2) {
            return g1.size() <= g2.size() ? g2.merge(g1) : g1.merge(g2);
        }
    }
}
