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

package io.kgraph.streaming;

import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;

import io.kgraph.EdgeWithValue;

/**
 * A stream of discrete graphs, each maintaining
 * the graph state of the edges contained in the respective window.
 * It is created by calling {@link KGraphStream#slice(Windows)}.
 * The graph slice is keyed by the source or target vertex of the edge stream,
 * so that all edges of a vertex are in the same tumbling window.
 *
 * @param <K>  the vertex ID type
 * @param <EV> the edge value type
 */
public class KGraphWindowedStream<K, EV> {

    private final TimeWindowedKStream<K, EdgeWithValue<K, EV>> windowedStream;

    KGraphWindowedStream(TimeWindowedKStream<K, EdgeWithValue<K, EV>> window) {
        this.windowedStream = window;
    }

    /**
     * Performs a neighborhood fold on the graph window stream.
     *
     * @param initialValue the initial value
     * @param foldFunction the fold function
     * @return the result stream after applying the user-defined fold operation on the window
     */
    public <T> KTable<Windowed<K>, T> foldNeighbors(T initialValue, final EdgeFoldFunction<K, EV, T> foldFunction) {
        return windowedStream.aggregate(() -> initialValue, new ApplyEdgesFoldFunction<K, EV, T>(foldFunction));
    }

    public static final class ApplyEdgesFoldFunction<K, EV, T>
        implements Aggregator<K, EdgeWithValue<K, EV>, T> {

        private final EdgeFoldFunction<K, EV, T> foldFunction;

        public ApplyEdgesFoldFunction(EdgeFoldFunction<K, EV, T> foldFunction) {
            this.foldFunction = foldFunction;
        }

        @Override
        public T apply(K vertex, EdgeWithValue<K, EV> edge, T accumulator) {
            return foldFunction.foldEdges(accumulator, edge.source(), edge.target(), edge.value());
        }
    }

    /**
     * Performs an aggregation on the neighboring edges of each vertex on the graph window stream.
     * <p>
     * For each vertex, the transformation consecutively calls a
     * {@link EdgeReduceFunction} function until only a single value for each edge remains.
     * The {@link EdgeReduceFunction} function combines two edge values into one new value of the same type.
     *
     * @param reduceFunction the aggregation function
     * @return a result stream of Tuple2, containing one tuple per vertex.
     * The first field is the vertex ID and the second field is the final value,
     * after applying the user-defined aggregation operation on the neighborhood.
     */
    public KTable<Windowed<K>, EV> reduceOnEdges(final EdgeReduceFunction<EV> reduceFunction) {
        return windowedStream.reduce(new ApplyEdgesReduceFunction<K, EV>(reduceFunction))
            .mapValues(EdgeWithValue::value);
    }

    public static final class ApplyEdgesReduceFunction<K, EV> implements Reducer<EdgeWithValue<K, EV>> {

        private final EdgeReduceFunction<EV> reduceFunction;

        public ApplyEdgesReduceFunction(EdgeReduceFunction<EV> reduceFunction) {
            this.reduceFunction = reduceFunction;
        }

        @Override
        public EdgeWithValue<K, EV> apply(EdgeWithValue<K, EV> firstEdge, EdgeWithValue<K, EV> secondEdge) {
            EV reducedValue = reduceFunction.reduceEdges(firstEdge.value(), secondEdge.value());
            return new EdgeWithValue<K, EV>(firstEdge.source(), firstEdge.target(), reducedValue);
        }
    }
}
