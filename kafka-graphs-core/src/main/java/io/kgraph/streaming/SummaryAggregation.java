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

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

import io.kgraph.Edge;

/**
 * @param <K>  key type
 * @param <EV> edge value type
 * @param <S>  intermediate state type
 * @param <T>  fold result type
 */
public abstract class SummaryAggregation<K, EV, S, T> {

    /**
     * A function applied to each edge in an edge stream that aggregates a user-defined graph property state. In case
     * we slice the edge stream into windows a fold will output its aggregation state value per window, otherwise, this
     * operates edge-wise
     */
    private final EdgeFoldFunction<K, EV, S> updateFun;

    /**
     * An optional combine function for updating graph property state
     */
    private final Reducer<S> combineFun;

    /**
     * An optional map function that converts state to output
     */
    private final ValueMapper<S, T> transform;

    private final S initialValue;

    /**
     * This flag indicates whether a merger state is cleaned up after an operation
     */
    private final boolean transientState;

    protected SummaryAggregation(EdgeFoldFunction<K, EV, S> updateFun, Reducer<S> combineFun, ValueMapper<S, T> transform, S initialValue, boolean transientState) {
        this.updateFun = updateFun;
        this.combineFun = combineFun;
        this.transform = transform;
        this.initialValue = initialValue;
        this.transientState = transientState;
    }

    public abstract KTable<Windowed<Short>, T> run(KStream<Edge<K>, EV> edgeStream);

    public Reducer<S> combineFun() {
        return combineFun;
    }

    public EdgeFoldFunction<K, EV, S> updateFun() {
        return updateFun;
    }

    public ValueMapper<S, T> transform() {
        return transform;
    }

    public boolean isTransientState() {
        return transientState;
    }

    public S initialValue() {
        return initialValue;
    }

    protected ValueMapper<S, S> aggregator(final KStream<Edge<K>, EV> edgeStream) {
        return new Merger<>(initialValue(), combineFun(), isTransientState());
    }

    /**
     * In this prototype the Merger is non-blocking and merges partitions incrementally
     *
     * @param <S>
     */
    private static final class Merger<S> implements ValueMapper<S, S> {

        private final S initialVal;
        private final Reducer<S> combiner;
        private S summary;
        private final boolean transientState;

        private Merger(S initialVal, Reducer<S> combiner, boolean transientState) {
            this.initialVal = initialVal;
            this.combiner = combiner;
            this.summary = initialVal;
            this.transientState = transientState;
        }

        @Override
        public S apply(S s) {
            if (combiner != null) {
                summary = combiner.apply(s, summary);
                if (transientState) {
                    summary = initialVal;
                }
                return summary;
            } else {
                return s;
            }
        }
    }
}
