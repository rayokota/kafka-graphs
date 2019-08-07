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

import static io.kgraph.streaming.KGraphStream.GLOBAL_KEY;

import java.time.Duration;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

import io.kgraph.Edge;
import io.kgraph.KGraph;
import io.kgraph.utils.KryoSerde;

/**
 * Graph Aggregation on Parallel Time Window
 *
 * @param <K>  the edge stream's key type
 * @param <EV> the edges stream's value type
 * @param <S>  the output type of the partial aggregation
 * @param <T>  the output type of the result
 */
public class SummaryBulkAggregation<K, EV, S, T> extends SummaryAggregation<K, EV, S, T> {

    private final long timeMillis;

    public SummaryBulkAggregation(EdgeFoldFunction<K, EV, S> updateFun,
                                  Reducer<S> combineFun,
                                  ValueMapper<S, T> transformFun,
                                  S initialVal,
                                  long timeMillis,
                                  boolean transientState) {
        super(updateFun, combineFun, transformFun, initialVal, transientState);
        this.timeMillis = timeMillis;
    }

    public SummaryBulkAggregation(EdgeFoldFunction<K, EV, S> updateFun,
                                  Reducer<S> combineFun,
                                  S initialVal,
                                  long timeMillis,
                                  boolean transientState) {
        this(updateFun, combineFun, null, initialVal, timeMillis, transientState);
    }

    @SuppressWarnings("unchecked")
    @Override
    public KTable<Windowed<Short>, T> run(final KStream<Edge<K>, EV> edgeStream) {

        //For parallel window support we key the edge stream by partition and apply a parallel fold per partition.
        //Finally, we merge all locally combined results into our final graph aggregation property.
        KTable<Windowed<Short>, S> partialAgg = edgeStream
            .groupByKey(Grouped.with(new KryoSerde<>(), new KryoSerde<>()))
            .windowedBy(TimeWindows.of(Duration.ofMillis(timeMillis)))
            .aggregate(this::initialValue, new PartialAgg<>(updateFun()))
            .toStream()
            .groupBy((k, v) -> GLOBAL_KEY)
            .windowedBy(TimeWindows.of(Duration.ofMillis(timeMillis)))
            .reduce(combineFun())
            .mapValues(aggregator(edgeStream), Materialized.<Windowed<Short>, S, KeyValueStore<Bytes, byte[]>>
                as(KGraph.generateStoreName()).withKeySerde(new KryoSerde<>()).withValueSerde(new KryoSerde<>()));

        if (transform() != null) {
            return partialAgg.mapValues(
                transform(),
                Materialized.<Windowed<Short>, T, KeyValueStore<Bytes, byte[]>>
                    as(KGraph.generateStoreName()).withKeySerde(new KryoSerde<>()).withValueSerde(new KryoSerde<>())
            );
        }

        return (KTable<Windowed<Short>, T>) partialAgg;
    }

    private static final class PartialAgg<K, EV, S>
        implements Aggregator<Edge<K>, EV, S> {

        private final EdgeFoldFunction<K, EV, S> foldFunction;

        public PartialAgg(EdgeFoldFunction<K, EV, S> foldFunction) {
            this.foldFunction = foldFunction;
        }

        @Override
        public S apply(Edge<K> edge, EV value, S s) {
            return foldFunction.foldEdges(s, edge.source(), edge.target(), value);
        }
    }
}

