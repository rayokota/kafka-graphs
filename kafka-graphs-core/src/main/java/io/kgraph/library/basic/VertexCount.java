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

package io.kgraph.library.basic;

import java.util.Map;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.ComputeFunction;
import io.kgraph.pregel.aggregators.LongSumAggregator;

public class VertexCount<K, VV, EV, Message> implements ComputeFunction<K, VV, EV, Message> {

    public static final String VERTEX_COUNT_AGGREGATOR = "vertex.count.aggregator";

    @Override
    public void init(Map<String, ?> configs, InitCallback cb) {
        cb.registerAggregator(VERTEX_COUNT_AGGREGATOR, LongSumAggregator.class);
    }

    @Override
    public void compute(
        int superstep,
        VertexWithValue<K, VV> vertex,
        Iterable<Message> messages,
        Iterable<EdgeWithValue<K, EV>> edges,
        Callback<K, VV, EV, Message> cb
    ) {

        if (superstep == 0) {
            cb.aggregate(VERTEX_COUNT_AGGREGATOR, 1L);
        }
    }
}
