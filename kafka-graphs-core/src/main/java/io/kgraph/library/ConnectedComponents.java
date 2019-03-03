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

package io.kgraph.library;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;
import io.kgraph.pregel.ComputeFunction;

public class ConnectedComponents<EV> implements ComputeFunction<Long, Long, EV, Long> {
    private static final Logger log = LoggerFactory.getLogger(ConnectedComponents.class);

    @Override
    public void compute(
        int superstep,
        VertexWithValue<Long, Long> vertex,
        Iterable<Long> messages,
        Iterable<EdgeWithValue<Long, EV>> edges,
        Callback<Long, Long, EV, Long> cb) {

        Long currentValue = vertex.value();

        for (Long message : messages) {
            currentValue = Math.min(currentValue, message);
        }

        if (currentValue < vertex.value()) {
            log.debug(">>> Vertex {} has new value {}", vertex.id(), currentValue);
            cb.setNewVertexValue(currentValue);
        }

        for (EdgeWithValue<Long, EV> e : edges) {
            if (currentValue < e.target()) {
                log.debug(">>> Vertex {} sent to {} = {}", vertex.id(), e.target(), currentValue);
                cb.sendMessageTo(e.target(), currentValue);
            } else if (currentValue > e.target()) {
                log.debug(">>> Vertex {} sent to {} = {}", vertex.id(), currentValue, e.target());
                cb.sendMessageTo(currentValue, e.target());
            }
        }

        cb.voteToHalt();
    }
}
