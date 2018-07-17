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

package io.kgraph;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.streams.KafkaStreams;

public class GraphAlgorithmState<T> {

    private final KafkaStreams streams;
    private final State state;
    private final long runningTime;
    private final CompletableFuture<T> result;
    private final int superstep;

    public GraphAlgorithmState(KafkaStreams streams, State state, int superstep, long runningTime, CompletableFuture<T> result) {
        this.streams = streams;
        this.state = state;
        this.superstep = superstep;
        this.runningTime = runningTime;
        this.result = result;
    }

    public KafkaStreams streams() {
        return streams;
    }

    public State state() {
        return state;
    }

    public long runningTime() {
        return runningTime;
    }

    public CompletableFuture<T> result() {
        return result;
    }

    public int superstep() {
        return superstep;
    }

    public enum State {
        CREATED(0),
        RUNNING(1),
        COMPLETED(2),
        CANCELLED(3),
        ERROR(4);

        private static final Map<Integer, State> lookup = new HashMap<>();

        static {
            for (State m : EnumSet.allOf(State.class)) {
                lookup.put(m.code(), m);
            }
        }

        private final int code;

        State(int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }

        public static State get(int code) {
            return lookup.get(code);
        }
    }
}
