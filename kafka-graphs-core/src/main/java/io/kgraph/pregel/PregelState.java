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

package io.kgraph.pregel;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.kgraph.GraphAlgorithmState;
import io.kgraph.GraphAlgorithmState.State;

public class PregelState {

    private final State state;
    private final int superstep;
    private final Stage stage;
    private final long startTime;
    private final long endTime;

    public enum Stage {
        RECEIVE(0),
        SEND(1);

        private static final Map<Integer, Stage> lookup = new HashMap<>();

        static {
            for (Stage s : EnumSet.allOf(Stage.class)) {
                lookup.put(s.code(), s);
            }
        }

        private final int code;

        Stage(int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }

        public static Stage get(int code) {
            return lookup.get(code);
        }
    }

    public PregelState(GraphAlgorithmState.State state, int superstep, Stage stage) {
        this.state = state;
        this.superstep = superstep;
        this.stage = stage;
        this.startTime = state == State.RUNNING ? System.currentTimeMillis() : 0L;
        this.endTime = 0L;
    }

    protected PregelState(GraphAlgorithmState.State state, int superstep, Stage stage,
                          long startTime, long endTime) {
        this.state = state;
        this.superstep = superstep;
        this.stage = stage;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public PregelState next() {
        switch (stage) {
            case RECEIVE:
                return new PregelState(state, superstep, Stage.SEND, startTime, endTime);
            case SEND:
                return new PregelState(state, superstep + 1, Stage.RECEIVE, startTime, endTime);
            default:
                throw new IllegalArgumentException("Invalid stage");
        }
    }

    public PregelState state(State state) {
        return new PregelState(state, superstep, stage, startTime, System.currentTimeMillis());
    }

    public int superstep() {
        return superstep;
    }

    public Stage stage() {
        return stage;
    }

    public GraphAlgorithmState.State state() {
        return state;
    }

    protected long startTime() {
        return startTime;
    }

    protected long endTime() {
        return endTime;
    }

    public long runningTime() {
        switch (state) {
            case CREATED:
                return 0L;
            case RUNNING:
                return System.currentTimeMillis() - startTime;
            default:
                return endTime - startTime;
        }
    }

    public static PregelState fromBytes(byte[] bytes) {
        return new PregelStateDeserializer().deserialize(null, bytes);
    }

    public byte[] toBytes() {
        return new PregelStateSerializer().serialize(null, this);
    }

    @Override
    public String toString() {
        return "Superstep{" +
            "state=" + state +
            ", superstep=" + superstep +
            ", stage=" + stage +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PregelState pregelState = (PregelState) o;
        return superstep == pregelState.superstep &&
            state == pregelState.state &&
            stage == pregelState.stage;
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, superstep, stage);
    }
}
