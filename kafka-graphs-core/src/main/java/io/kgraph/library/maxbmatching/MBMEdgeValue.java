/*
 * Copyright 2014 Grafos.ml
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kgraph.library.maxbmatching;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MBMEdgeValue {
    private final double weight;
    private final State state;

    public MBMEdgeValue() {
        this(0, State.DEFAULT);
    }

    public MBMEdgeValue(double weight) {
        this(weight, State.DEFAULT);
    }

    public MBMEdgeValue(double weight, State state) {
        this.weight = weight;
        this.state = state;
    }

    public double getWeight() {
        return weight;
    }

    public State getState() {
        return state;
    }

    @Override
    public String toString() {
        return weight + "\t" + state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MBMEdgeValue that = (MBMEdgeValue) o;
        return Double.compare(that.weight, weight) == 0 &&
            state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(weight, state);
    }

    public enum State {
        DEFAULT ((byte) 0), // starting state
        PROPOSED((byte) 1), // proposed for inclusion in the matching
        REMOVED ((byte) 2), // cannot be included in the matching
        INCLUDED((byte) 3); // included in the matching

        private final byte value;
        private static final Map<Byte, State> lookup = new HashMap<>();
        static {
            for (State s : values())
                lookup.put(s.value, s);
        }

        State(byte value) {
            this.value = value;
        }

        public static State fromValue(byte value) {
            State result = lookup.get(value);
            if (result == null)
                throw new IllegalArgumentException("Cannot build edge State from illegal value: " + value);
            return result;
        }

        public byte value() {
            return value;
        }
    }
}
