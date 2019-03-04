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

import java.util.Objects;

public class EdgeWithValue<K, V> {

    private K source;
    private K target;
    private V value;

    public EdgeWithValue() {
    }

    public EdgeWithValue(Edge<K> edge, V value) {
        this(edge.source(), edge.target(), value);
    }

    public EdgeWithValue(K source, K target, V value) {
        this.source = source;
        this.target = target;
        this.value = value;
    }

    public EdgeWithValue<K, V> reverse() {

        return new EdgeWithValue<>(target(), source(), value());
    }

    public K source() {
        return source;
    }

    public K target() {
        return target;
    }

    public V value() {
        return value;
    }

    public String toString() {
        return "Edge{src=" + source + ",tgt=" + target + ",val=" + value + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EdgeWithValue<?, ?> that = (EdgeWithValue<?, ?>) o;
        return Objects.equals(source, that.source) &&
            Objects.equals(target, that.target) &&
            Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, target, value);
    }
}
