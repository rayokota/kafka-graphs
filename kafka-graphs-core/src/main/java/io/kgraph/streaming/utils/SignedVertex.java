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

package io.kgraph.streaming.utils;

import java.util.Objects;

public class SignedVertex {

    private final long vertex;
    private final boolean sign;

    public SignedVertex(long vertex, boolean sign) {
        this.vertex = vertex;
        this.sign = sign;
    }

    public long vertex() {
        return vertex;
    }

    public boolean sign() {
        return sign;
    }

    public SignedVertex reverse() {
        return new SignedVertex(vertex(), !sign());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SignedVertex that = (SignedVertex) o;
        return vertex == that.vertex &&
            sign == that.sign;
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertex, sign);
    }

    @Override
    public String toString() {
        return "(" + vertex + "," + sign + ")";
    }
}