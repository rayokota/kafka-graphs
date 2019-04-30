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

package io.kgraph.utils;

import io.kgraph.EdgeWithValue;
import io.kgraph.VertexWithValue;

public class Parsers<T> {

    public static final class IntegerParser implements Parser<Integer> {
        @Override
        public Integer parse(String s) {
            return Integer.parseInt(s);
        }
    }

    public static final class LongParser implements Parser<Long> {
        @Override
        public Long parse(String s) {
            return Long.parseLong(s);
        }
    }

    public static final class FloatParser implements Parser<Float> {
        @Override
        public Float parse(String s) {
            return Float.parseFloat(s);
        }
    }

    public static final class DoubleParser implements Parser<Double> {
        @Override
        public Double parse(String s) {
            return Double.parseDouble(s);
        }
    }

    public static final class StringParser implements Parser<String> {
        @Override
        public String parse(String s) {
            return s;
        }
    }

    public static class VertexParser<K, V> implements Parser<VertexWithValue<K, V>> {
        private final Parser<K> idParser;
        private final Parser<V> valueParser;
        public VertexParser(Parser<K> idParser, Parser<V> valueParser) {
            this.idParser = idParser;
            this.valueParser = valueParser;
        }
        @Override
        public VertexWithValue<K, V> parse(String s) {
            String[] tokens = s.trim().split("\\s");
            K id = idParser.parse(tokens[0]);
            V value = tokens.length > 1 ? valueParser.parse(tokens[1]) : null;
            return new VertexWithValue<>(id, value);
        }
    }

    public static class EdgeParser<K, V> implements Parser<EdgeWithValue<K, V>> {
        private final Parser<K> sourceIdParser;
        private final Parser<K> targetIdParser;
        private final Parser<V> valueParser;
        public EdgeParser(Parser<K> sourceIdParser, Parser<K> targetIdParser, Parser<V> valueParser) {
            this.sourceIdParser = sourceIdParser;
            this.targetIdParser = targetIdParser;
            this.valueParser = valueParser;
        }
        @Override
        public EdgeWithValue<K, V> parse(String s) {
            String[] tokens = s.trim().split("\\s");
            K sourceId = sourceIdParser.parse(tokens[0]);
            K targetId = targetIdParser.parse(tokens[1]);
            V value = tokens.length > 2 ? valueParser.parse(tokens[2]) : null;
            return new EdgeWithValue<>(sourceId, targetId, value);
        }
    }
}
