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

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.KeyValue;

public class TestGraphUtils {

    public static List<KeyValue<Long, Long>> getLongLongVertices() {
        List<KeyValue<Long, Long>> vertices = new ArrayList<>();
        vertices.add(new KeyValue<>(1L, 1L));
        vertices.add(new KeyValue<>(2L, 2L));
        vertices.add(new KeyValue<>(3L, 3L));
        vertices.add(new KeyValue<>(4L, 4L));
        vertices.add(new KeyValue<>(5L, 5L));

        return vertices;
    }

    public static List<KeyValue<Edge<Long>, Double>> getLCCEdges() {
        List<KeyValue<Edge<Long>, Double>> edges = new ArrayList<>();
        edges.add(new KeyValue<>(new Edge<>(0L, 1L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(0L, 2L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(2L, 1L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(2L, 3L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(3L, 1L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(3L, 4L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(5L, 3L), 1.0));

        return edges;
    }

    public static List<KeyValue<Edge<Long>, Long>> getLongLongEdges() {
        List<KeyValue<Edge<Long>, Long>> edges = new ArrayList<>();
        edges.add(new KeyValue<>(new Edge<>(1L, 2L), 12L));
        edges.add(new KeyValue<>(new Edge<>(1L, 3L), 13L));
        edges.add(new KeyValue<>(new Edge<>(2L, 3L), 23L));
        edges.add(new KeyValue<>(new Edge<>(3L, 4L), 34L));
        edges.add(new KeyValue<>(new Edge<>(3L, 5L), 35L));
        edges.add(new KeyValue<>(new Edge<>(4L, 5L), 45L));
        edges.add(new KeyValue<>(new Edge<>(5L, 1L), 51L));

        return edges;
    }

    public static List<KeyValue<Edge<Long>, Double>> getLongDoubleEdges() {
        List<KeyValue<Edge<Long>, Double>> edges = new ArrayList<>();
        edges.add(new KeyValue<>(new Edge<>(1L, 2L), 12.0));
        edges.add(new KeyValue<>(new Edge<>(1L, 3L), 13.0));
        edges.add(new KeyValue<>(new Edge<>(2L, 3L), 23.0));
        edges.add(new KeyValue<>(new Edge<>(3L, 4L), 34.0));
        edges.add(new KeyValue<>(new Edge<>(3L, 5L), 35.0));
        edges.add(new KeyValue<>(new Edge<>(4L, 5L), 45.0));
        edges.add(new KeyValue<>(new Edge<>(5L, 1L), 51.0));

        return edges;
    }

    public static List<KeyValue<Edge<Long>, Double>> getChain() {
        List<KeyValue<Edge<Long>, Double>> edges = new ArrayList<>();
        edges.add(new KeyValue<>(new Edge<>(0L, 1L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(1L, 2L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(2L, 3L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(3L, 4L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(4L, 5L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(5L, 6L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(6L, 7L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(7L, 8L), 1.0));
        edges.add(new KeyValue<>(new Edge<>(8L, 9L), 1.0));

        return edges;
    }

    public static List<KeyValue<Edge<Long>, Long>> getTwoChains() {
        List<KeyValue<Edge<Long>, Long>> edges = new ArrayList<>();
        edges.add(new KeyValue<>(new Edge<>(0L, 1L), 1L));
        edges.add(new KeyValue<>(new Edge<>(1L, 2L), 1L));
        edges.add(new KeyValue<>(new Edge<>(2L, 3L), 1L));
        edges.add(new KeyValue<>(new Edge<>(3L, 4L), 1L));
        edges.add(new KeyValue<>(new Edge<>(4L, 5L), 1L));
        edges.add(new KeyValue<>(new Edge<>(5L, 6L), 1L));
        edges.add(new KeyValue<>(new Edge<>(6L, 7L), 1L));
        edges.add(new KeyValue<>(new Edge<>(7L, 8L), 1L));
        edges.add(new KeyValue<>(new Edge<>(8L, 9L), 1L));

        edges.add(new KeyValue<>(new Edge<>(10L, 11L), 1L));
        edges.add(new KeyValue<>(new Edge<>(11L, 12L), 1L));
        edges.add(new KeyValue<>(new Edge<>(12L, 13L), 1L));
        edges.add(new KeyValue<>(new Edge<>(13L, 14L), 1L));
        edges.add(new KeyValue<>(new Edge<>(14L, 15L), 1L));
        edges.add(new KeyValue<>(new Edge<>(15L, 16L), 1L));
        edges.add(new KeyValue<>(new Edge<>(16L, 17L), 1L));
        edges.add(new KeyValue<>(new Edge<>(17L, 18L), 1L));
        edges.add(new KeyValue<>(new Edge<>(18L, 19L), 1L));
        edges.add(new KeyValue<>(new Edge<>(19L, 20L), 1L));

        return edges;
    }

    public static List<KeyValue<Edge<Long>, Long>> getTwoCliques(long n) {
        List<KeyValue<Edge<Long>, Long>> edges = new ArrayList<>();
        for (long i = 0; i < n; i++) {
            for (long j = 0; j < n; j++) {
                edges.add(new KeyValue<>(new Edge<>(i, j), 1L));
            }
        }
        for (long i = 0; i < n; i++) {
            for (long j = 0; j < n; j++) {
                edges.add(new KeyValue<>(new Edge<>(i + n, j + n), 1L));
            }
        }
        edges.add(new KeyValue<>(new Edge<>(0L, 5L), 1L));
        return edges;
    }
}
