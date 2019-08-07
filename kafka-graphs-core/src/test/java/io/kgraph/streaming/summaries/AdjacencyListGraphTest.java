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

package io.kgraph.streaming.summaries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class AdjacencyListGraphTest {

    @Test
    public void testAddEdge() {
        AdjacencyListGraph<Integer> g = new AdjacencyListGraph<>(3);
        g.addEdge(1, 2);
        assertEquals(2, g.adjacencyMap().size());
        assertTrue(g.adjacencyMap().get(1).contains(2));
        assertTrue(g.adjacencyMap().get(2).contains(1));
        assertEquals(1, g.adjacencyMap().get(1).size());
        assertEquals(1, g.adjacencyMap().get(2).size());

        g.addEdge(1, 3);
        assertEquals(3, g.adjacencyMap().size());
        assertTrue(g.adjacencyMap().get(1).contains(2));
        assertTrue(g.adjacencyMap().get(1).contains(3));
        assertTrue(g.adjacencyMap().get(3).contains(1));

        g.addEdge(3, 1);
        assertEquals(3, g.adjacencyMap().size());
        assertEquals(2, g.adjacencyMap().get(1).size());
        assertEquals(1, g.adjacencyMap().get(3).size());

        g.addEdge(1, 2);
        assertEquals(3, g.adjacencyMap().size());
        assertEquals(2, g.adjacencyMap().get(1).size());
        assertEquals(1, g.adjacencyMap().get(2).size());
    }

    @Test
    public void testBoundedBFS() {
        AdjacencyListGraph<Integer> g = new AdjacencyListGraph<>(3);
        g.addEdge(1, 4);
        g.addEdge(4, 5);
        g.addEdge(5, 6);
        g.addEdge(4, 7);
        g.addEdge(7, 8);

        // check edge 2-3 (should be added)
        assertFalse(g.boundedBFS(2, 3));
        g.addEdge(2, 3);

        // check edge 3-4 (should be added)
        assertFalse(g.boundedBFS(3, 4));
        g.addEdge(3, 4);

        // check edge 3-6 (should be dropped)
        assertTrue(g.boundedBFS(3, 6));

        // check edge 8-9 (should be added)
        assertFalse(g.boundedBFS(8, 9));
        g.addEdge(8, 9);

        // check edge 8-6 (should be added)
        assertFalse(g.boundedBFS(8, 6));
        g.addEdge(8, 6);

        // check edge 5-9 (should be dropped)
        assertTrue(g.boundedBFS(5, 9));
    }
}