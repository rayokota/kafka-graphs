/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kgraph.utils;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kgraph.Edge;

public class TestUtils {
    private static final Logger log = LoggerFactory.getLogger(TestUtils.class);

    public static <T> void compareResultAsTuples(List<T> result, String expected) {
        compareResult(result, expected, true, true);
    }

    public static <T> void compareResultAsText(List<T> result, String expected) {
        compareResult(result, expected,
            false, true);
    }

    public static <T> void compareOrderedResultAsText(List<T> result, String expected) {
        compareResult(result, expected, false, false);
    }

    public static <T> void compareOrderedResultAsText(List<T> result, String expected, boolean asTuples) {
        compareResult(result, expected, asTuples, false);
    }

    private static <T> void compareResult(List<T> result, String expected, boolean asTuples, boolean sort) {
        String[] expectedStrings = expected.split("\n");
        String[] resultStrings = new String[result.size()];

        for (int i = 0; i < resultStrings.length; i++) {
            T val = result.get(i);

            if (asTuples) {
                if (val instanceof KeyValue) {
                    KeyValue t = (KeyValue) val;
                    Object first = t.key;
                    String firstString;
                    if (first instanceof Edge) {
                        Edge<?> edge = (Edge<?>) first;
                        firstString = edge.source() + "," + edge.target();
                    } else if (first != null) {
                        firstString = first.toString();
                    } else {
                        firstString = "null";
                    }
                    Object next = t.value;
                    resultStrings[i] = firstString + ',' + (next == null ? "null" : next.toString());
                } else {
                    throw new IllegalArgumentException(val + " is no tuple");
                }
            } else {
                resultStrings[i] = (val == null) ? "null" : val.toString();
            }
        }

        if (sort) {
            Arrays.sort(expectedStrings);
            Arrays.sort(resultStrings);
        }

        // Include content of both arrays to provide more context in case of a test failure
        String msg = String.format(
            "Different elements in arrays: expected %d elements and received %d\n expected: %s\n received: %s",
            expectedStrings.length, resultStrings.length,
            Arrays.toString(expectedStrings), Arrays.toString(resultStrings));

        assertEquals(msg, expectedStrings.length, resultStrings.length);

        for (int i = 0; i < expectedStrings.length; i++) {
            assertEquals(msg, expectedStrings[i], resultStrings[i]);
        }
    }
}
