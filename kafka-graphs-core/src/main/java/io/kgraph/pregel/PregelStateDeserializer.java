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

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import io.kgraph.GraphAlgorithmState;

public class PregelStateDeserializer implements Deserializer<PregelState> {

    public PregelStateDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public PregelState deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        short mode = buf.getShort();
        int superstep = buf.getInt();
        short stage = buf.getShort();
        long startTime = buf.getLong();
        long endTime = buf.getLong();

        return new PregelState(GraphAlgorithmState.State.get(mode), superstep, PregelState.Stage.get(stage),
            startTime, endTime);
    }

    @Override
    public void close() {
    }
}
