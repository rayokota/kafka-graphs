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

import org.apache.kafka.common.serialization.Serializer;

public class PregelStateSerializer implements Serializer<PregelState> {
    private static final int MODE_SIZE = 2;
    private static final int SUPERSTEP_SIZE = 4;
    private static final int STAGE_SIZE = 2;
    private static final int TIME_SIZE = 8;

    public PregelStateSerializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, PregelState data) {
        if (data == null) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.allocate(MODE_SIZE + SUPERSTEP_SIZE + STAGE_SIZE + TIME_SIZE + TIME_SIZE);
        buf.putShort((short) data.state().code());
        buf.putInt(data.superstep());
        buf.putShort((short) data.stage().code());
        buf.putLong(data.startTime());
        buf.putLong(data.endTime());
        return buf.array();
    }

    @Override
    public void close() {
    }
}
