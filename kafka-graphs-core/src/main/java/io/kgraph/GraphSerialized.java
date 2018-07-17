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
package io.kgraph;

import org.apache.kafka.common.serialization.Serde;

public class GraphSerialized<K, VV, EV> {

    private final Serde<K> keySerde;
    private final Serde<VV> vertexValueSerde;
    private final Serde<EV> edgeValueSerde;

    private GraphSerialized(Serde<K> keySerde,
                            Serde<VV> vertexValueSerde,
                            Serde<EV> edgeValueSerde) {
        this.keySerde = keySerde;
        this.vertexValueSerde = vertexValueSerde;
        this.edgeValueSerde = edgeValueSerde;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<VV> vertexValueSerde() {
        return vertexValueSerde;
    }

    public Serde<EV> edgeValueSerde() {
        return edgeValueSerde;
    }

    protected GraphSerialized(GraphSerialized<K, VV, EV> serialized) {
        this(serialized.keySerde, serialized.vertexValueSerde, serialized.edgeValueSerde);
    }

    public static <K, VV, EV> GraphSerialized<K, VV, EV> with(Serde<K> keySerde,
                                                              Serde<VV> vertexValueSerde,
                                                              Serde<EV> edgeValueSerde) {
        return new GraphSerialized<>(keySerde, vertexValueSerde, edgeValueSerde);
    }
}
