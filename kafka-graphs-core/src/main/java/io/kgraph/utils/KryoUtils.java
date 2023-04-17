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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.util.Pool;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyMapSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptySetSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonMapSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonSetSerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KryoUtils {
    private static final Logger log = LoggerFactory.getLogger(KryoUtils.class);

    private static final List<String> SINGLETON_LIST = Collections.singletonList("");
    private static final Set<String> SINGLETON_SET = Collections.singleton("");
    private static final Map<String, String> SINGLETON_MAP = Collections.singletonMap("", "");

    // Pool constructor arguments: thread safe, soft references
    private static final Pool<Kryo> kryoPool = new Pool<>(true, true) {
        protected Kryo create() {
            Kryo kryo = new Kryo();
            kryo.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
            kryo.register(Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer());
            kryo.register(Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer());
            kryo.register(SINGLETON_LIST.getClass(), new CollectionsSingletonListSerializer());
            kryo.register(SINGLETON_SET.getClass(), new CollectionsSingletonSetSerializer());
            kryo.register(SINGLETON_MAP.getClass(), new CollectionsSingletonMapSerializer());
            kryo.setRegistrationRequired(false);

            UnmodifiableCollectionsSerializer.registerSerializers(kryo);
            SynchronizedCollectionsSerializer.registerSerializers(kryo);
            ((DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new
                StdInstantiatorStrategy());
            return kryo;
        }
    };

    public static byte[] serialize(final Object obj) {
        Kryo kryo = kryoPool.obtain();
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            Output output = new Output(stream);
            kryo.writeClassAndObject(output, obj);
            output.close();
            return stream.toByteArray();
        } finally {
            kryoPool.free(kryo);
        }
    }

    @SuppressWarnings("unchecked")
    public static <V> V deserialize(final byte[] objectData) {
        Kryo kryo = kryoPool.obtain();
        try {
            Input input = new Input(objectData);
            return (V) kryo.readClassAndObject(input);
        } finally {
            kryoPool.free(kryo);
        }
    }
}
