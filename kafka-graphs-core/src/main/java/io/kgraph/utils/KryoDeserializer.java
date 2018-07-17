package io.kgraph.utils;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class KryoDeserializer<T> implements Deserializer<T> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        return KryoUtils.deserialize(bytes);
    }

    @Override
    public void close() {
    }
}
