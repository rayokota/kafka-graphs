package io.kgraph.utils;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

public class KryoSerializer<T> implements Serializer<T> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, T object) {
        return KryoUtils.serialize(object);
    }

    @Override
    public void close() {
    }
}
