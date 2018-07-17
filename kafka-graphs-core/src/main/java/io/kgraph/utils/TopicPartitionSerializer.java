package io.kgraph.utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class TopicPartitionSerializer implements Serializer<TopicPartition> {
    private static final String ENCODING = "UTF8";
    private static final int ARRAY_LENGTH_SIZE = 4;
    private static final int PARTITION_SIZE = 4;

    public TopicPartitionSerializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, TopicPartition data) {
        if (data == null) {
            return null;
        }
        try {
            byte[] topicBytes = data.topic().getBytes(ENCODING);

            ByteBuffer buf = ByteBuffer.allocate(ARRAY_LENGTH_SIZE + topicBytes.length
                + PARTITION_SIZE);
            buf.putInt(topicBytes.length);
            buf.put(topicBytes);
            buf.putInt(data.partition());
            return buf.array();
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing string to byte[]");
        }
    }

    @Override
    public void close() {
    }
}
