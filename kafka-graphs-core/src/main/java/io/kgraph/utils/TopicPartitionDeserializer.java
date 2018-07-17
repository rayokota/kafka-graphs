package io.kgraph.utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class TopicPartitionDeserializer implements Deserializer<TopicPartition> {
    private static final String ENCODING = "UTF8";

    public TopicPartitionDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public TopicPartition deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            ByteBuffer buf = ByteBuffer.wrap(data);
            int topicLength = buf.getInt();
            byte[] topicBytes = new byte[topicLength];
            buf.get(topicBytes);
            String otherTopic = new String(topicBytes, ENCODING);
            int partition = buf.getInt();

            return new TopicPartition(otherTopic, partition);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing byte[] to string");
        }
    }

    @Override
    public void close() {
    }
}
