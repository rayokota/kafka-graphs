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
