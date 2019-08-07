package io.kgraph.streaming.library;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Test;

import io.kgraph.AbstractIntegrationTest;
import io.kgraph.Edge;
import io.kgraph.GraphSerialized;
import io.kgraph.streaming.KGraphStream;
import io.kgraph.streaming.EdgeStream;
import io.kgraph.streaming.summaries.AdjacencyListGraph;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.StreamUtils;

public class SpannerTest extends AbstractIntegrationTest {

    private static final long mergeWindowTime = 1000;
    private static final int k = 3;

    @Test
    public void test() throws Exception {

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            LongSerializer.class, new Properties()
        );
        StreamsBuilder builder = new StreamsBuilder();

        // Use 1 partition for deterministic ordering
        int numPartitions = 1;
        KStream<Edge<Long>, Void> edges = StreamUtils.streamFromCollection(builder, producerConfig,
            "temp-" + UUID.randomUUID(), numPartitions, (short) 1, new KryoSerde<>(), new KryoSerde<>(), getEdges()
        );
        KGraphStream<Long, Void, Void> graph =
            new EdgeStream<>(edges, GraphSerialized.with(new KryoSerde<>(), new KryoSerde<>(), new KryoSerde<>()));

        KTable<Windowed<Short>, AdjacencyListGraph<Long>> sets = graph.aggregate(new Spanner<>(mergeWindowTime, k));

        startStreams(builder, new KryoSerde<>(), new KryoSerde<>());

        Thread.sleep(10000);

        List<String> values = StreamUtils.listFromTable(streams, sets).stream()
            .map(kv -> kv.value.toString())
            .collect(Collectors.toList());

        // This result will vary depending on the number of partitions
        assertEquals(
            "[{1=[4], 2=[3], 3=[2, 4], 4=[1, 3, 5, 7], 5=[4, 6], 6=[5, 8], 7=[4, 8], 8=[6, 7, 9], 9=[8]}]",
            values.toString()
        );

        streams.close();
    }

    static List<KeyValue<Edge<Long>, Void>> getEdges() {
        List<KeyValue<Edge<Long>, Void>> edges = new ArrayList<>();
        edges.add(new KeyValue<>(new Edge<>(1L, 4L), null));
        edges.add(new KeyValue<>(new Edge<>(4L, 7L), null));
        edges.add(new KeyValue<>(new Edge<>(7L, 8L), null));
        edges.add(new KeyValue<>(new Edge<>(4L, 8L), null));
        edges.add(new KeyValue<>(new Edge<>(4L, 5L), null));
        edges.add(new KeyValue<>(new Edge<>(5L, 6L), null));
        edges.add(new KeyValue<>(new Edge<>(2L, 3L), null));
        edges.add(new KeyValue<>(new Edge<>(3L, 4L), null));
        edges.add(new KeyValue<>(new Edge<>(3L, 6L), null));
        edges.add(new KeyValue<>(new Edge<>(8L, 9L), null));
        edges.add(new KeyValue<>(new Edge<>(6L, 8L), null));
        edges.add(new KeyValue<>(new Edge<>(5L, 9L), null));
        return edges;
    }
}

