package io.kgraph.streaming.library;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Assert;
import org.junit.Test;

import io.kgraph.AbstractIntegrationTest;
import io.kgraph.Edge;
import io.kgraph.GraphSerialized;
import io.kgraph.streaming.KGraphStream;
import io.kgraph.streaming.EdgeStream;
import io.kgraph.streaming.summaries.DisjointSet;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.StreamUtils;

public class ConnectedComponentsTest extends AbstractIntegrationTest {

    @Test
    public void test() throws Exception {

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            LongSerializer.class, new Properties()
        );
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Edge<Long>, Void> edges = StreamUtils.streamFromCollection(builder, producerConfig, new KryoSerde<>(),
            new KryoSerde<>(), getEdges()
        );
        KGraphStream<Long, Void, Void> graph =
            new EdgeStream<>(edges, GraphSerialized.with(new KryoSerde<>(), new KryoSerde<>(), new KryoSerde<>()));

        KTable<Windowed<Short>, DisjointSet<Long>> sets = graph.aggregate(new ConnectedComponents<>(500L));

        startStreams(builder, new KryoSerde<>(), new KryoSerde<>());

        Thread.sleep(10000);

        List<String> values = StreamUtils.listFromTable(streams, sets).stream()
            .map(kv -> kv.value.toString())
            .collect(Collectors.toList());

        // verify the results
        String expectedResultStr = "1, 2, 3, 5\n" + "6, 7\n" + "8, 9\n";
        String[] result = parser(values);
        String[] expected = expectedResultStr.split("\n");

        assertEquals("Different number of lines in expected and obtained result.", expected.length, result.length);
        Assert.assertArrayEquals("Different connected components.", expected, result);

        streams.close();
    }

    static List<KeyValue<Edge<Long>, Void>> getEdges() {
        List<KeyValue<Edge<Long>, Void>> edges = new ArrayList<>();
        edges.add(new KeyValue<>(new Edge<>(1L, 2L), null));
        edges.add(new KeyValue<>(new Edge<>(1L, 3L), null));
        edges.add(new KeyValue<>(new Edge<>(2L, 3L), null));
        edges.add(new KeyValue<>(new Edge<>(1L, 5L), null));
        edges.add(new KeyValue<>(new Edge<>(6L, 7L), null));
        edges.add(new KeyValue<>(new Edge<>(8L, 9L), null));
        return edges;
    }

    static String[] parser(List<String> list) {
        int s = list.size();
        String r = list.get(s - 1);  // to get the final combine result which is stored at the end of result
        String t;
        list.clear();
        String[] G = r.split("=");
        for (String value : G) {
            if (value.contains("[")) {
                String[] k = value.split("]");
                t = k[0].substring(1);
                list.add(t);
            }
        }
        String[] result = list.toArray(new String[0]);
        Arrays.sort(result);
        return result;
    }
}

