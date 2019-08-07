package io.kgraph.streaming.library;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
import io.kgraph.streaming.summaries.Candidates;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.KryoSerde;
import io.kgraph.utils.StreamUtils;

public class BipartitenessCheckTest extends AbstractIntegrationTest {

    @Test
    public void testBipartite() throws Exception {

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            LongSerializer.class, new Properties()
        );
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Edge<Long>, Void> edges = StreamUtils.streamFromCollection(builder, producerConfig, new KryoSerde<>(),
            new KryoSerde<>(), getBipartiteEdges()
        );
        KGraphStream<Long, Void, Void> graph =
            new EdgeStream<>(edges, GraphSerialized.with(new KryoSerde<>(), new KryoSerde<>(), new KryoSerde<>()));

        KTable<Windowed<Short>, Candidates> candidates = graph.aggregate(new BipartitenessCheck<>(500L));

        startStreams(builder, new KryoSerde<>(), new KryoSerde<>());

        Thread.sleep(10000);

        List<String> result = StreamUtils.listFromTable(streams, candidates).stream()
            .map(kv -> kv.value.toString())
            .collect(Collectors.toList());

        // verify the results
        assertEquals(
            Lists.newArrayList(
                "(true,{1={1=(1,true), 2=(2,false), 3=(3,false), 4=(4,false), 5=(5,true), 7=(7,true), 9=(9,true)}})"),
            result
        );

        streams.close();
    }

    @Test
    public void testNonBipartite() throws Exception {

        Properties producerConfig = ClientUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
            LongSerializer.class, new Properties()
        );
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Edge<Long>, Void> edges = StreamUtils.streamFromCollection(builder, producerConfig, new KryoSerde<>(),
            new KryoSerde<>(), getNonBipartiteEdges()
        );
        KGraphStream<Long, Void, Void> graph =
            new EdgeStream<>(edges, GraphSerialized.with(new KryoSerde<>(), new KryoSerde<>(), new KryoSerde<>()));

        KTable<Windowed<Short>, Candidates> candidates = graph.aggregate(new BipartitenessCheck<>(500L));

        startStreams(builder, new KryoSerde<>(), new KryoSerde<>());

        Thread.sleep(10000);

        List<String> result = StreamUtils.listFromTable(streams, candidates).stream()
            .map(kv -> kv.value.toString())
            .collect(Collectors.toList());

        // verify the results
        assertEquals(
            Lists.newArrayList(
                "(false,{})"),
            result
        );

        streams.close();
    }

    static List<KeyValue<Edge<Long>, Void>> getBipartiteEdges() {
        List<KeyValue<Edge<Long>, Void>> edges = new ArrayList<>();
        edges.add(new KeyValue<>(new Edge<>(1L, 2L), null));
        edges.add(new KeyValue<>(new Edge<>(1L, 3L), null));
        edges.add(new KeyValue<>(new Edge<>(1L, 4L), null));
        edges.add(new KeyValue<>(new Edge<>(4L, 5L), null));
        edges.add(new KeyValue<>(new Edge<>(4L, 7L), null));
        edges.add(new KeyValue<>(new Edge<>(4L, 9L), null));
        return edges;
    }

    static List<KeyValue<Edge<Long>, Void>> getNonBipartiteEdges() {
        List<KeyValue<Edge<Long>, Void>> edges = new ArrayList<>();
        edges.add(new KeyValue<>(new Edge<>(1L, 2L), null));
        edges.add(new KeyValue<>(new Edge<>(2L, 3L), null));
        edges.add(new KeyValue<>(new Edge<>(3L, 1L), null));
        edges.add(new KeyValue<>(new Edge<>(4L, 5L), null));
        edges.add(new KeyValue<>(new Edge<>(5L, 7L), null));
        edges.add(new KeyValue<>(new Edge<>(4L, 1L), null));
        return edges;
    }
}