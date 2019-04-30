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

package io.kgraph.tools.importer;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;

import io.kgraph.rest.server.utils.EdgeLongIdLongValueParser;
import io.kgraph.rest.server.utils.VertexLongIdLongValueParser;
import io.kgraph.utils.ClientUtils;
import io.kgraph.utils.GraphUtils;
import io.kgraph.utils.Parsers;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@CommandLine.Command(description = "Imports a Kafka graph.",
    name = "graph-import", mixinStandardHelpOptions = true, version = "graph-import 1.0")
public class GraphImporter<K, VV, EV> implements Callable<Void> {

    @Parameters(index = "0", description = "List of Kafka servers.")
    private String bootstrapServers;

    @Option(names = {"-vt", "--verticesTopic"}, description = "The vertices topic.")
    private String verticesTopic;

    @Option(names = {"-et", "--edgesTopic"}, description = "The edges topic.")
    private String edgesTopic;

    @Option(names = {"-vf", "--verticesFile"}, description = "The vertices file.")
    private File verticesFile;

    @Option(names = {"-ef", "--edgesFile"}, description = "The edges file.")
    private File edgesFile;

    @Option(names = {"-vp", "--vertexParser"}, description = "The vertex parser.")
    private String vertexParser = VertexLongIdLongValueParser.class.getName();

    @Option(names = {"-ep", "--edgeParser"}, description = "The edge parser.")
    private String edgeParser = EdgeLongIdLongValueParser.class.getName();

    @Option(names = {"-k", "--keySerializer"}, description = "The key serializer.")
    private String keySerializer = LongSerializer.class.getName();

    @Option(names = {"-vv", "--vertexValueSerializer"}, description = "The vertex value serializer.")
    private String vertexValueSerializer = LongSerializer.class.getName();

    @Option(names = {"-ev", "--edgeValueSerializer"}, description = "The edge value serializer.")
    private String edgeValueSerializer = LongSerializer.class.getName();

    @Option(names = {"-np", "--numPartitions"}, description = "The number of partitions for topics.")
    private int numPartitions = 50;

    @Option(names = {"-rf", "--replicationFactor"}, description = "The replication factor for topics.")
    private short replicationFactor = 1;

    public GraphImporter() {
    }

    public GraphImporter(String bootstrapServers,
                         String verticesTopic,
                         String edgesTopic,
                         File verticesFile,
                         File edgesFile,
                         String vertexParser,
                         String edgeParser,
                         String keySerializer,
                         String vertexValueSerializer,
                         String edgeValueSerializer,
                         int numPartitions,
                         short replicationFactor) {
        this.bootstrapServers = bootstrapServers;
        this.verticesTopic = verticesTopic;
        this.edgesTopic = edgesTopic;
        this.verticesFile = verticesFile;
        this.edgesFile = edgesFile;
        this.vertexParser = vertexParser;
        this.edgeParser = edgeParser;
        this.keySerializer = keySerializer;
        this.vertexValueSerializer = vertexValueSerializer;
        this.edgeValueSerializer = edgeValueSerializer;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void call() throws Exception {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (verticesFile != null) {
            if (verticesTopic == null) {
                throw new IllegalArgumentException("Missing vertices topic.");
            }
            Parsers.VertexParser<K, VV> vertexReader = (Parsers.VertexParser<K, VV>)
                ClientUtils.getConfiguredInstance(Class.forName(vertexParser), null);
            Serializer<K> keyWriter = (Serializer<K>)
                ClientUtils.getConfiguredInstance(Class.forName(keySerializer), null);
            Serializer<VV> vertexValueWriter = (Serializer<VV>)
                ClientUtils.getConfiguredInstance(Class.forName(vertexValueSerializer), null);
            GraphUtils.<K, VV>verticesToTopic(
                new BufferedInputStream(new FileInputStream(verticesFile)),
                vertexReader, keyWriter, vertexValueWriter,
                props, verticesTopic, numPartitions, replicationFactor
            );
        }
        if (edgesFile != null) {
            if (edgesTopic == null) {
                throw new IllegalArgumentException("Missing edges topic.");
            }
            Parsers.EdgeParser<K, EV> edgeReader = (Parsers.EdgeParser<K, EV>)
                ClientUtils.getConfiguredInstance(Class.forName(edgeParser), null);
            Serializer<EV> edgeValueWriter = (Serializer<EV>)
                ClientUtils.getConfiguredInstance(Class.forName(edgeValueSerializer), null);
            GraphUtils.<K, EV>edgesToTopic(
                new BufferedInputStream(new FileInputStream(edgesFile)),
                edgeReader, edgeValueWriter,
                props, edgesTopic, numPartitions, replicationFactor
            );
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        CommandLine.call(new GraphImporter(), args);
    }
}
