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
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongSerializer;

import io.kgraph.utils.GraphUtils;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@CommandLine.Command(description = "Imports a Kafka graph.",
    name = "graph-import", mixinStandardHelpOptions = true, version = "graph-import 1.0")
public class GraphImporter implements Callable<Void> {

    @Parameters(index = "0", description = "List of Kafka servers.")
    private String bootstrapServers;

    @Parameters(index = "1", description = "The vertices file.")
    private File verticesFile;

    @Parameters(index = "2", description = "The edges file.")
    private File edgesFile;

    @Parameters(index = "3", description = "The vertices topic.")
    private String verticesTopic;

    @Parameters(index = "4", description = "The edges topic.")
    private String edgesTopic;

    @Option(names = {"-d", "--double"}, description = "Whether values are of type double or long.")
    private boolean valuesOfTypeDouble = false;

    @Option(names = {"-np", "--numPartitions"}, description = "The number of partitions for topics.")
    private int numPartitions = 50;

    @Option(names = {"-rf", "--replicationFactor"}, description = "The replication factor for topics.")
    private short replicationFactor = 1;

    public GraphImporter() {
    }

    public GraphImporter(String bootstrapServers,
                         File verticesFile,
                         File edgesFile,
                         String verticesTopic,
                         String edgesTopic,
                         boolean valuesOfTypeDouble,
                         int numPartitions,
                         short replicationFactor) {
        this.bootstrapServers = bootstrapServers;
        this.verticesFile = verticesFile;
        this.edgesFile = edgesFile;
        this.verticesTopic = verticesTopic;
        this.edgesTopic = edgesTopic;
        this.valuesOfTypeDouble = valuesOfTypeDouble;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
    }

    @Override
    public Void call() throws Exception {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (valuesOfTypeDouble) {
            GraphUtils.verticesToTopic(
                new BufferedInputStream(new FileInputStream(verticesFile)),
                Double::parseDouble, new DoubleSerializer(),
                props, verticesTopic, numPartitions, replicationFactor
            );
            GraphUtils.edgesToTopic(
                new BufferedInputStream(new FileInputStream(edgesFile)),
                Double::parseDouble, new DoubleSerializer(),
                props, edgesTopic, numPartitions, replicationFactor
            );
        } else {
            GraphUtils.verticesToTopic(
                new BufferedInputStream(new FileInputStream(verticesFile)),
                Long::parseLong, new LongSerializer(),
                props, verticesTopic, numPartitions, replicationFactor
            );
            GraphUtils.edgesToTopic(
                new BufferedInputStream(new FileInputStream(edgesFile)),
                Long::parseLong, new LongSerializer(),
                props, edgesTopic, numPartitions, replicationFactor
            );
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        CommandLine.call(new GraphImporter(), args);
    }
}
