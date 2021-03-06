# Kafka Graphs - Graph Analytics with Kafka

[![Build Status][github-actions-shield]][github-actions-link]
[![Maven][maven-shield]][maven-link]
[![Javadoc][javadoc-shield]][javadoc-link]

[github-actions-shield]: https://github.com/rayokota/kafka-graphs/workflows/build/badge.svg?branch=master
[github-actions-link]: https://github.com/rayokota/kafka-graphs/actions
[maven-shield]: https://img.shields.io/maven-central/v/io.kgraph/kafka-graphs-core.svg
[maven-link]: https://search.maven.org/#search%7Cga%7C1%7Cio.kgraph
[javadoc-shield]: https://javadoc.io/badge/io.kgraph/kafka-graphs-core.svg?color=blue
[javadoc-link]: https://javadoc.io/doc/io.kgraph/kafka-graphs-core

Kafka Graphs is a client layer for distributed processing of graphs with Apache Kafka. 

## Installing

Releases of Kafka Graphs are deployed to Maven Central.

```xml
<dependency>
    <groupId>io.kgraph</groupId>
    <artifactId>kafka-graphs-core</artifactId>
    <version>1.5.0</version>
</dependency>
```
## Creating Graphs

A graph in Kafka Graphs is represented by two tables from Kafka Streams, one for vertices and one for edges. The vertex table is comprised of an ID and a vertex value, while the edge table is comprised of a source ID, target ID, and edge value.

```java
KTable<Long, Long> vertices = ...
KTable<Edge<Long>, Long> edges = ...
KGraph<Long, Long, Long> graph = new KGraph<>(
    vertices, 
    edges, 
    GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long())
);
```

## Transforming Graphs

Kafka Graphs provides a number of APIs for transforming graphs in the same manner as Apache Flink Gelly and Apache Spark GraphX.  

- Filtering methods
  - ``filterOnEdges``
  - ``filterOnVertices``
  - ``subgraph``
- Joining methods
  - ``joinWithEdges``
  - ``joinWithEdgesOnSource``
  - ``joinWithEdgesOnTarget``
  - ``joinWithVertices``
- Mapping methods
  - ``mapEdges``
  - ``mapVertices``
- Reducing methods
  - ``groupReduceOnEdges``
  - ``groupReduceOnNeighbors``
  - ``reduceOnEdges``
  - ``reduceOnNeighbors``
- Transforming methods
  - ``inDegrees``
  - ``outDegrees``
  - ``undirected``

For example, the following will compute the sum of the values of all incoming neighbors for each vertex.

```java
graph.reduceOnNeighbors(new SumValues(), EdgeDirection.IN);
```

## Pregel-Based Graph Algorithms

Kafka Graphs provides a number of graph algorithms based on the vertex-centric approach of Pregel.  The vertex-centric approach allows a computation to "think like a vertex" so that it only need consider how the value of a vertex should change based on messages sent from other vertices.  The following algorithms are provided by Kafka Graphs:

1. **Breadth-first search (BFS)**: given a source vertex, determines the minimum number of hops to reach every other vertex.
2. **Label propagation (LP)**: finds communities in a graph by propagating labels between neighbors.
3. **Local clustering coefficient (LCC)**: computes the degree of clustering for each vertex as determined by the ratio between the number of triangles a vertex closes with its neighbors to the maximum number of triangles it could close.
4. **Multiple-source shortest paths (MSSP)**: given a set of source vertices, finds the shortest paths from these vertices to all other vertices.
5. **PageRank (PR)**: measures the rank or popularity of each vertex by propagating influence between vertices.
6. **Single-source shortest paths (SSSP)**: given a source vertex, finds the shortest paths to all other vertices.
7. **Weakly connected components (WCC)**: determines the weakly connected component for each vertex.

For example, here is the implementation of the single-source shortest paths (SSSP) algorithm:

```java
public final class SSSPComputeFunction 
  implements ComputeFunction<Long, Double, Double, Double> {

  public void compute(
    int superstep,
    VertexWithValue<Long, Double> vertex,
    Map<Long, Double> messages,
    Iterable<EdgeWithValue<Long, Double>> edges,
    Callback<Long, Double, Double> cb) {

    double minDistance = (vertex.id().equals(srcVertexId)) 
      ? 0d : Double.POSITIVE_INFINITY;

    for (Double message : messages.values()) {
      minDistance = Math.min(minDistance, message);
    }

    if (minDistance < vertex.value()) {
      cb.setNewVertexValue(minDistance);
      for (EdgeWithValue<Long, Double> edge : edges) {
        double distance = minDistance + edge.value();
        cb.sendMessageTo(edge.target(), distance);
      }
    }
    
    cb.voteToHalt();
  }
}
```

Custom Pregel-based graph algorithms can also be added by implementing the `ComputeFunction` interface.

## Distributed Graph Processing

Since Kafka Graphs is built on top of Kafka Streams, it is able to leverage the underlying partitioning scheme of Kafka Streams in order to support distributed graph processing.  To facilitate running graph algorithms in a distributed manner, Kafka Graphs provides a REST application for managing graph algorithm executions.

```bash
java -jar kafka-graphs-rest-app-1.3.0.jar \
  --kafka.graphs.bootstrapServers=localhost:9092 \
  --kafka.graphs.zookeeperConnect=localhost:2181
```

When multiple instantiations of the REST application are started on different hosts, all configured with the same Kafka and ZooKeeper servers, they will automatically coordinate with each other to partition the set of vertices when executing a graph algorithm.   When a REST request is sent to one host, it will automatically proxy the request to the other hosts if necessary.

Assuming that tables for the vertices and edges have already created, the following steps can be used to execute a graph algorithm using the REST APIs.

First, the graph is prepared by grouping the edges by the source ID, and also ensuring that the topics for the vertices and edges have the same number of partitions.

```bash
POST /prepare
{
  "algorithm": "sssp",
  "initialVerticesTopic": "vertices", 
  "initialEdgesTopic": "edges", 
  "verticesTopic": "newvertices",
  "edgesGroupedBySourceTopic": "newedges"
}
```

Next, the graph algorithm is configured.

```bash
POST /pregel
{
  "algorithm": "sssp",
  "configs": {
    "srcVertexId": 0
  },
  "verticesTopic": "newvertices",
  "edgesGroupedBySourceTopic": "newedges"
}
```

The above REST request will return an ID for the graph algorithm instantiation.  The graph algorithm is executed as follows.

```bash
POST /pregel/{id}
{
  "numIterations": 30
}
```

While the graph algorithm is running we can inquire its state.

```bash
GET /pregel/{id}
```

When the state is `COMPLETED` we can obtain the result.  The result will be streamed (with content type `text/event-stream`) from all hosts that processed the graph.

```bash
GET /pregel/{id}/result
```

Finally, we clean up resources.

```bash
DELETE /pregel/{id}
```

As you can see above, a graph algorithm may have specific parameters.  Here are the possible algorithms and their associated parameters, if any.

| Algorithm | Parameters | Example |
|-----------|------------|---------|
| bfs  | srcVertexId | "configs": { "srcVertexId": 0 } |
| lcc | | |
| lp | | |
| mssp | landmarkVertexIds | "configs": { "landmarkVertexIds": "0,1,2" } |
| pagerank | tolerance, resetProbability | "configs": { "tolerance": 0.0001, "resetProbability": 0.15 } |
| sssp | srcVertexId | "configs": { "srcVertexId": 0 } |
| wcc | | |

## Graph Streaming

Kafka Graphs also provides an experimental single-pass graph streaming analytics framework based on [Gelly Streaming](https://github.com/vasia/gelly-streaming).  See the [Java APIs](https://javadoc.io/static/io.kgraph/kafka-graphs-core/1.4.0/io/kgraph/streaming/library/package-summary.html).
