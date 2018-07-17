package io.kgraph.rest.server.graph;

import java.util.Map;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
class GraphAlgorithmCreateRequest {
    private GraphAlgorithmType algorithm;
    private Map<String, String> params;
    private String verticesTopic;
    private String edgesGroupedBySourceTopic;
    private int numPartitions = 50;
    private short replicationFactor = 1;
    private boolean valuesOfTypeDouble = false;
}
