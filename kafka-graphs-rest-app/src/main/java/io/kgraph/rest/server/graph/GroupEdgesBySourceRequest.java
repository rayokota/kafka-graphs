package io.kgraph.rest.server.graph;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
class GroupEdgesBySourceRequest {
    private String initialVerticesTopic;
    private String initialEdgesTopic;
    private String verticesTopic;
    private String edgesGroupedBySourceTopic;
    private int numPartitions = 50;
    private short replicationFactor = 1;
    private boolean valuesOfTypeDouble = false;
    private boolean async = true;
}
