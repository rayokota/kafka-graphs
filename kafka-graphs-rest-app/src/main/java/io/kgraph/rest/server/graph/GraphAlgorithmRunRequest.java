package io.kgraph.rest.server.graph;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
class GraphAlgorithmRunRequest {
    private int numIterations = Integer.MAX_VALUE;
}
