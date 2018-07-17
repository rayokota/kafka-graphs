package io.kgraph.rest.server.graph;

import io.kgraph.GraphAlgorithmState;
import io.kgraph.GraphAlgorithmState.State;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
class GraphAlgorithmStatus {

    public GraphAlgorithmStatus(GraphAlgorithmState<?> state) {
        this.state = state.state();
        this.superstep = state.superstep();
        this.runningTime = state.runningTime();
    }

    private State state;
    private int superstep;
    private long runningTime;
}
