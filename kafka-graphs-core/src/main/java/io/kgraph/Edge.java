package io.kgraph;

import java.util.Objects;

public class Edge<K> {

    private K source;
    private K target;

    public Edge() {
    }

    public Edge(K source, K target) {
        this.source = source;
        this.target = target;
    }

    public Edge<K> reverse() {

        return new Edge<>(target(), source());
    }

    public void setSource(K source) {
        this.source = source;
    }

    public K source() {
        return source;
    }

    public void setTarget(K target) {
        this.target = target;
    }

    public K target() {
        return target;
    }

    public String toString() {
        return "Edge{src=" + source + ",tgt=" + target + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Edge<?> edge = (Edge<?>) o;
        return Objects.equals(source, edge.source) &&
            Objects.equals(target, edge.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, target);
    }
}
