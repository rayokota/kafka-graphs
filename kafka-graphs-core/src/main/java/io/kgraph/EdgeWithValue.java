package io.kgraph;

import java.util.Objects;

public class EdgeWithValue<K, V> {

    private K source;
    private K target;
    private V value;

    public EdgeWithValue() {
    }

    public EdgeWithValue(Edge<K> edge, V value) {
        this(edge.source(), edge.target(), value);
    }

    public EdgeWithValue(K source, K target, V value) {
        this.source = source;
        this.target = target;
        this.value = value;
    }

    public EdgeWithValue<K, V> reverse() {

        return new EdgeWithValue<>(target(), source(), value());
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

    public void setValue(V value) {
        this.value = value;
    }

    public V value() {
        return value;
    }

    public String toString() {
        return "Edge{src=" + source + ",tgt=" + target + ",val=" + value + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EdgeWithValue<?, ?> that = (EdgeWithValue<?, ?>) o;
        return Objects.equals(source, that.source) &&
            Objects.equals(target, that.target) &&
            Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, target, value);
    }
}
