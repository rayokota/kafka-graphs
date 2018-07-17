package io.kgraph;

import java.util.Objects;

public class VertexWithValue<K, V> {

    private K id;
    private V value;

    public VertexWithValue() {
    }

    public VertexWithValue(K id, V value) {
        this.id = id;
        this.value = value;
    }

    public K id() {
        return id;
    }

    public V value() {
        return value;
    }

    public void setId(K id) {
        this.id = id;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public String toString() {
        return "Vertex{id=" + id + ",val=" + value + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VertexWithValue<?, ?> that = (VertexWithValue<?, ?>) o;
        return Objects.equals(id, that.id) &&
            Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, value);
    }
}
