package io.kgraph.library.clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The type of the vertex value in K-means
 * It stores the coordinates of the point
 * and the currently assigned cluster id
 *
 */
public class KMeansVertexValue {
	private final List<Double> pointCoordinates;
	private final int clusterId;
	
	public KMeansVertexValue(List<Double> coordinates, int id) {
		this.pointCoordinates = coordinates;
		this.clusterId = id;
	}

	public KMeansVertexValue() {
		this.pointCoordinates = new ArrayList<>();
		this.clusterId = 0;
	}

	public List<Double> getPointCoordinates() {
		return this.pointCoordinates;
	}
	
	public int getClusterId() {
		return this.clusterId;
	}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KMeansVertexValue that = (KMeansVertexValue) o;
        return clusterId == that.clusterId &&
            Objects.equals(pointCoordinates, that.pointCoordinates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pointCoordinates, clusterId);
    }

    @Override
    public String toString() {
        return String.valueOf(this.clusterId);
    }
}
