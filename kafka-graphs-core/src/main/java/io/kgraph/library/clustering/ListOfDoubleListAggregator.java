package io.kgraph.library.clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;


import org.apache.kafka.common.Configurable;

import io.kgraph.pregel.aggregators.Aggregator;

public class ListOfDoubleListAggregator implements Aggregator<List<List<Double>>>, Configurable {

	public static final String CLUSTER_CENTERS_COUNT = "kmeans.cluster.centers.count";
    public static final String POINTS_COUNT = "kmeans.points.count";

	private int k; // the number of the cluster centers
	private int pointsCount; // the number of input points
	private List<List<Double>> value = new ArrayList<>();

	@SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) {
	    Map<String, Object> c = (Map<String, Object>) configs;
        k = (Integer) c.getOrDefault(CLUSTER_CENTERS_COUNT, 0);
        pointsCount = (Integer) c.getOrDefault(POINTS_COUNT, 0);
    }

    @Override
    public List<List<Double>> getAggregate() {
        return value;
    }

    @Override
    public void setAggregate(List<List<Double>> value) {
        this.value = value;
    }

    /**
     * Used to randomly select initial points for k-means
     * If the size of the current list is less than k (#centers)
     * then the element is appended in the list
     * else it replaces an element in a random position
     * with probability k/N, where N is the total number of points
     */
    @Override
    public void aggregate(List<List<Double>> other) {
        for (List<Double> doubles : other) {
            if (getAggregate().size() < k) {
                value.add(doubles);
            } else {
                Random ran = new Random(0);
                int index = ran.nextInt(k);
                if (Math.random() > ((double) k / (double) pointsCount)) {
                    value.set(index, doubles);
                }
            }
        }
    }

    @Override
    public void reset() {
        value = new ArrayList<>();
    }
}
