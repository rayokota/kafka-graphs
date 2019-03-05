package io.kgraph.library.clustering;

import java.util.ArrayList;
import java.util.List;

import io.kgraph.pregel.aggregators.Aggregator;

public class DoubleListAggregator implements Aggregator<List<Double>> {

    private List<Double> value = new ArrayList<>();

    @Override
    public List<Double> getAggregate() {
        return value;
    }

    @Override
    public void setAggregate(List<Double> value) {
        this.value = value;
    }

    @Override
    public void aggregate(List<Double> other) {
        List<Double> aggrValue = getAggregate();
        if ( aggrValue.size() == 0 ) {
            // first-time creation
            for ( int i = 0; i < other.size(); i ++ ) {
                aggrValue.add(other.get(i));
            }
            setAggregate(aggrValue);
        }
        else if ( aggrValue.size() < other.size() ) {
            throw new IndexOutOfBoundsException("The value to be aggregated " +
                "cannot have larger size than the aggregator value");
        }
        else {
            for ( int i = 0; i < other.size(); i ++ ) {
                Double element = new Double(aggrValue.get(i) + other.get(i));
                aggrValue.set(i, element);
            }
            setAggregate(aggrValue);
        }
    }

    @Override
    public void reset() {
        value = new ArrayList<>();
    }
}
