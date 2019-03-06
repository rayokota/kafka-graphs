/*
 * Copyright 2014 Grafos.ml
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
            aggrValue.addAll(other);
            setAggregate(aggrValue);
        }
        else if ( aggrValue.size() < other.size() ) {
            throw new IndexOutOfBoundsException("The value to be aggregated " +
                "cannot have larger size than the aggregator value");
        }
        else {
            for ( int i = 0; i < other.size(); i ++ ) {
                Double element = aggrValue.get(i) + other.get(i);
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
