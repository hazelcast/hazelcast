package com.hazelcast.aggregation;

import com.hazelcast.aggregation.impl.AbstractAggregator;

import java.util.Map;

public class TestDoubleAverageAggregator<K, V> extends AbstractAggregator<Double, K, V> {

    private double sum;
    private int count;

    public TestDoubleAverageAggregator() {
        super();
    }

    public TestDoubleAverageAggregator(String attributePath) {
        super(attributePath);
    }

    @Override
    public void accumulate(Map.Entry<K, V> entry) {
        count++;
        sum += (Double) extract(entry);
    }

    @Override
    public void combine(Aggregator aggregator) {
        TestDoubleAverageAggregator doubleAverageAggregator = (TestDoubleAverageAggregator) aggregator;
        this.sum += doubleAverageAggregator.sum;
        this.count += doubleAverageAggregator.count;
    }

    @Override
    public Double aggregate() {
        if (count == 0) {
            return null;
        }
        return (sum / (double) count);
    }

}
