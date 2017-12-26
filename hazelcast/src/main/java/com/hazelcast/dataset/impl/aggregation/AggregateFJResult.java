package com.hazelcast.dataset.impl.aggregation;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.dataset.impl.AggregatorRecursiveTask;

public class AggregateFJResult {
    public final AggregatorRecursiveTask task;
    public final Aggregator aggregator;

    public AggregateFJResult(Aggregator aggregator, AggregatorRecursiveTask task) {
        this.aggregator = aggregator;
        this.task = task;
    }


}
