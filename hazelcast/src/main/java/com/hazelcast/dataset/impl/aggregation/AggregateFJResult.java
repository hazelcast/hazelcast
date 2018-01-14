package com.hazelcast.dataset.impl.aggregation;

import com.hazelcast.aggregation.Aggregator;

import java.util.concurrent.CompletableFuture;

public class AggregateFJResult {
    public final CompletableFuture<Aggregator> future;
    public final Aggregator aggregator;

    public AggregateFJResult(Aggregator aggregator, CompletableFuture<Aggregator> future) {
        this.aggregator = aggregator;
        this.future = future;
    }
}
