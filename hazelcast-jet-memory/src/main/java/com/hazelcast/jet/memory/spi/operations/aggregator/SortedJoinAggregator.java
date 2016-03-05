package com.hazelcast.jet.memory.spi.operations.aggregator;

import com.hazelcast.jet.memory.spi.operations.aggregator.sorting.SortedAggregator;

public interface SortedJoinAggregator<T> extends JoinAggregator<T>, SortedAggregator<T> {
}
