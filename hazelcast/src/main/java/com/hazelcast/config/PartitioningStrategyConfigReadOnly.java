package com.hazelcast.config;

import com.hazelcast.core.PartitioningStrategy;

/**
 * @ali 10/11/13
 */
public class PartitioningStrategyConfigReadOnly extends PartitioningStrategyConfig {

    public PartitioningStrategyConfigReadOnly(PartitioningStrategyConfig config) {
        super(config);
    }

    public PartitioningStrategyConfig setPartitioningStrategyClass(String partitionStrategyClass) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public PartitioningStrategyConfig setPartitionStrategy(PartitioningStrategy partitionStrategy) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
