package com.hazelcast.dataset.impl;

import com.hazelcast.config.DataSetConfig;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class DataSetContainer {

    private final Partition[] partitions;
    private final DataSetConfig config;
    private final NodeEngineImpl nodeEngine;
    private final Compiler compiler = new Compiler();

    public DataSetContainer(DataSetConfig config, NodeEngineImpl nodeEngine) {
        this.config = config;
        this.nodeEngine = nodeEngine;
        this.partitions = new Partition[nodeEngine.getPartitionService().getPartitionCount()];
    }

    public Partition getPartition(int partitionId) {
        Partition partition = partitions[partitionId];
        if (partition == null) {
            partition = new Partition(config, nodeEngine.getSerializationService(), compiler);
            partitions[partitionId] = partition;
        }
        return partition;
    }
}
