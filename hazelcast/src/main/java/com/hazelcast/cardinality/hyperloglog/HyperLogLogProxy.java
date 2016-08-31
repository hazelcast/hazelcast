package com.hazelcast.cardinality.hyperloglog;

import com.hazelcast.cardinality.hyperloglog.operations.AddHashOperation;
import com.hazelcast.core.ICardinalityEstimator;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

public class HyperLogLogProxy extends AbstractDistributedObject<HyperLogLogService>
        implements ICardinalityEstimator {

    private final String name;
    private final int partitionId;

    public HyperLogLogProxy(String name, NodeEngine nodeEngine, HyperLogLogService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return HyperLogLogService.SERVICE_NAME;
    }

    @Override
    public void addString(CharSequence value) {
        Operation operation = new AddHashOperation(name, value.hashCode())
                .setPartitionId(partitionId);
        invokeOnPartition(operation);
    }

    @Override
    public String toString() {
        return "ICardinalityEstimator{" + "name='" + name + '\'' + '}';
    }
}
