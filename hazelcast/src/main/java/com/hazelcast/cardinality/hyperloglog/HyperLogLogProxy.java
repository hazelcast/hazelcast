package com.hazelcast.cardinality.hyperloglog;

import com.hazelcast.cardinality.hyperloglog.operations.AddHashOperation;
import com.hazelcast.cardinality.hyperloglog.operations.EstimateCardinalityOperation;
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
    public void addHash(long hash) {
        Operation operation = new AddHashOperation(name, hash)
                .setPartitionId(partitionId);
        invokeOnPartition(operation);
    }

    @Override
    public long estimate() {
        Operation operation = new EstimateCardinalityOperation(name)
                .setPartitionId(partitionId);
        return (Long) invokeOnPartition(operation).join();
    }

    @Override
    public String toString() {
        return "ICardinalityEstimator{" + "name='" + name + '\'' + '}';
    }
}
