/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cardinality.impl;

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.cardinality.impl.operations.AggregateOperation;
import com.hazelcast.cardinality.impl.operations.EstimateOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class CardinalityEstimatorProxy
        extends AbstractDistributedObject<CardinalityEstimatorService>
        implements CardinalityEstimator {

    private final String name;
    private final int partitionId;

    CardinalityEstimatorProxy(String name, NodeEngine nodeEngine, CardinalityEstimatorService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return CardinalityEstimatorService.SERVICE_NAME;
    }

    @Override
    public void add(Object obj) {
        addAsync(obj).join();
    }

    @Override
    public long estimate() {
        return estimateAsync().join();
    }

    @Override
    public InternalCompletableFuture<Void> addAsync(Object obj) {
        checkNotNull(obj, "Object is null.");
        Data data = getNodeEngine().getSerializationService().toData(obj);
        Operation operation = new AggregateOperation(name, data.hash64())
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public InternalCompletableFuture<Long> estimateAsync() {
        Operation operation = new EstimateOperation(name)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public String toString() {
        return "CardinalityEstimator{" + "name='" + name + '\'' + '}';
    }
}
