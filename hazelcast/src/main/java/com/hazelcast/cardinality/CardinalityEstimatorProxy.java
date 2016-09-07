/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cardinality;

import com.hazelcast.cardinality.operations.AggregateAndEstimateOperation;
import com.hazelcast.cardinality.operations.AggregateOperation;
import com.hazelcast.cardinality.operations.BatchAggregateAndEstimateOperation;
import com.hazelcast.cardinality.operations.BatchAggregateOperation;
import com.hazelcast.cardinality.operations.EstimateOperation;
import com.hazelcast.core.ICardinalityEstimator;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.HashUtil;

public class CardinalityEstimatorProxy
        extends AbstractDistributedObject<CardinalityEstimatorService>
        implements ICardinalityEstimator {

    private final String name;
    private final int partitionId;

    public CardinalityEstimatorProxy(String name, NodeEngine nodeEngine, CardinalityEstimatorService service) {
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
        return CardinalityEstimatorService.SERVICE_NAME;
    }

    @Override
    public boolean aggregateHash(long hash) {
        return aggregateHashAsync(hash).join();
    }

    @Override
    public boolean aggregateHashes(long[] hashes) {
        return aggregateHashesAsync(hashes).join();
    }

    @Override
    public long aggregateHashAndEstimate(long hash) {
        return aggregateHashAndEstimateAsync(hash).join();
    }

    @Override
    public long aggregateHashesAndEstimate(long[] hashes) {
        return aggregateHashesAndEstimateAsync(hashes).join();
    }

    @Override
    public boolean aggregateString(String value) {
        return aggregateStringAsync(value).join();
    }

    @Override
    public boolean aggregateStrings(String[] values) {
        return aggregateStringsAsync(values).join();
    }

    @Override
    public long estimate() {
        return estimateAsync().join();
    }

    @Override
    public InternalCompletableFuture<Boolean> aggregateHashAsync(long hash) {
        Operation operation = new AggregateOperation(name, hash)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public InternalCompletableFuture<Boolean> aggregateHashesAsync(long[] hashes) {
        Operation operation = new BatchAggregateOperation(name, hashes)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public InternalCompletableFuture<Long> aggregateHashAndEstimateAsync(long hash) {
        Operation operation = new AggregateAndEstimateOperation(name, hash)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public InternalCompletableFuture<Long> aggregateHashesAndEstimateAsync(long[] hashes) {
        Operation operation = new BatchAggregateAndEstimateOperation(name, hashes)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public InternalCompletableFuture<Boolean> aggregateStringAsync(String value) {
        byte[] bytes = value.getBytes();
        long hash = HashUtil.MurmurHash3_x64_64(bytes, 0, bytes.length);

        return aggregateHashAsync(hash);
    }

    @Override
    public InternalCompletableFuture<Boolean> aggregateStringsAsync(String[] values) {
        long[] hashes = new long[values.length];

        int i = 0;
        for (String value : values) {
            byte[] bytes = value.getBytes();
            hashes[i++] = HashUtil.MurmurHash3_x64_64(bytes, 0, bytes.length);
        }

        return aggregateHashesAsync(hashes);
    }

    @Override
    public InternalCompletableFuture<Long> estimateAsync() {
        Operation operation = new EstimateOperation(name)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public String toString() {
        return "ICardinalityEstimator{" + "name='" + name + '\'' + '}';
    }
}
