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

package com.hazelcast.cardinality.impl;

import com.hazelcast.cardinality.impl.operations.AggregateAndEstimateOperation;
import com.hazelcast.cardinality.impl.operations.AggregateOperation;
import com.hazelcast.cardinality.impl.operations.BatchAggregateAndEstimateOperation;
import com.hazelcast.cardinality.impl.operations.BatchAggregateOperation;
import com.hazelcast.cardinality.impl.operations.EstimateOperation;
import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.HashUtil;

import java.nio.charset.Charset;

class CardinalityEstimatorProxy
        extends AbstractDistributedObject<CardinalityEstimatorService>
        implements CardinalityEstimator {

    private final String name;
    private final int partitionId;

    CardinalityEstimatorProxy(String name, NodeEngine nodeEngine, CardinalityEstimatorService service) {
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
    public boolean aggregate(long hash) {
        return aggregateAsync(hash).join();
    }

    @Override
    public boolean aggregateAll(long[] hashes) {
        return aggregateAllAsync(hashes).join();
    }

    @Override
    public long aggregateAndEstimate(long hash) {
        return aggregateAndEstimateAsync(hash).join();
    }

    @Override
    public long aggregateAllAndEstimate(long[] hashes) {
        return aggregateAllAndEstimateAsync(hashes).join();
    }

    @Override
    public boolean aggregateString(String value) {
        return aggregateStringAsync(value).join();
    }

    @Override
    public boolean aggregateAllStrings(String[] values) {
        return aggregateAllStringsAsync(values).join();
    }

    @Override
    public long estimate() {
        return estimateAsync().join();
    }

    @Override
    public InternalCompletableFuture<Boolean> aggregateAsync(long hash) {
        Operation operation = new AggregateOperation(name, hash)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public InternalCompletableFuture<Boolean> aggregateAllAsync(long[] hashes) {
        Operation operation = new BatchAggregateOperation(name, hashes)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public InternalCompletableFuture<Long> aggregateAndEstimateAsync(long hash) {
        Operation operation = new AggregateAndEstimateOperation(name, hash)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public InternalCompletableFuture<Long> aggregateAllAndEstimateAsync(long[] hashes) {
        Operation operation = new BatchAggregateAndEstimateOperation(name, hashes)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public InternalCompletableFuture<Boolean> aggregateStringAsync(String value) {
        byte[] bytes = value.getBytes(Charset.defaultCharset());
        long hash = HashUtil.MurmurHash3_x64_64(bytes, 0, bytes.length);

        return aggregateAsync(hash);
    }

    @Override
    public InternalCompletableFuture<Boolean> aggregateAllStringsAsync(String[] values) {
        long[] hashes = new long[values.length];

        int i = 0;
        for (String value : values) {
            byte[] bytes = value.getBytes(Charset.defaultCharset());
            hashes[i++] = HashUtil.MurmurHash3_x64_64(bytes, 0, bytes.length);
        }

        return aggregateAllAsync(hashes);
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
