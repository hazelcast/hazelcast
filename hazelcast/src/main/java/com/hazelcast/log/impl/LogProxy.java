/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.log.impl;

import com.hazelcast.config.LogConfig;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.log.CloseableIterator;
import com.hazelcast.log.Log;
import com.hazelcast.log.impl.operations.ClearOperation;
import com.hazelcast.log.impl.operations.ClearOperationFactory;
import com.hazelcast.log.impl.operations.CountOperationFactory;
import com.hazelcast.log.impl.operations.GetOperation;
import com.hazelcast.log.impl.operations.PutOperation;
import com.hazelcast.log.impl.operations.ReduceOperation;
import com.hazelcast.log.impl.operations.SupplyOperation;
import com.hazelcast.log.impl.operations.SupplyOperationFactory;
import com.hazelcast.log.impl.operations.UsageOperation;
import com.hazelcast.log.impl.operations.UsageOperationFactory;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class LogProxy<E> extends AbstractDistributedObject<LogService> implements Log<E> {

    private final LogConfig config;
    private final int partitionCount;
    private final String name;

    public LogProxy(String name, LogService logService, NodeEngine nodeEngine, LogConfig logConfig) {
        super(nodeEngine, logService);
        this.name = name;
        this.config = logConfig;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();

    }

    private void checkPartitionId(int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId can't be smaller than 0, partitionId=" + partitionId);
        }

        if (partitionId >= partitionCount) {
            throw new IllegalArgumentException("partitionId can't be equal or larger than partitionCount, partitionId="
                    + partitionId + " partitionCount=" + partitionCount);
        }
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void put(E item) {
        asyncPut(item).join();
    }

    @Override
    public CompletableFuture<Void> asyncPut(E item) {
        checkNotNull(item, "item can't be null");

        int partitionId = ThreadLocalRandom.current().nextInt(partitionCount);
        Operation operation = new PutOperation(name, toData(item))
                .setPartitionId(partitionId);
        //todo: the future will contain the sequenceid, but we want to return void.
        return invokeOnPartition(operation);
    }

    @Override
    public E get(int partitionId, long sequence) {
        return asyncGet(partitionId, sequence).join();
    }

    @Override
    public CompletableFuture<E> asyncGet(int partitionId, long sequence) {
        checkPartitionId(partitionId);

        Operation operation = new GetOperation(name, sequence)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public long put(int partitionId, E item) {
        return asyncPut(partitionId, item).join();
    }

    @Override
    public CompletableFuture<Long> asyncPut(int partitionId, E item) {
        checkPartitionId(partitionId);
        checkNotNull(item, "item can't be null");

        Operation operation = new PutOperation(name, toData(item))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Void> supplyAsync(int partitionId, Supplier<E> supplier) {
        checkPartitionId(partitionId);
        checkNotNull(supplier, "supplier can't be null");

        Operation operation = new SupplyOperation(name, supplier)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public void supply(Supplier<E> supplier) {
        checkNotNull(supplier, "supplier can't be null");

        getOperationService()
                .invokeOnAllPartitionsAsync(LogService.SERVICE_NAME, new SupplyOperationFactory(name, supplier));
    }

    @Override
    public Optional<E> reduce(int partitionId, BinaryOperator<E> accumulator) {
        checkPartitionId(partitionId);
        checkNotNull(accumulator, "accumulator can't be null");

        Operation operation = new ReduceOperation(name, accumulator)
                .setPartitionId(partitionId);
        Object result = invokeOnPartition(operation).join();
        return Optional.ofNullable((E) result);
    }

    @Override
    public UsageInfo usage(int partitionId) {
        checkPartitionId(partitionId);

        Operation operation = new UsageOperation(name)
                .setPartitionId(partitionId);
        return (UsageInfo) invokeOnPartition(operation).join();
    }

    @Override
    public UsageInfo usage() {
        try {
            Map<Integer, Object> results = getOperationService()
                    .invokeOnAllPartitions(LogService.SERVICE_NAME, new UsageOperationFactory(name));
            int segments = 0;
            long bytesInUse = 0;
            long bytesAllocated = 0;
            long count = 0;

            for (Object o : results.values()) {
                UsageInfo t = (UsageInfo) o;
                count += t.getCount();
                bytesInUse += t.getBytesInUse();
                bytesAllocated += t.getBytesAllocated();
                segments += t.getSegments();
            }
            return new UsageInfo(segments, bytesInUse, bytesAllocated, count);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long count() {
        try {
            Map<Integer, Object> results = getOperationService()
                    .invokeOnAllPartitions(LogService.SERVICE_NAME, new CountOperationFactory(name));
            long total = 0;
            for (Object o : results.values()) {
                total += (Long) o;
            }
            return total;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clear(int partitionId) {
        checkPartitionId(partitionId);

        Operation operation = new ClearOperation(name)
                .setPartitionId(partitionId);
        invokeOnPartition(operation).join();
    }

    @Override
    public void clear() {
        try {
            getOperationService()
                    .invokeOnAllPartitions(LogService.SERVICE_NAME, new ClearOperationFactory(name));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CloseableIterator<E> localIterator(final int partitionId) {
        checkPartitionId(partitionId);

        OperationService operationService = getOperationService();
        final AtomicReference ref = new AtomicReference();
        operationService.execute(new PartitionSpecificRunnable() {
            @Override
            public int getPartitionId() {
                return partitionId;
            }

            @Override
            public void run() {
                try {
                    IPartitionService partitionService = getNodeEngine().getPartitionService();
                    if (partitionService.getPartition(partitionId).isLocal()) {
                        throw new RuntimeException();
                    }

                    LogContainer container = getService().getContainer(partitionId, name);
                    ref.set(container.iterator());
                } catch (Throwable e) {
                    ref.set(e);
                }
            }
        });

        Object result = ref.get();
        if (result instanceof Throwable) {
            throw new RuntimeException((Throwable) result);
        }
        return (CloseableIterator) ref;
    }
}
