/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.IFunction;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.operations.AddAllOperation;
import com.hazelcast.ringbuffer.impl.operations.AddOperation;
import com.hazelcast.ringbuffer.impl.operations.GenericOperation;
import com.hazelcast.ringbuffer.impl.operations.ReadManyOperation;
import com.hazelcast.ringbuffer.impl.operations.ReadOneOperation;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

import static com.hazelcast.ringbuffer.OverflowPolicy.OVERWRITE;
import static com.hazelcast.ringbuffer.impl.RingbufferService.SERVICE_NAME;
import static com.hazelcast.ringbuffer.impl.operations.GenericOperation.OPERATION_HEAD;
import static com.hazelcast.ringbuffer.impl.operations.GenericOperation.OPERATION_REMAINING_CAPACITY;
import static com.hazelcast.ringbuffer.impl.operations.GenericOperation.OPERATION_SIZE;
import static com.hazelcast.ringbuffer.impl.operations.GenericOperation.OPERATION_TAIL;
import static com.hazelcast.internal.util.ExceptionUtil.rethrowAllowInterrupted;
import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static java.lang.String.format;

/**
 * The serverside proxy to access a {@link Ringbuffer}.
 *
 * @param <E> the type of the elements in the ringbuffer.
 */
public class RingbufferProxy<E> extends AbstractDistributedObject<RingbufferService> implements Ringbuffer<E> {

    /**
     * The maximum number of items that can be retrieved in 1 go using the
     * {@link #readManyAsync(long, int, int, IFunction)} method.
     */
    public static final int MAX_BATCH_SIZE = 1000;

    private final String name;
    private final int partitionId;
    private final RingbufferConfig config;

    public RingbufferProxy(NodeEngine nodeEngine, RingbufferService service, String name, RingbufferConfig config) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = service.getRingbufferPartitionId(name);
        this.config = config;
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
        return SERVICE_NAME;
    }

    @Override
    public long capacity() {
        getService().ensureNoSplitBrain(name, SplitBrainProtectionOn.READ);
        return config.getCapacity();
    }

    @Override
    public long size() {
        Operation op = new GenericOperation(name, OPERATION_SIZE)
                .setPartitionId(partitionId);
        InvocationFuture<Long> f = invokeOnPartition(op);
        return f.joinInternal();
    }

    @Override
    public long tailSequence() {
        Operation op = new GenericOperation(name, OPERATION_TAIL)
                .setPartitionId(partitionId);
        InvocationFuture<Long> f = invokeOnPartition(op);
        return f.joinInternal();
    }

    @Override
    public long headSequence() {
        Operation op = new GenericOperation(name, OPERATION_HEAD)
                .setPartitionId(partitionId);
        InvocationFuture<Long> f = invokeOnPartition(op);
        return f.joinInternal();
    }

    @Override
    public long remainingCapacity() {
        // we don't need to make a remote call if ttl is not set since in this case the remaining
        // capacity will always be equal to the capacity.
        if (config.getTimeToLiveSeconds() == 0) {
            getService().ensureNoSplitBrain(name, SplitBrainProtectionOn.READ);
            return config.getCapacity();
        }

        Operation op = new GenericOperation(name, OPERATION_REMAINING_CAPACITY)
                .setPartitionId(partitionId);
        InvocationFuture<Long> f = invokeOnPartition(op);
        return f.joinInternal();
    }

    @Override
    public long add(@Nonnull E item) {
        checkNotNull(item, "item can't be null");

        Operation op = new AddOperation(name, toData(item), OVERWRITE)
                .setPartitionId(partitionId);
        InvocationFuture<Long> f = invokeOnPartition(op);
        return f.joinInternal();
    }

    @Override
    public CompletionStage<Long> addAsync(@Nonnull E item, @Nonnull OverflowPolicy overflowPolicy) {
        checkNotNull(item, "item can't be null");
        checkNotNull(overflowPolicy, "overflowPolicy can't be null");

        Operation op = new AddOperation(name, toData(item), overflowPolicy)
                .setPartitionId(partitionId);
        return invokeOnPartition(op);
    }

    @Override
    public E readOne(long sequence) throws InterruptedException {
        checkSequence(sequence);

        Operation op = new ReadOneOperation(name, sequence)
                .setPartitionId(partitionId);
        InvocationFuture<E> f = invokeOnPartition(op);
        try {
            return f.get();
        } catch (Throwable t) {
            throw rethrowAllowInterrupted(t);
        }
    }

    @Override
    public CompletionStage<Long> addAllAsync(@Nonnull Collection<? extends E> collection,
                                                @Nonnull OverflowPolicy overflowPolicy) {
        checkNotNull(collection, "collection can't be null");
        checkNotNull(overflowPolicy, "overflowPolicy can't be null");
        checkFalse(collection.isEmpty(), "collection can't be empty");
        checkTrue(collection.size() <= MAX_BATCH_SIZE, "collection can't be larger than " + MAX_BATCH_SIZE);

        Operation op = new AddAllOperation(name, toDataArray(collection), overflowPolicy)
                .setPartitionId(partitionId);
        OperationService operationService = getOperationService();
        return operationService.createInvocationBuilder(null, op, partitionId)
                               .setCallTimeout(Long.MAX_VALUE)
                               .invoke();
    }

    private Data[] toDataArray(Collection<? extends E> collection) {
        Data[] items = new Data[collection.size()];
        int k = 0;
        for (E item : collection) {
            checkNotNull(item, "collection can't contains null items");
            items[k] = toData(item);
            k++;
        }
        return items;
    }

    @Override
    public CompletionStage<ReadResultSet<E>> readManyAsync(long startSequence, int minCount, int maxCount,
                                                           @Nullable IFunction<E, Boolean> filter) {
        checkSequence(startSequence);
        checkNotNegative(minCount, "minCount can't be smaller than 0");
        checkTrue(maxCount >= minCount, "maxCount should be equal or larger than minCount");
        checkTrue(maxCount <= config.getCapacity(), "the maxCount should be smaller than or equal to the capacity");
        checkTrue(maxCount <= MAX_BATCH_SIZE, "maxCount can't be larger than " + MAX_BATCH_SIZE);

        Operation op = new ReadManyOperation<>(name, startSequence, minCount, maxCount, filter)
                .setPartitionId(partitionId);
        OperationService operationService = getOperationService();
        return operationService.createInvocationBuilder(null, op, partitionId)
                               .setCallTimeout(Long.MAX_VALUE)
                               .invoke();
    }

    private static void checkSequence(long sequence) {
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence can't be smaller than 0, but was: " + sequence);
        }
    }

    @Override
    public String toString() {
        return format("Ringbuffer{name='%s'}", name);
    }
}
