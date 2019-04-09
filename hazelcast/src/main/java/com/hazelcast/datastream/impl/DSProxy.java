/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl;

import com.hazelcast.core.IMap;
import com.hazelcast.datastream.DataFrame;
import com.hazelcast.datastream.DataStream;
import com.hazelcast.datastream.DataStreamPublisher;
import com.hazelcast.datastream.DataStreamSubscriber;
import com.hazelcast.datastream.impl.operations.FillOperation;
import com.hazelcast.datastream.impl.operations.HeadOperation;
import com.hazelcast.datastream.impl.operations.IteratorOperation;
import com.hazelcast.datastream.impl.operations.PopulateOperationFactory;
import com.hazelcast.datastream.impl.operations.TailOperation;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.function.Supplier;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class DSProxy<E> extends AbstractDistributedObject<DSService> implements DataStream<E> {

    protected final IPartitionService partitionService;
    protected final OperationService operationService;
    private final String name;
    private final InternalSerializationService serializationService;
    private final DataFrameImpl<E> frame;

    public DSProxy(String name, NodeEngine nodeEngine, DSService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionService = nodeEngine.getPartitionService();
        this.operationService = nodeEngine.getOperationService();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.frame = new DataFrameImpl<>(name, operationService, nodeEngine, service);
    }

    @Override
    public long tail(String partitionKey) {
        int partitionId = partitionService.getPartitionId(partitionKey);
        InternalCompletableFuture<Object> future = operationService
                .invokeOnPartition(new TailOperation(name).setPartitionId(partitionId));
        return (Long) future.join();
    }

    @Override
    public long head(String partitionKey) {
        int partitionId = partitionService.getPartitionId(partitionKey);
        InternalCompletableFuture<Object> future = operationService
                .invokeOnPartition(new HeadOperation(name).setPartitionId(partitionId));
        return (Long) future.join();
    }

    @Override
    public void fill(long count, Supplier<E> supplier) {
        checkNotNull(supplier, "supplier can't be null");
        checkNotNegative(count, "count can't be smaller than 0");

        int partitionCount = getNodeEngine().getPartitionService().getPartitionCount();
        long countPerPartition = count / partitionCount;
        long remaining = count % partitionCount;

        SerializationService ss = getNodeEngine().getSerializationService();
        List<InternalCompletableFuture> futures = new LinkedList<>();
        for (int k = 0; k < partitionCount; k++) {
            long c = k == partitionCount - 1
                    ? countPerPartition + remaining
                    : countPerPartition;

            // we need to clone the supplier
            Supplier s = ss.toObject(ss.toData(supplier));
            Operation op = new FillOperation(name, s, c)
                    .setPartitionId(k);
            InternalCompletableFuture<Object> f = operationService.invokeOnPartition(op);
            futures.add(f);
        }

        for (InternalCompletableFuture f : futures) {
            f.join();
        }
    }

    public Iterator<E> iterator(int partitionId) {
        Operation op = new IteratorOperation(name).setPartitionId(partitionId);
        return (Iterator) operationService.invokeOnPartition(op).join();
    }

    public void populate(IMap src) {
        checkNotNull(src, "map can't be null");

        try {
            operationService.invokeOnAllPartitions(
                    DSService.SERVICE_NAME, new PopulateOperationFactory(name, src.getName()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataStreamPublisher<E> createPublisher() {
        return new DSPublisherImpl<>(serializationService, operationService, partitionService, name);
    }

    @Override
    public DataStreamSubscriber<E> createSubscriber() {
        return new DSSubscriberImpl<>(serializationService, getService(), name);
    }

    @Override
    public DataFrame<E> asFrame() {
        return frame;
    }

    @Override
    public String getServiceName() {
        return DSService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }
}
