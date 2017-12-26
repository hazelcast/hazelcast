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

package com.hazelcast.dataseries.impl;

import com.hazelcast.core.IMap;
import com.hazelcast.dataseries.DataSeries;
import com.hazelcast.dataseries.MemoryInfo;
import com.hazelcast.dataseries.impl.operations.AppendOperation;
import com.hazelcast.dataseries.impl.operations.CountOperationFactory;
import com.hazelcast.dataseries.impl.operations.IteratorOperation;
import com.hazelcast.dataseries.impl.operations.MemoryUsageOperation;
import com.hazelcast.dataseries.impl.operations.MemoryUsageOperationFactory;
import com.hazelcast.dataseries.impl.operations.PopulateOperationFactory;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DataSeriesProxy extends AbstractDistributedObject<DataSeriesService> implements DataSeries {

    private final IPartitionService partitionService;
    private final OperationService operationService;
    private final String name;

    public DataSeriesProxy(String name, NodeEngine nodeEngine, DataSeriesService dataSeriesService) {
        super(nodeEngine, dataSeriesService);
        this.name = name;
        this.partitionService = nodeEngine.getPartitionService();
        this.operationService = nodeEngine.getOperationService();
    }

    @Override
    public byte[] get(int partitionId, long sequence) {
        return getAsync(partitionId, sequence).join();
    }

    @Override
    public InternalCompletableFuture<byte[]> getAsync(int partitionId, long sequenceId) {
        return null;
    }

    //    @Override
//    public void fill(long count, Supplier<V> supplier) {
//        checkNotNull(supplier, "supplier can't be null");
//        checkNotNegative(count, "count can't be smaller than 0");
//
//        int partitionCount = getNodeEngine().getPartitionService().getPartitionCount();
//        long countPerPartition = count / partitionCount;
//        long remaining = count % partitionCount;
//
//        SerializationService ss = getNodeEngine().getSerializationService();
//        List<InternalCompletableFuture> futures = new LinkedList<InternalCompletableFuture>();
//        for (int k = 0; k < partitionCount; k++) {
//            long c = k == partitionCount - 1
//                    ? countPerPartition + remaining
//                    : countPerPartition;
//
//            // we need to clone the supplier
//            Supplier s = ss.toObject(ss.toData(supplier));
//            Operation op = new FillOperation(name, s, c)
//                    .setPartitionId(k);
//            InternalCompletableFuture<Object> f = operationService.invokeOnPartition(op);
//            futures.add(f);
//        }
//
//        for (InternalCompletableFuture f : futures) {
//            f.join();
//        }
//    }

    public Iterator<byte[]> iterator(int partitionId) {
        Operation op = new IteratorOperation(name).setPartitionId(partitionId);
        return (Iterator) operationService.invokeOnPartition(op).join();
    }

//    // needed for Jet integration
//    public Iterator<Map.Entry> shiterator(int partitionId) {
//        Iterator it = iterator(partitionId);
//        Object bogusKey = bogusKeys[partitionId];
//        return new Iterator<Map.Entry>() {
//            @Override
//            public boolean hasNext() {
//                return it.hasNext();
//            }
//
//            @Override
//            public Map.Entry next() {
//                Object value = it.next();
//                return new Map.Entry() {
//                    @Override
//                    public Object getKey() {
//                        return bogusKey;
//                    }
//
//                    @Override
//                    public Object getValue() {
//                        return value;
//                    }
//
//                    @Override
//                    public Object setValue(Object value) {
//                        throw new UnsupportedOperationException();
//                    }
//                };
//            }
//        };
//    }

    public void populate(IMap src) {
        checkNotNull(src, "map can't be null");

        try {
            operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new PopulateOperationFactory(name, src.getName()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long append(int partitionId, byte[] value) {
        return appendAsync(partitionId, value).join();
    }

    @Override
    public InternalCompletableFuture<Long> appendAsync(int partitionId, byte[] value) {
        checkNotNull(value, "value can't be null");

        Operation op = new AppendOperation(name, value)
                .setPartitionId(partitionId);

        return operationService.invokeOnPartition(op);
    }

    @Override
    public long count() {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new CountOperationFactory(name));

            long size = 0;
            for (Object value : result.values()) {
                size += ((Long) value);
            }
            return size;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MemoryInfo memoryInfo() {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new MemoryUsageOperationFactory(name));

            long allocated = 0;
            long consumed = 0;
            long count = 0;
            int segmentsUsed = 0;
            for (Object value : result.values()) {
                MemoryInfo memoryInfo = (MemoryInfo) value;
                allocated += memoryInfo.allocatedBytes();
                consumed += memoryInfo.consumedBytes();
                segmentsUsed += memoryInfo.segmentsInUse();
                count += memoryInfo.count();
            }
            return new MemoryInfo(consumed, allocated, segmentsUsed, count);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MemoryInfo memoryInfo(int partitionId) {
        Operation op = new MemoryUsageOperation(name).setPartitionId(partitionId);
        InternalCompletableFuture<MemoryInfo> f = operationService.invokeOnPartition(op);
        return f.join();
    }

    @Override
    public String getServiceName() {
        return DataSeriesService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }
}
