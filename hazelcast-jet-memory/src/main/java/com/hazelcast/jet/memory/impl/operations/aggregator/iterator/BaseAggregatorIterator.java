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

package com.hazelcast.jet.memory.impl.operations.aggregator.iterator;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.io.serialization.JetDataInput;
import com.hazelcast.jet.memory.spi.operations.ContainersPull;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.api.binarystorage.BloomFilter;
import com.hazelcast.jet.memory.api.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.spi.operations.BinaryDataFetcher;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.operations.functors.BinaryFunctor;
import com.hazelcast.jet.io.serialization.JetSerializationService;
import com.hazelcast.jet.io.impl.serialization.JetSerializationServiceImpl;
import com.hazelcast.jet.memory.api.operations.aggregator.AggregationIterator;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueDataPartition;
import com.hazelcast.jet.memory.impl.memory.impl.data.KeyValueBinaryDataFetcher;

import java.io.IOException;

public abstract class BaseAggregatorIterator<T> implements AggregationIterator<T> {
    protected boolean hasNextCalled;

    protected boolean lastHasNextResult;

    protected final IOContext ioContext;

    protected final boolean useBigEndian;

    protected final StorageHeader header;

    protected BinaryComparator comparator;

    protected final JetDataInput dataInput;

    protected final BloomFilter bloomFilter;

    protected final ElementsWriter<T> keyWriter;

    protected final ElementsWriter<T> valueWriter;

    protected final BinaryFunctor binaryFunctor;

    protected final MemoryBlock serviceMemoryBlock;

    protected final ContainersPull<T> containersPull;

    protected final MemoryBlock temporaryMemoryBlock;

    protected final KeyValueDataPartition[] partitions;

    protected final BinaryDataFetcher<T> binaryDataFetcher;

    public BaseAggregatorIterator(
            MemoryBlock serviceMemoryBlock,
            MemoryBlock temporaryMemoryBlock,
            BinaryFunctor binaryFunctor,
            ContainersPull<T> containersPull,
            KeyValueDataPartition[] partitions,
            StorageHeader header,
            ElementsWriter<T> keyWriter,
            ElementsWriter<T> valueWriter,
            BloomFilter bloomFilter,
            IOContext ioContext,
            boolean useBigEndian) {
        this.header = header;
        this.keyWriter = keyWriter;
        this.ioContext = ioContext;
        this.valueWriter = valueWriter;
        this.partitions = partitions;
        this.bloomFilter = bloomFilter;
        this.useBigEndian = useBigEndian;
        this.binaryFunctor = binaryFunctor;
        this.containersPull = containersPull;

        this.serviceMemoryBlock = serviceMemoryBlock;
        this.temporaryMemoryBlock = temporaryMemoryBlock;

        JetSerializationService jetSerializationService =
                new JetSerializationServiceImpl();

        this.dataInput = jetSerializationService.createObjectDataInput(
                null,
                useBigEndian
        );

        this.binaryDataFetcher = new KeyValueBinaryDataFetcher<T>(
                ioContext,
                containersPull,
                useBigEndian
        );
    }

    protected void fetchRecord(
            MemoryBlock memoryBlock,
            long recordAddress
    ) {
        try {
            binaryDataFetcher.prepareContainerForNextRecord();
            keyWriter.setSource(binaryDataFetcher.getCurrentContainer());
            valueWriter.setSource(binaryDataFetcher.getCurrentContainer());

            binaryDataFetcher.fetchRecord(
                    memoryBlock,
                    recordAddress,
                    keyWriter,
                    valueWriter
            );
        } catch (IOException e) {
            throw Util.reThrow(e);
        }
    }

    @Override
    public boolean hasNext() {
        if (hasNextCalled) {
            return lastHasNextResult;
        }

        hasNextCalled = true;
        lastHasNextResult = checkHasNext();
        return lastHasNextResult;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new IllegalStateException("Iterator doesn't have next elements");
        }

        try {
            return readNext();
        } finally {
            hasNextCalled = false;
        }
    }

    protected abstract T readNext();

    protected abstract boolean checkHasNext();

    protected boolean checkFunctor() {
        return binaryFunctor != null;
    }

    public void reset(BinaryComparator comparator) {
        this.hasNextCalled = false;
        this.lastHasNextResult = false;
        this.comparator = comparator;
    }
}
