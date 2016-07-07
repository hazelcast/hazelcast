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

package com.hazelcast.jet.memory.operation.aggregator.cursor;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.impl.serialization.JetSerializationServiceImpl;
import com.hazelcast.jet.io.serialization.JetDataInput;
import com.hazelcast.jet.io.serialization.JetSerializationService;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.memory.Partition;
import com.hazelcast.jet.memory.TupleFetcher;
import com.hazelcast.jet.memory.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;

/**
 * Base class for tuple cursor implementations.
 *
 * @param <K> type of key
 * @param <V> type of value
 */
public abstract class TupleCursorBase<K, V> implements TupleCursor<K, V> {

    protected final IOContext ioContext;
    protected final boolean useBigEndian;
    protected final StorageHeader header;
    protected final JetDataInput dataInput;
    protected final Accumulator accumulator;
    protected final MemoryBlock serviceMemoryBlock;
    protected final Tuple<K, V> destTuple;
    protected final MemoryBlock temporaryMemoryBlock;
    protected final Partition[] partitions;
    protected final TupleFetcher<K, V> tupleFetcher;

    protected Comparator comparator;

    protected TupleCursorBase(
            MemoryBlock serviceMemoryBlock, MemoryBlock temporaryMemoryBlock, Accumulator accumulator,
            Tuple<K, V> destTuple, Partition[] partitions, StorageHeader header, IOContext ioContext,
            boolean useBigEndian
    ) {
        this.header = header;
        this.ioContext = ioContext;
        this.partitions = partitions;
        this.useBigEndian = useBigEndian;
        this.accumulator = accumulator;
        this.destTuple = destTuple;
        this.serviceMemoryBlock = serviceMemoryBlock;
        this.temporaryMemoryBlock = temporaryMemoryBlock;
        JetSerializationService jetSerializationService = new JetSerializationServiceImpl();
        this.dataInput = jetSerializationService.createObjectDataInput(null, useBigEndian);
        this.tupleFetcher = new TupleFetcher<>(ioContext, this.destTuple, useBigEndian);
    }

    @Override
    public Tuple<K, V> asTuple() {
        return tupleFetcher.tuple();
    }

    @Override
    public void reset(Comparator comparator) {
        this.comparator = comparator;
    }
}
