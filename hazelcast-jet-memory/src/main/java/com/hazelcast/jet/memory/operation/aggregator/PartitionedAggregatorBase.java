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

package com.hazelcast.jet.memory.operation.aggregator;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.impl.serialization.JetSerializationServiceImpl;
import com.hazelcast.jet.io.serialization.JetDataOutput;
import com.hazelcast.jet.io.serialization.JetSerializationService;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.memory.JetMemoryException;
import com.hazelcast.jet.memory.JetOutOfMemoryException;
import com.hazelcast.jet.memory.Partition;
import com.hazelcast.jet.memory.binarystorage.ObjectHolder;
import com.hazelcast.jet.memory.binarystorage.Storage;
import com.hazelcast.jet.memory.binarystorage.StorageHeader;
import com.hazelcast.jet.memory.binarystorage.accumulator.Accumulator;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.memoryblock.DefaultMemoryBlockChain;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.memoryblock.MemoryBlockChain;
import com.hazelcast.jet.memory.memoryblock.MemoryChainingRule;
import com.hazelcast.jet.memory.memoryblock.MemoryContext;
import com.hazelcast.jet.memory.operation.aggregator.cursor.TupleCursor;
import com.hazelcast.jet.memory.spilling.SpillFileCursor;
import com.hazelcast.jet.memory.spilling.Spiller;
import com.hazelcast.jet.memory.util.JetIoUtil;
import com.hazelcast.jet.memory.util.Util;
import com.hazelcast.nio.IOUtil;

import java.io.File;
import java.util.function.LongConsumer;

import static com.hazelcast.jet.memory.multimap.TupleMultimapHsa.FIRST_FOOTER_SIZE_BYTES;
import static com.hazelcast.jet.memory.operation.aggregator.AggregatorState.AGGREGATING;
import static com.hazelcast.jet.memory.operation.aggregator.AggregatorState.SPILLING;
import static com.hazelcast.jet.memory.util.JetIoUtil.addrOfValueBlockAt;
import static com.hazelcast.jet.memory.util.JetIoUtil.addressOfKeyBlockAt;
import static com.hazelcast.jet.memory.util.JetIoUtil.sizeOfKeyBlockAt;
import static com.hazelcast.jet.memory.util.JetIoUtil.sizeOfValueBlockAt;

/**
 * Base class for partitioned aggregators.
 */
public abstract class PartitionedAggregatorBase implements Aggregator {
    protected final boolean useBigEndian;
    protected final int spillingBufferSize;
    protected final int spillingChunkSize;
    protected final File spillingDirectory;
    protected final MemoryContext memoryContext;
    protected final Accumulator accumulator;
    protected final MemoryChainingRule memoryChainingRule;
    protected final IOContext ioContext;
    protected final Comparator defaultComparator;
    protected final ObjectHolder<Comparator> comparatorHolder = new ObjectHolder<>();
    protected final MemoryBlock serviceMemoryBlock;
    protected final MemoryBlock temporaryMemoryBlock;
    protected final Partition[] partitions;
    protected final StorageHeader header;
    protected final LongConsumer hsaResizeListener;
    protected final Tuple2 destTuple;
    protected SpillFileCursor spillFileCursor;

    private final boolean spillToDisk;
    private final int partitionHashMask;
    private final JetDataOutput jetDataOutput;
    private Partition currentPartition;
    private AggregatorState state = initialState();

    @SuppressWarnings({"checkstyle:parameternumber", "checkstyle:executablestatementcount"})
    protected PartitionedAggregatorBase(
            int partitionCount, int spillingBufferSize, IOContext ioContext, Comparator comparator,
            MemoryContext memoryContext, MemoryChainingRule memoryChainingRule, Tuple2 destTuple,
            Accumulator accumulator, String spillPathname, int spillingChunkSize, boolean spillToDisk,
            boolean useBigEndian
    ) {
        this.spillingDirectory = new File(spillPathname);
        assert Util.isPositivePowerOfTwo(partitionCount) : "Partition count must be a power of two";
        assert !spillToDisk
                || spillingDirectory.exists() && spillingDirectory.isDirectory() && spillingDirectory.canWrite()
                : "Invalid spill directory " + spillingDirectory.getAbsolutePath();
        this.ioContext = ioContext;
        this.spillToDisk = spillToDisk;
        this.useBigEndian = useBigEndian;
        this.accumulator = accumulator;
        this.memoryContext = memoryContext;
        this.destTuple = destTuple;
        this.memoryChainingRule = memoryChainingRule;
        this.defaultComparator = comparator;
        this.spillingChunkSize = spillingChunkSize;
        this.partitionHashMask = partitionCount - 1;
        this.spillingBufferSize = spillingBufferSize;
        this.header = new StorageHeader();
        this.hsaResizeListener = header::setBaseStorageAddress;
        this.partitions = new Partition[partitionCount];
        final JetSerializationService serializationService = new JetSerializationServiceImpl();
        this.jetDataOutput = serializationService.createObjectDataOutput(null, useBigEndian);
        initPartitions(partitionCount);
        MemoryBlockChain memoryBlockChain = new DefaultMemoryBlockChain(memoryContext, false, memoryChainingRule);
        memoryBlockChain.acquireNext(false);
        this.serviceMemoryBlock = memoryBlockChain.get(0);
        memoryBlockChain.acquireNext(false);
        this.temporaryMemoryBlock = memoryBlockChain.get(1);
    }

    @Override
    public void dispose() {
        try {
            for (Partition partition : partitions) {
                partition.getMemoryBlockChain().dispose();
            }
        } finally {
            disposeSpiller();
        }
    }

    @Override
    public void setComparator(Comparator comparator) {
        comparatorHolder.set(comparator);
    }

    @Override
    public void startSpilling() {
        if (!spillToDisk) {
            dispose();
            throw new JetOutOfMemoryException("RAM exhausted and spill-over disabled");
        }
        onSpillingStarted();
    }

    @Override
    public boolean spillNextChunk() {
        if (state != SPILLING) {
            return true;
        }
        boolean spillingDone = !spiller().processNextChunk();
        if (spillingDone) {
            state = AGGREGATING;
        }
        return spillingDone;
    }

    @Override
    public void finishSpilling() {
        onSpillingFinished();
        state = AGGREGATING;
    }

    @Override
    public boolean accept(Tuple2 tuple) {
        try {
            writeTuple(tuple, getComparator());
        } catch (JetOutOfMemoryException exception) {
            MemoryBlockChain chain = currentPartition.getMemoryBlockChain();
            if (!chain.gotoNext() && !acquireNextBlock(chain)) {
                return false;
            }
            activateMemoryBlock(currentPartition, chain.current());
            writeTuple(tuple, getComparator());
        }
        return true;
    }

    protected void activatePartitions() {
        for (Partition partition : partitions) {
            MemoryBlock memoryBlock = partition.getMemoryBlockChain().current();
            activateMemoryBlock(partition, memoryBlock);
        }
    }

    private void activateMemoryBlock(Partition partition, MemoryBlock memoryBlock) {
        memoryBlock.reset(true);
        partition.getStorage().setMemoryBlock(memoryBlock);
        header.setMemoryBlock(memoryBlock);
        header.allocatedHeader();
        long baseAddress = partition.getStorage().gotoNew();
        header.setBaseStorageAddress(baseAddress);
    }

    protected AggregatorState initialState() {
        return AGGREGATING;
    }

    protected void onSpillingStarted() {
        state = SPILLING;
        spiller().start(partitions);
    }

    protected Comparator getComparator() {
        return comparatorHolder.get(defaultComparator);
    }

    protected void onSpillingFinished() {
        spiller().stop();
        resetBlocks();
        activatePartitions();
    }

    protected abstract Partition newPartition(int partitionID);

    protected abstract Spiller spiller();

    protected abstract TupleCursor newResultCursor();

    private void resetBlocks() {
        for (Partition partition : partitions) {
            for (int i = 0; i < partition.getMemoryBlockChain().size(); i++) {
                MemoryBlock memoryBlock = partition.getMemoryBlockChain().get(i);
                memoryBlock.reset(true);
                header.setMemoryBlock(memoryBlock);
                header.resetHeader();
            }
            partition.getMemoryBlockChain().setCurrent(0);
        }
    }

    private void initPartitions(int partitionCount) {
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = newPartition(i);
        }
    }

    private void disposeSpiller() {
        try {
            spiller().dispose();
        } finally {
            IOUtil.delete(spillingDirectory);
        }
    }


    private boolean acquireNextBlock(MemoryBlockChain memoryBlockChain) {
        try {
            memoryBlockChain.acquireNext(true);
        } catch (JetOutOfMemoryException e) {
            if (!spillToDisk) {
                throw new JetMemoryException("Not enough memory for processing. Spilling is turned off");
            }
            //Need to start spilling
            return false;
        }
        return true;
    }

    private void writeTuple(Tuple2 tuple, Comparator comparator) {
        serviceMemoryBlock.reset();
        JetIoUtil.writeTuple(tuple, jetDataOutput, ioContext, serviceMemoryBlock);
        long serviceTupleAddress = jetDataOutput.baseAddress();
        long partitionHash = comparator.getPartitionHasher().hash(
                serviceMemoryBlock.getAccessor(), addressOfKeyBlockAt(serviceTupleAddress),
                sizeOfKeyBlockAt(serviceTupleAddress, serviceMemoryBlock.getAccessor())
        );
        final long tupleSize = jetDataOutput.usedSize();
        final int partitionId = (int) (partitionHash & partitionHashMask);

        // Set up the context for the current method call in this object and collaborators
        currentPartition = partitions[partitionId];
        final Storage storage = currentPartition.getStorage();
        final MemoryBlock memoryBlock = currentPartition.getMemoryBlockChain().current();
        header.setMemoryBlock(memoryBlock);
        storage.setMemoryBlock(memoryBlock);
        jetDataOutput.setMemoryManager(memoryBlock);

        final long tupleAddress = memoryBlock.getAllocator().allocate(tupleSize + FIRST_FOOTER_SIZE_BYTES);
        memoryBlock.copyFrom(serviceMemoryBlock, serviceTupleAddress, tupleAddress, tupleSize);
        if (accumulator != null && accumulator.isAssociative()) {
            accumulate(tupleAddress, tupleSize, partitionHash, storage, comparator, memoryBlock);
        } else {
            final long slotAddress = storage.insertTuple(tupleAddress, comparator);
            if (slotAddress > 0) {
                storage.setSlotHashCode(slotAddress, partitionHash);
            }
        }
    }

    private void accumulate(long tupleAddress, long tupleSize, long hashCode, Storage storage,
                            Comparator comparator, MemoryBlock memoryBlock
    ) {
        long slotAddress = storage.getOrCreateSlotWithSameKey(tupleAddress, comparator);
        if (slotAddress > 0) {
            storage.setSlotHashCode(slotAddress, hashCode);
            return;
        }
        slotAddress = -slotAddress;
        long addrOfHeadTuple = storage.addrOfFirstTuple(slotAddress);
        final MemoryAccessor mem = memoryBlock.getAccessor();
        long addrOfHeadValue = addrOfValueBlockAt(addrOfHeadTuple, mem);
        long sizeOfHeadValue = sizeOfValueBlockAt(addrOfHeadTuple, mem);
        long addrOfValue = addrOfValueBlockAt(tupleAddress, mem);
        long sizeOfValue = sizeOfValueBlockAt(tupleAddress, mem);
        accumulator.accept(mem, addrOfHeadValue, sizeOfHeadValue, addrOfValue, sizeOfValue, useBigEndian);
        memoryBlock.getAllocator().free(tupleAddress, tupleSize + FIRST_FOOTER_SIZE_BYTES);
    }
}
