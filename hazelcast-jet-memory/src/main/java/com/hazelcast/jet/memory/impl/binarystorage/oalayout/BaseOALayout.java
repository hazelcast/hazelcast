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

package com.hazelcast.jet.memory.impl.binarystorage.oalayout;

import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.memory.impl.util.IOUtil;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.memory.impl.util.MemoryUtil;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryHasher;
import com.hazelcast.jet.memory.impl.binarystorage.ObjectHolder;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.internal.util.hashslot.HashSlotArray8byteKey;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.OALayout;
import com.hazelcast.jet.memory.api.binarystorage.JetHashSlotArray8byteKey;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.HsaResizeListener;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.CompactionContext;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.RecordHeaderLayOut;
import com.hazelcast.jet.memory.impl.binarystorage.JetHashSlotArray8ByteKeyImpl;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.HsaSlotCreationListener;

public abstract class BaseOALayout implements OALayout {
    ///// Objects
    protected final float loadFactor;

    protected boolean wasLastSlotCreated;

    protected JetHashSlotArray8byteKey hsa;

    protected MemoryAccessor memoryAccessor;

    protected final BinaryHasher defaultBinaryHasher;

    protected final ObjectHolder<BinaryHasher> hasherHolder;

    protected final ObjectHolder<MemoryAccessor> memoryAccessorHolder;

    private final long slotSize;

    private HsaSlotCreationListener layOutListener;

    private final CompactionContext sourceContext;

    private final CompactionContext targetContext;

    private long capacityMask;

    public BaseOALayout(
            MemoryBlock memoryBlock,
            BinaryHasher defaultBinaryHasher,
            HsaResizeListener hsaResizeListener,
            int initialCapacity,
            float loadFactor
    ) {
        assert Util.isPositivePowerOfTwo(initialCapacity);

        this.loadFactor = loadFactor;
        this.defaultBinaryHasher = defaultBinaryHasher;
        this.hasherHolder = new ObjectHolder<BinaryHasher>();
        this.memoryAccessorHolder = new ObjectHolder<MemoryAccessor>();

        this.sourceContext = new DefaultCompactionContext();
        this.targetContext = new DefaultCompactionContext();

        this.hsa = new JetHashSlotArray8ByteKeyImpl(
                MemoryUtil.NULL_VALUE,
                defaultBinaryHasher,
                this.hasherHolder,
                this.memoryAccessorHolder,
                hsaResizeListener,
                RecordHeaderLayOut.HASH_CODE_OFFSET,
                initialCapacity,
                loadFactor
        );

        if (memoryBlock != null) {
            this.memoryAccessor = memoryBlock.getAccessor();
            this.hsa.setMemoryManager(memoryBlock.getAuxMemoryManager());
            this.hsa.gotoNew();
        }

        this.slotSize = this.hsa.keySize() + this.hsa.valueSize();
    }

    private void setCapacityMask() {
        capacityMask = hsa.capacity() - 1;
    }

    @Override
    public long getSlotHashCode(long slotAddress) {
        long recordAddress = getHeaderRecordAddress(slotAddress);
        long recordSize = IOUtil.getRecordSize(recordAddress, memoryAccessor);
        return IOUtil.getLong(recordAddress, recordSize + RecordHeaderLayOut.HASH_CODE_OFFSET, memoryAccessor);
    }

    @Override
    public void setSlotHashCode(long slotAddress, long hashCode) {
        long recordAddress = getHeaderRecordAddress(slotAddress);
        long recordSize = IOUtil.getRecordSize(recordAddress, memoryAccessor);
        IOUtil.setLong(recordAddress, recordSize + RecordHeaderLayOut.HASH_CODE_OFFSET, hashCode, memoryAccessor);
    }

    @Override
    public long getKeyAddress(long recordAddress) {
        return IOUtil.getKeyAddress(recordAddress, memoryAccessor);
    }

    @Override
    public long getKeyWrittenBytes(long recordAddress) {
        return IOUtil.getKeyWrittenBytes(recordAddress, memoryAccessor);
    }

    @Override
    public byte getSlotMarker(long slotAddress) {
        long recordAddress = getHeaderRecordAddress(slotAddress);
        long recordSize = IOUtil.getRecordSize(recordAddress, memoryAccessor);
        return IOUtil.getByte(
                recordAddress,
                recordSize + RecordHeaderLayOut.MARKER_OFFSET,
                memoryAccessor
        );
    }

    @Override
    public void markSlot(long slotAddress, byte marker) {
        long recordAddress = getHeaderRecordAddress(slotAddress);
        long recordSize = IOUtil.getRecordSize(recordAddress, memoryAccessor);
        IOUtil.setByte(
                recordAddress,
                recordSize + RecordHeaderLayOut.MARKER_OFFSET,
                marker,
                memoryAccessor
        );
    }

    @Override
    public long getSlotsWrittenRecords(long slotAddress) {
        long recordAddress = getHeaderRecordAddress(slotAddress);
        long recordSize = IOUtil.getRecordSize(recordAddress, memoryAccessor);

        return IOUtil.getLong(recordAddress,
                recordSize +
                        RecordHeaderLayOut.RECORDS_COUNT_OFFSET,
                memoryAccessor
        );
    }

    @Override
    public long getHeaderRecordAddress(long slotAddress) {
        return memoryAccessor.getLong(slotAddress);
    }

    @Override
    public int getSlotSizeInBytes() {
        return KEY_SIZE_IN_BYTES;
    }

    @Override
    public long ensureSlot(long recordAddress,
                           short sourceId,
                           MemoryAccessor memoryAccessor,
                           boolean createIfNotExists,
                           boolean createIfExists) {
        return ensureSlot(
                recordAddress,
                sourceId,
                memoryAccessor,
                defaultBinaryHasher,
                createIfNotExists,
                createIfExists
        );
    }

    @Override
    public long ensureSlot(
            long recordAddress,
            short sourceId,
            final MemoryAccessor ma,
            BinaryHasher hasher,
            boolean createIfNotExists,
            boolean createIfExists
    ) {
        wasLastSlotCreated = false;
        hasherHolder.setObject(hasher);
        memoryAccessorHolder.setObject(ma);

        try {
            if (createIfNotExists) {
                long valueAddress = hsa.ensure(recordAddress);

                //New slot has been created
                if (valueAddress > 0) {
                    long slotAddress =
                            JetHashSlotArray8ByteKeyImpl.valueAddr2slotBase(valueAddress);

                    onSlotCreated(slotAddress, sourceId, recordAddress, valueAddress);

                    if (layOutListener != null) {
                        layOutListener.onSlotCreated();
                    }

                    wasLastSlotCreated = true;
                } else {
                    if (createIfExists) {
                        long slotAddress =
                                JetHashSlotArray8ByteKeyImpl.valueAddr2slotBase(
                                        Math.abs(valueAddress)
                                );

                        onSlotUpdated(Math.abs(slotAddress), sourceId, recordAddress);
                    }
                    wasLastSlotCreated = false;
                }

                return valueAddress;
            } else {
                return hsa.get(recordAddress);
            }
        } finally {
            memoryAccessorHolder.setObject(memoryAccessor);
            hasherHolder.setObject(defaultBinaryHasher);
        }
    }

    protected abstract void onSlotCreated(long slotAddress, short sourceId, long recordAddress, long valueAddress);

    protected abstract void onSlotUpdated(long slotAddress, short sourceId, long recordAddress);

    @Override
    public HashSlotArray8byteKey getHashSlotAllocator() {
        return hsa;
    }

    @Override
    public long slotsCount() {
        return hsa.size();
    }

    @Override
    public long gotoNew() {
        return hsa.gotoNew();
    }

    @Override
    public void gotoAddress(long baseAddress) {
        hsa.gotoAddress(baseAddress);
    }

    @Override
    public void setListener(HsaSlotCreationListener listener) {
        this.layOutListener = listener;
    }

    @Override
    public boolean wasLastSlotCreated() {
        return wasLastSlotCreated;
    }

    @Override
    public void compact() {
        setCapacityMask();
        sourceContext.reset();
        targetContext.reset();
        processSlots();
    }

    private void processSlots() {
        long baseAddress = hsa.address();

        for (long slotNumber = 0; slotNumber < hsa.capacity(); slotNumber++) {
            boolean isAssigned = hsa.isSlotAssigned(baseAddress, slotNumber);

            CompactionContext contextToUpdate =
                    isAssigned
                            ?
                            sourceContext
                            :
                            targetContext;

            boolean needToUpdateContext =
                    (!isAssigned)
                            || (targetContext.slotsCount() > 0);

            boolean needToCopy = !isAssigned
                    && targetContext.slotsCount() > 0L
                    && sourceContext.slotsCount() > 0L;

            boolean isLastSlot = isLastSlot(slotNumber);

            needToCopy = needToCopy || isLastSlot;

            if (isLastSlot) {
                updateContext(baseAddress, slotNumber, contextToUpdate);
                needToUpdateContext = false;
            }

            if (needToCopy) {
                copySlots();
            }

            if (needToUpdateContext) {
                updateContext(
                        baseAddress,
                        slotNumber,
                        contextToUpdate
                );
            }
        }
    }

    private void updateContext(long baseAddress,
                               long slotNumber,
                               CompactionContext compactionContext) {
        if (compactionContext.chunkAddress() == MemoryUtil.NULL_VALUE) {
            compactionContext.setChunkAddress(
                    hsa.slotBaseAddress(baseAddress, slotNumber)
            );

            compactionContext.setSlotNumber(slotNumber);
        }

        compactionContext.incrementCount();
    }

    private void copySlots() {
        long slotsToCopy =
                sourceContext.slotsCount();

        memoryAccessor.copyMemory(
                sourceContext.chunkAddress(),
                targetContext.chunkAddress(),
                slotsToCopy * this.slotSize
        );

        long newTargetSlotNumber = targetContext.slotNumber()
                +
                sourceContext.slotsCount();

        targetContext.setChunkAddress(
                hsa.slotBaseAddress(
                        hsa.address(),
                        newTargetSlotNumber
                )
        );

        targetContext.setSlotNumber(newTargetSlotNumber);
        targetContext.setSlotCount(
                Math.abs(sourceContext.slotsCount() - targetContext.slotsCount())
        );

        sourceContext.reset();
    }

    private boolean isLastSlot(long slotNumber) {
        return slotNumber == hsa.capacity() - 1;
    }

    @Override
    public long capacityMask() {
        return capacityMask;
    }

    @Override
    public void setAuxMemoryManager(MemoryManager auxMemoryManager) {
        this.memoryAccessor = auxMemoryManager.getAccessor();
        hsa.setMemoryManager(auxMemoryManager);
    }
}
