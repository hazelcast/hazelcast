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

package com.hazelcast.jet.memory.impl.binarystorage;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.memory.impl.util.IOUtil;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.memory.impl.util.MemoryUtil;
import com.hazelcast.jet.io.serialization.JetDataOutput;
import com.hazelcast.jet.memory.spi.operations.ElementsReader;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryHasher;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.OALayout;
import com.hazelcast.jet.memory.api.binarystorage.BinaryKeyValueStorage;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.HsaResizeListener;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.RecordHeaderLayOut;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinarySlotIterator;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinaryRecordIterator;
import com.hazelcast.jet.memory.impl.binarystorage.iterator.BinarySlotIteratorImpl;
import com.hazelcast.jet.memory.impl.binarystorage.iterator.BinaryRecordIteratorImpl;
import com.hazelcast.jet.memory.impl.binarystorage.oalayout.DefaultMultiValueOALayout;

import java.io.IOException;

import static com.hazelcast.jet.memory.impl.util.MemoryUtil.toSlotAddress;
import static com.hazelcast.jet.memory.api.binarystorage.oalayout.OALayout.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.jet.memory.api.binarystorage.oalayout.OALayout.DEFAULT_INITIAL_CAPACITY;

/**
 * Open addressing-based storage
 * Implementation is based on HashSlot allocator;
 */
@SuppressWarnings({
        "checkstyle:methodcount"
})
public class BinaryKeyValueOpenAddressingStorage<T>
        implements BinaryKeyValueStorage<T> {
    private static final short DEFAULT_SOURCE_ID = 0;

    private final OALayout layOut;
    private MemoryBlock memoryBlock;
    private final BinarySlotIterator binarySlotIterator;
    private final BinaryRecordIterator binaryRecordIterator;

    public BinaryKeyValueOpenAddressingStorage(
            MemoryBlock memoryBlock,
            BinaryHasher binaryHasher,
            HsaResizeListener hsaResizeListener) {
        this(
                memoryBlock,
                binaryHasher,
                hsaResizeListener,
                DEFAULT_INITIAL_CAPACITY,
                DEFAULT_LOAD_FACTOR
        );
    }

    public BinaryKeyValueOpenAddressingStorage(
            MemoryBlock memoryBlock,
            BinaryHasher binaryHasher,
            HsaResizeListener hsaResizeListener,
            int initialCapacity,
            float loadFactor) {
        this.layOut = new DefaultMultiValueOALayout(
                memoryBlock,
                binaryHasher,
                hsaResizeListener,
                initialCapacity,
                loadFactor
        );

        this.memoryBlock = memoryBlock != null
                ?
                memoryBlock
                :
                null;

        this.binarySlotIterator = new BinarySlotIteratorImpl(getLayOut());
        this.binaryRecordIterator = new BinaryRecordIteratorImpl(getLayOut());
    }


    @Override
    public long getNextRecordAddress(long recordAddress) {
        return layOut.getNextRecordAddress(recordAddress);
    }

    @Override
    public long getHeaderRecordAddress(long slotAddress) {
        return layOut.getHeaderRecordAddress(slotAddress);
    }

    @Override
    public long getValueAddress(long recordAddress) {
        return layOut.getValueAddress(recordAddress);
    }

    @Override
    public long getValueWrittenBytes(long recordAddress) {
        return layOut.getValueWrittenBytes(recordAddress);
    }

    @Override
    public BinaryRecordIterator recordIterator(long slotAddress) {
        this.binaryRecordIterator.reset(slotAddress, 0);
        return this.binaryRecordIterator;
    }

    @Override
    public BinarySlotIterator slotIterator() {
        binarySlotIterator.reset();
        return binarySlotIterator;
    }

    @Override
    public long getSlotAddress(long recordAddress, short sourceId, boolean createIfNotExists) {
        return getSlotAddress0(recordAddress, sourceId, memoryBlock.getAccessor(), createIfNotExists, true, null);
    }

    @Override
    public long getSlotAddress(long recordAddress,
                               short sourceId,
                               BinaryComparator comparator,
                               boolean createIfNotExists) {
        return getSlotAddress0(recordAddress, sourceId, memoryBlock.getAccessor(), createIfNotExists, true, getHasher(comparator));
    }

    @Override
    public long createOrGetSlot(long recordAddress, short sourceId, BinaryComparator comparator) {
        return getSlotAddress0(recordAddress, sourceId, memoryBlock.getAccessor(), true, false, getHasher(comparator));
    }

    @Override
    public long createOrGetSlot(long recordAddress, BinaryComparator comparator) {
        return getSlotAddress0(recordAddress, DEFAULT_SOURCE_ID, memoryBlock.getAccessor(), true, false, getHasher(comparator));
    }

    @Override
    public long lookUpSlot(long recordAddress,
                           MemoryAccessor recordMemoryAccessor) {
        return getSlotAddress0(recordAddress, DEFAULT_SOURCE_ID, recordMemoryAccessor, false, false, null);
    }

    @Override
    public long lookUpSlot(long recordAddress,
                           BinaryComparator comparator,
                           MemoryAccessor recordMemoryAccessor) {
        return getSlotAddress0(recordAddress, DEFAULT_SOURCE_ID, recordMemoryAccessor, false, false, getHasher(comparator));
    }

    @Override
    public long getKeyAddress(long recordAddress) {
        return layOut.getKeyAddress(recordAddress);
    }

    @Override
    public long getKeyWrittenBytes(long recordAddress) {
        return layOut.getKeyWrittenBytes(recordAddress);
    }

    @Override
    public long getRecordsCount(long recordAddress) {
        return layOut.getSlotsWrittenRecords(recordAddress);
    }

    @Override
    public void markSlot(long slotAddress, byte marker) {
        layOut.markSlot(slotAddress, marker);
    }

    @Override
    public byte getSlotMarker(long slotAddress) {
        return layOut.getSlotMarker(slotAddress);
    }

    @Override
    public void setKeyHashCode(long slotAddress, long hashCode) {
        layOut.setSlotHashCode(slotAddress, hashCode);
    }

    @Override
    public long getSlotHashCode(long slotAddress) {
        return layOut.getSlotHashCode(slotAddress);
    }

    @Override
    public long count() {
        return layOut.slotsCount();
    }

    @Override
    public boolean validate() {
        return true;
    }

    @Override
    public boolean wasLastSlotCreated() {
        return layOut.wasLastSlotCreated();
    }

    @Override
    public long gotoNew() {
        return layOut.getHashSlotAllocator().gotoNew();
    }

    @Override
    public void gotoAddress(long baseAddress) {
        layOut.gotoAddress(baseAddress);
    }

    @Override
    public long rootAddress() {
        return layOut.getHashSlotAllocator().address();
    }

    private long addRecord0(long recordAddress, short sourceId) {
        try {
            long valueAddress =
                    layOut.ensureSlot(
                            recordAddress,
                            sourceId,
                            memoryBlock.getAccessor(),
                            true,
                            true
                    );

            checkLastHeaders();
            return JetHashSlotArray8ByteKeyImpl.valueAddr2slotBase(Math.abs(valueAddress));
        } catch (Throwable e) {
            throw Util.reThrow(e);
        }
    }

    private void checkLastHeaders() {
        if (!layOut.wasLastSlotCreated()) {
            memoryBlock.getAllocator().free(
                    MemoryUtil.NULL_VALUE,
                    RecordHeaderLayOut.HEADERS_DELTA
            );
        }
    }

    private long addRecord0(long recordAddress,
                            short sourceId,
                            BinaryHasher binaryHasher) {
        try {
            long valueAddress =
                    layOut.ensureSlot(
                            recordAddress,
                            sourceId,
                            memoryBlock.getAccessor(),
                            binaryHasher,
                            true,
                            true
                    );

            checkLastHeaders();
            return JetHashSlotArray8ByteKeyImpl.valueAddr2slotBase(Math.abs(valueAddress));
        } catch (Throwable e) {
            throw Util.reThrow(e);
        }
    }

    private void addRecord0(T record,
                            short sourceId,
                            ElementsReader<T> keyReader,
                            ElementsReader<T> valueReader,
                            IOContext ioContext,
                            JetDataOutput output,
                            BinaryHasher binaryHasher) {
        try {
            IOUtil.writeRecord(
                    record,
                    keyReader,
                    valueReader,
                    ioContext,
                    output,
                    memoryBlock
            );

            output.stepOn(RecordHeaderLayOut.HEADER_SIZE_BYTES);
            addRecord0(output.getPointer(), sourceId, binaryHasher);
        } catch (IOException e) {
            throw Util.reThrow(e);
        }
    }

    private void addRecord0(T record,
                            short sourceId,
                            ElementsReader<T> keyReader,
                            ElementsReader<T> valueReader,
                            IOContext ioContext,
                            JetDataOutput output) {
        try {
            IOUtil.writeRecord(
                    record,
                    keyReader,
                    valueReader,
                    ioContext,
                    output,
                    memoryBlock
            );

            output.stepOn(RecordHeaderLayOut.HEADER_SIZE_BYTES);
            addRecord0(output.getPointer(), sourceId);
        } catch (IOException e) {
            throw Util.reThrow(e);
        }
    }

    @Override
    public void addRecord(T record,
                          short sourceId,
                          ElementsReader<T> keyReader,
                          ElementsReader<T> valueReader,
                          IOContext ioContext,
                          JetDataOutput output,
                          BinaryComparator binaryComparator) {
        addRecord0(
                record,
                sourceId,
                keyReader,
                valueReader,
                ioContext,
                output,
                getHasher(binaryComparator)
        );
    }

    @Override
    public void addRecord(T record,
                          short sourceId,
                          ElementsReader<T> keyReader,
                          ElementsReader<T> valueReader,
                          IOContext ioContext,
                          JetDataOutput output) {
        addRecord0(
                record,
                sourceId,
                keyReader,
                valueReader,
                ioContext,
                output
        );
    }

    @Override
    public void addRecord(T tuple,
                          ElementsReader<T> keyReader,
                          ElementsReader<T> valueReader,
                          IOContext ioContext,
                          JetDataOutput output) {
        addRecord(
                tuple,
                DEFAULT_SOURCE_ID,
                keyReader,
                valueReader,
                ioContext,
                output
        );
    }

    @Override
    public long addRecord(long recordAddress,
                          short sourceId,
                          BinaryComparator binaryComparator) {
        return addRecord0(recordAddress, sourceId, getHasher(binaryComparator));
    }

    @Override
    public long addRecord(long recordAddress, BinaryComparator binaryComparator) {
        return addRecord0(recordAddress, DEFAULT_SOURCE_ID, getHasher(binaryComparator));
    }

    @Override
    public void setMemoryBlock(MemoryBlock memoryBlock) {
        this.memoryBlock = memoryBlock;
        this.layOut.setAuxMemoryManager(memoryBlock.getAuxMemoryManager());
    }

    @Override
    public MemoryBlock getMemoryBlock() {
        return memoryBlock;
    }

    public OALayout getLayOut() {
        return layOut;
    }

    protected BinaryHasher getHasher(BinaryComparator comparator) {
        return comparator == null ? null : comparator.getHasher();
    }

    private long getSlotAddress0(long recordAddress,
                                 short sourceId,
                                 MemoryAccessor memoryAccessor,
                                 boolean createIfNotExists,
                                 boolean createIfExists,
                                 BinaryHasher binaryHasher) {
        long valueBlockAddress =
                ensureSlot0(
                        recordAddress,
                        sourceId,
                        memoryAccessor,
                        binaryHasher,
                        createIfNotExists,
                        createIfExists
                );

        return toSlotAddress(valueBlockAddress);
    }

    private long ensureSlot0(long recordAddress,
                             short sourceId,
                             MemoryAccessor memoryAccessor,
                             BinaryHasher binaryHasher,
                             boolean createIfNotExists,
                             boolean createIfExists) {
        return binaryHasher == null
                ? layOut.ensureSlot(
                recordAddress,
                sourceId,
                memoryAccessor,
                createIfNotExists,
                createIfExists
        )
                :
                layOut.ensureSlot(
                        recordAddress,
                        sourceId,
                        memoryAccessor,
                        binaryHasher,
                        createIfNotExists,
                        createIfExists
                );
    }
}
