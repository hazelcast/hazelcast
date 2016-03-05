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


import com.hazelcast.jet.memory.api.binarystorage.oalayout.HsaResizeListener;
import com.hazelcast.jet.memory.impl.util.IOUtil;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.memory.impl.util.MemoryUtil;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryHasher;
import com.hazelcast.internal.util.hashslot.HashSlotCursor8byteKey;
import com.hazelcast.jet.memory.api.binarystorage.JetHashSlotArray8byteKey;
import com.hazelcast.jet.memory.impl.binarystorage.hsa.HashSlotArray8byteKeyNoValue;

public class JetHashSlotArray8ByteKeyImpl
        extends HashSlotArray8byteKeyNoValue implements JetHashSlotArray8byteKey {
    private long lastHashCode;
    private final int hashCodeOffset;
    private final BinaryHasher binaryHasher;
    private final HashSlotCursor8byteKey cursorLongKey2;
    private final ObjectHolder<BinaryHasher> objectHolder;
    private final HsaResizeListener hsaResizeListener;
    private final ObjectHolder<MemoryAccessor> memoryAccessorHolder;

    public JetHashSlotArray8ByteKeyImpl(long nullSentinel,
                                        BinaryHasher binaryHasher,
                                        ObjectHolder<BinaryHasher> hasherHolder,
                                        ObjectHolder<MemoryAccessor> memoryAccessorHolder,
                                        HsaResizeListener hsaResizeListener,
                                        int hashCodeOffset,
                                        int initialCapacity,
                                        float loadFactor) {
        super(nullSentinel, null, initialCapacity, loadFactor);
        this.binaryHasher = binaryHasher;
        this.objectHolder = hasherHolder;
        this.hashCodeOffset = hashCodeOffset;
        this.cursorLongKey2 = super.cursor();
        this.hsaResizeListener = hsaResizeListener;
        this.memoryAccessorHolder = memoryAccessorHolder;
    }

    protected boolean equal(long key1a, long key2a, long key1b, long key2b) {
        MemoryAccessor leftMemoryAccessor = this.mem;

        MemoryAccessor rightMemoryAccessor =
                memoryAccessorHolder.getObject(this.mem);

        return objectHolder.getObject(binaryHasher).equal(
                leftMemoryAccessor,
                rightMemoryAccessor,
                IOUtil.getKeyAddress(key1a, leftMemoryAccessor),
                IOUtil.getKeyWrittenBytes(key1a, leftMemoryAccessor),
                IOUtil.getKeyAddress(key1b, leftMemoryAccessor),
                IOUtil.getKeyWrittenBytes(key2a, leftMemoryAccessor)
        );
    }

    @Override
    protected void resizeTo(long newCapacity) {
        final long oldCapacity = capacity();
        final long oldAddress;

        oldAddress = baseAddress;
        allocateArrayAndAdjustFields(size(), newCapacity);
        final long mask = mask();

        for (long slot = oldCapacity; --slot >= 0; ) {
            if (isAssigned(oldAddress, slot)) {
                long newSlot = slotHash(oldAddress, slot) & mask;

                while (isSlotAssigned(newSlot)) {
                    newSlot = (newSlot + 1) & mask;
                }

                putKey(baseAddress, newSlot, key1OfSlot(oldAddress, slot), key2OfSlot(oldAddress, slot));
                final long valueAddrOfOldSlot = slotBase(oldAddress, slot) + valueOffset;
                mem.copyMemory(valueAddrOfOldSlot, valueAddrOfSlot(newSlot), valueLength);
            }
        }

        int slotSize = keySize() + valueSize();
        long newHsaBase = address() - HEADER_SIZE;
        long oldHsaLength = (oldCapacity * slotSize) + HEADER_SIZE;
        long newHsaLength = (capacity() * slotSize) + HEADER_SIZE;
        long destHsaBase = newHsaBase + oldHsaLength;
        mem.copyMemory(newHsaBase, destHsaBase, newHsaLength);
        malloc.free(MemoryUtil.NULL_VALUE, oldHsaLength);
        gotoAddress(destHsaBase + HEADER_SIZE);
        hsaResizeListener.onHsaResize(destHsaBase + HEADER_SIZE);

    }

    @Override
    public long slotBaseAddress(long baseAddress, long slotNumber) {
        return super.slotBase(baseAddress, slotNumber);
    }

    @Override
    public long getLastHashCode() {
        return lastHashCode;
    }

    @Override
    public boolean isSlotAssigned(long baseAddress, long slot) {
        return super.isAssigned(baseAddress, slot);
    }

    @Override
    public void setMemoryManager(MemoryManager memoryManager) {
        mem = memoryManager.getAccessor();
        malloc = memoryManager.getAllocator();
    }

    @Override
    protected long keyHash(long record, long ignore) {
        MemoryAccessor memoryAccessor =
                memoryAccessorHolder.getObject(this.mem);

        long keyAddress = IOUtil.getKeyAddress(record, memoryAccessor);
        long keySize = IOUtil.getKeyWrittenBytes(record, memoryAccessor);

        lastHashCode =
                objectHolder.getObject(binaryHasher).hash(
                        keyAddress,
                        keySize,
                        memoryAccessor
                );

        return lastHashCode;
    }

    public static long valueAddr2slotBase(long valueAddr) {
        return valueAddr - KEY_SIZE;
    }

    @Override
    protected long slotHash(long baseAddress, long slot) {
        long slotAddress = slotBase(baseAddress, slot);
        long recordAddress = mem.getLong(slotAddress);
        return mem.getLong(recordAddress + IOUtil.getRecordSize(recordAddress, mem) + hashCodeOffset);
    }

    @Override
    public HashSlotCursor8byteKey cursor() {
        cursorLongKey2.reset();
        return cursorLongKey2;
    }
}
