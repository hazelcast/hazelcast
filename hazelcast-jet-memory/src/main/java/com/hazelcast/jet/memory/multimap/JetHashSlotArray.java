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

package com.hazelcast.jet.memory.multimap;


import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.util.hashslot.HashSlotArray8byteKey;
import com.hazelcast.internal.util.hashslot.HashSlotCursor8byteKey;
import com.hazelcast.internal.util.hashslot.impl.HashSlotArrayBase;
import com.hazelcast.jet.memory.binarystorage.Hasher;
import com.hazelcast.jet.memory.util.JetIoUtil;

import java.util.function.LongConsumer;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.jet.memory.multimap.TupleMultimapHsa.HASH_CODE_OFFSET;

/**
 * Open-addressed hashtable with {@code long}-typed slots. Assumes a specific implementation of memory
 * allocator, matching Jet's "aux" allocator:
 * <ol><li>
 * Blocks are allocated from the top of the address space downwards, so that each new allocated block
 * has a lower address.
 * </li><li>
 * Blocks are right next to each other (no headers, no overhead between them).
 * </li><li>
 * {@code free(NULL_ADDRESS, size)} does not free the block at null-address, only adjusts the pointer which
 * marks the beginning of free space.
 * </li></ol>
 */
final class JetHashSlotArray extends HashSlotArrayBase implements HashSlotArray8byteKey {

    private long lastHashCode;
    private final HashSlotCursor8byteKey cursor;
    private final LongConsumer hsaResizeListener;
    private Hasher hasher;
    private MemoryAccessor localMem;

    JetHashSlotArray(LongConsumer hsaResizeListener, int initialCapacity, float loadFactor) {
        super(NULL_ADDRESS, 0L, null, null, TupleMultimapHsa.KEY_SIZE, 0, initialCapacity, loadFactor);
        this.cursor = new Cursor();
        this.hsaResizeListener = hsaResizeListener;
    }

    void setMemoryManager(MemoryManager memMgr) {
        assert memMgr != null : "Attempt to set null memory manager";
        setMemMgr(memMgr);
    }

    void setLocalMemoryAccessor(MemoryAccessor mem) {
        this.localMem = mem;
    }

    void setHasher(Hasher hasher) {
        assert hasher != null : "Attempt to set null hasher";
        this.hasher = hasher;
    }


    long slotBaseAddress(long baseAddress, long slotNumber) {
        return super.slotBase(baseAddress, slotNumber);
    }

    long getLastHashCode() {
        return lastHashCode;
    }

    boolean isSlotAssigned(long baseAddress, long slot) {
        return super.isAssigned(baseAddress, slot);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Whenever this method returns a positive value, the caller must ensure that the null-sentinel value
     * at the returned address is overwritten with a non-sentinel value.
     */
    @Override
    public long ensure(long key) {
        return super.ensure0(key, 0);
    }

    @Override
    public long get(long key) {
        return super.get0(key, 0);
    }

    @Override
    public boolean remove(long key) {
        return super.remove0(key, 0);
    }

    @Override
    protected long key2OfSlot(long baseAddress, long slot) {
        return 0;
    }

    @Override
    protected void putKey(long baseAddress, long slot, long key, long ignored) {
        mem().putLong(slotBase(baseAddress, slot) + KEY_1_OFFSET, key);
    }

    @Override
    public HashSlotCursor8byteKey cursor() {
        cursor.reset();
        return cursor;
    }

    @Override
    protected boolean equal(long leftKey1, long ignoredLeftKey2, long rightKey1, long ignoredRightKey2) {
        MemoryAccessor leftMem = mem();
        MemoryAccessor rightMem = resolveMem();
        return hasher.equal(leftMem, rightMem,
                JetIoUtil.addressOfKeyBlockAt(leftKey1),
                JetIoUtil.sizeOfKeyBlockAt(leftKey1, leftMem),
                JetIoUtil.addressOfKeyBlockAt(rightKey1),
                JetIoUtil.sizeOfKeyBlockAt(rightKey1, rightMem)
        );
    }

    @Override
    protected void resizeTo(long newCapacity) {
        final long oldCapacity = capacity();
        final long oldAddress;
        oldAddress = address();
        allocateArrayAndAdjustFields(size(), newCapacity);
        rehash(oldCapacity, oldAddress);
        long newHsaBase = address() - HEADER_SIZE;
        long oldHsaLength = (oldCapacity * slotLength) + HEADER_SIZE;
        long newHsaLength = (capacity() * slotLength) + HEADER_SIZE;
        long destHsaBase = newHsaBase + oldHsaLength;
        mem().copyMemory(newHsaBase, destHsaBase, newHsaLength);
        malloc().free(MemoryAllocator.NULL_ADDRESS, oldHsaLength);
        gotoAddress(destHsaBase + HEADER_SIZE);
        hsaResizeListener.accept(destHsaBase + HEADER_SIZE);
    }

    @Override
    protected long keyHash(long tupleAddress, long ignored) {
        final MemoryAccessor mem = resolveMem();
        final long keyAddress = JetIoUtil.addressOfKeyBlockAt(tupleAddress);
        final long keySize = JetIoUtil.sizeOfKeyBlockAt(tupleAddress, mem);
        lastHashCode = hasher.hash(mem, keyAddress, keySize);
        return lastHashCode;
    }

    @Override
    protected long slotHash(long baseAddress, long slot) {
        long headTupleAddress = mem().getLong(slotBase(baseAddress, slot));
        return mem().getLong(headTupleAddress + JetIoUtil.sizeOfTupleAt(headTupleAddress, mem()) + HASH_CODE_OFFSET);
    }

    private MemoryAccessor resolveMem() {
        return localMem != null ? localMem : mem();
    }

}
