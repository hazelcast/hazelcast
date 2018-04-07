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

package com.hazelcast.dictionary.impl;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static java.lang.Math.abs;

/**
 * The OffsetRegion is responsible for storing the mapping between hashcode
 * to heapOffset.
 */
public class OffsetRegion {
    private static final int SLOT_SIZE = INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;
    private static final Unsafe UNSAFE = UnsafeUtil.UNSAFE;
    private static final int INT_SIZE = INT_SIZE_IN_BYTES;
    private static final int EMPTY_HASH = 0;
    private final DataRegion dataRegion;
    private final EntryEncoder entryEncoder;

    private long address;
    private volatile int length;
    private int slotCount;
    private int slotsOccupied;
    private float maxLoadFactor = 0.5f;

    public OffsetRegion(int length, DataRegion dataRegion, EntryEncoder entryEncoder) {
        this.length = length;
        this.dataRegion = dataRegion;
        this.entryEncoder = entryEncoder;
    }

    public void init() {
        alloc(length);
    }

    private void clearMemory(long address) {
        for (int slot = 0; slot < slotCount; slot++) {
            UNSAFE.putInt(address, EMPTY_HASH);
            UNSAFE.putInt(address + INT_SIZE, EMPTY_HASH);
            address += SLOT_SIZE;
        }
    }

    public float loadFactor() {
        return ((float) slotsOccupied) / slotCount;
    }

    /**
     * Gets the heapOffset of entry.
     *
     * @param key           the key of the entry
     * @param partitionHash the hashcode of the entry (comes from Data).
     * @return the heapOffset or -1 if the key isn't found in the segment.
     */
    public int search(Object key, int partitionHash) {
        int i = 0;
        int hash = correctPartitionHash(partitionHash);
        for (; ; ) {
            int slot = slot(hash, i);
            long slotAddress = address + SLOT_SIZE * slot;
            int foundHash = UNSAFE.getInt(slotAddress);
            if (foundHash == EMPTY_HASH) {
                return -1;
            } else if (foundHash == hash) {
                int offset = UNSAFE.getInt(slotAddress + INT_SIZE);
                if (entryEncoder.keyMatches(dataRegion.address() + offset, key)) {
                    return offset;
                }
            }

            i++;
        }
    }

    public void insert(Object key, int partitionHash, int offset) {
        if (loadFactor() > maxLoadFactor) {
            expand();
        }

        int hash = correctPartitionHash(partitionHash);
        insert0(hash, offset);
    }

    private void insert0(int hash, int offset) {
        int i = 0;
        // System.out.println("writing hash:" + hash);
        // System.out.println("writing heapOffset:" + heapOffset);
        for (; ; ) {
            int slot = slot(hash, i);
            long slotAddress = address + SLOT_SIZE * slot;
            int foundHash = UNSAFE.getInt(slotAddress);

            if (foundHash != EMPTY_HASH) {
                i++;
                continue;
            }

            slotsOccupied++;
            // empty slot
            UNSAFE.putInt(slotAddress, hash);
            UNSAFE.putInt(slotAddress + INT_SIZE, offset);
            return;
        }
    }

    private void expand() {
        System.out.println("Expanding OffsetRegion from: " + length + " to "
                + (length * 2) + " bytes, loadfactor:" + loadFactor());

        long oldAddress = address;
        int oldSlotCount = slotCount;
        alloc(length * 2);

        for (int slot = 0; slot < oldSlotCount; slot++) {
            long slotAddress = oldAddress + SLOT_SIZE * slot;
            int hash = UNSAFE.getInt(slotAddress);
            if (hash == EMPTY_HASH) {
                continue;
            }
            int offset = UNSAFE.getInt(slotAddress + INT_SIZE);
            insert0(hash, offset);
        }

        UNSAFE.freeMemory(oldAddress);
    }

    private static int correctPartitionHash(int partitionHash) {
        if (partitionHash > 0) {
            return partitionHash;
        } else if (partitionHash < 0) {
            return partitionHash == Integer.MIN_VALUE ? partitionHash - 1 : partitionHash;
        } else {
            return Integer.MAX_VALUE;
        }
    }

    private int slot(int partitionHash, int i) {
        return (abs(partitionHash) + i) % slotCount;
    }

    private void alloc(int length) {
        this.length = length * 2;
        this.address = UNSAFE.allocateMemory(length);
        this.slotsOccupied = 0;
        this.slotCount = length / SLOT_SIZE;
        clearMemory(address);
    }

    public void clear() {
        slotsOccupied = 0;
        clearMemory(address);
    }

    public long allocated() {
        return length;
    }

    public long consumed() {
        return slotCount * SLOT_SIZE;
    }

    public int remove(Object key, int partitionHash) {
        int i = 0;
        int hash = correctPartitionHash(partitionHash);
        for (; ; ) {
            int slot = slot(hash, i);
            long slotAddress = address + SLOT_SIZE * slot;
            int foundHash = UNSAFE.getInt(slotAddress);
            if (foundHash == EMPTY_HASH) {
                return -1;
            } else if (foundHash == hash) {
                int offset = UNSAFE.getInt(slotAddress + INT_SIZE);

                if (entryEncoder.keyMatches(dataRegion.address() + offset, key)) {
                    return offset;
                }

                UNSAFE.putInt(slotAddress, EMPTY_HASH);
                return UNSAFE.getInt(slotAddress + INT_SIZE);
            }

            i++;
        }
    }
}
