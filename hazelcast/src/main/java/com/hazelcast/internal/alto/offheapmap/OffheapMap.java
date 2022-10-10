/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.alto.offheapmap;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.internal.alto.util.Allocator;
import sun.misc.Unsafe;

import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;
import static java.lang.Math.abs;
import static java.lang.System.currentTimeMillis;

/**
 * For configuration of jemalloc on the JVM
 * https://github.com/jeffgriffith/native-jvm-leaks
 * <p>
 * pveentjer@wopr:~$ ldconfig -p | grep jemalloc
 * libjemalloc.so.2 (libc6,x86-64) => /lib64/libjemalloc.so.2
 * libjemalloc.so (libc6,x86-64) => /lib64/libjemalloc.so
 * <p>
 * export LD_PRELOAD=/lib64/libjemalloc.so
 */
public class OffheapMap {

    private final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final int addressSize = unsafe.addressSize();
    private final Allocator allocator;
    private final float maxLoadFactor;
    private long mod;
    private long size;
    private long tableSize;
    private long tableAddress;

    public OffheapMap(long initialCapacity, Allocator allocator) {
        this(initialCapacity, allocator, 0.75f);
    }

    public OffheapMap(long initialCapacity,
                      Allocator allocator,
                      float maxLoadFactor) {
        this.tableSize = nextPowerOfTwo(initialCapacity);
        this.mod = tableSize - 1;
        this.allocator = allocator;
        this.maxLoadFactor = maxLoadFactor;
        this.tableAddress = allocator.callocate(initialCapacity * addressSize);
    }

    public long tableSize() {
        return tableSize;
    }

    private long index(Bin key) {
        return abs(key.hash()) & mod;
    }

    private static long index(int hash, long mod) {
        return abs(hash) & mod;
    }

    public void execute(Query t) {
        for (long node = 0; node < tableSize; node++) {
            long nodeAddress = unsafe.getAddress(tableAddress + node * addressSize);

            if (nodeAddress != 0) {
                int entryCount = unsafe.getInt(nodeAddress);
                for (long entry = 0; entry < entryCount; entry++) {
                    long recordAddress = unsafe.getAddress(nodeAddress + BYTES_INT + entry * addressSize);

                    if (recordAddress != 0) {
                        t.process(recordAddress);
                    }
                }
            }
        }
    }

    /**
     * Return the number of entries in this OffheapMap.
     *
     * @return number of entries.
     */
    public long size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public float loadFactor() {
        return size == 0 ? 0 : (1.0f * size) / tableSize;
    }

    private void resize() {
        if (loadFactor() <= maxLoadFactor) {
            return;
        }

        long oldTableSize = tableSize;
        long newTableSize = tableSize * 2;
        long startMs = currentTimeMillis();

        long newMod = newTableSize - 1;
        long newTableSizeInBytes = newTableSize * addressSize;
        long newTableAddress = allocator.callocate(newTableSizeInBytes);

        for (long index = 0; index < tableSize; index++) {
            final long nodeAddress = unsafe.getAddress(tableAddress + index * addressSize);

            if (nodeAddress == 0) {
                continue;
            }

            int nodeCount = unsafe.getInt(nodeAddress);
            for (long node = 0; node < nodeCount; node++) {
                long recordAddress = unsafe.getAddress(nodeAddress + BYTES_INT + node * addressSize);

                int keySize = unsafe.getInt(recordAddress);

                int hashcode = 1;
                // not very efficient
                for (int k = 0; k < keySize; k++) {
                    hashcode = 31 * hashcode + unsafe.getByte(recordAddress + BYTES_INT + k);
                }

                long newIndex = index(hashcode, newMod);
                //System.out.println("new bucket:"+newIndex);
                long newNodeAddress = unsafe.getAddress(newTableAddress + newIndex * addressSize);
                int newNodeCount;
                if (newNodeAddress == 0) {
                    newNodeCount = 0;
                    newNodeAddress = allocator.allocate(BYTES_INT + addressSize);
                } else {
                    newNodeCount = unsafe.getInt(newNodeAddress);
                    newNodeAddress = allocator.reallocate(newNodeAddress, BYTES_INT + addressSize * (newNodeCount + 1));
                }

                unsafe.putAddress(newNodeAddress + BYTES_INT + addressSize * newNodeCount, recordAddress);
                unsafe.putInt(newNodeAddress, newNodeCount + 1);
                unsafe.putAddress(newTableAddress + newIndex * addressSize, newNodeAddress);
            }

            unsafe.freeMemory(nodeAddress);
        }

        allocator.free(tableAddress);
        this.tableSize = newTableSize;
        this.tableAddress = newTableAddress;
        this.mod = newMod;

        long durationMs = currentTimeMillis() - startMs;
        System.out.println("resizing from " + oldTableSize + " to:" + newTableSize + " took " + durationMs + " ms");
    }

    public boolean get(Bin key, Bout value) {
        long index = index(key);
        long nodeAddress = unsafe.getAddress(tableAddress + index * addressSize);

        if (nodeAddress != 0) {
            int nodeCount = unsafe.getInt(nodeAddress);

            for (int node = 0; node < nodeCount; node++) {
                long recordAddress = unsafe.getAddress(nodeAddress + BYTES_INT + (long) node * addressSize);

                if (key.unsafeEquals(unsafe, recordAddress)) {
                    long valueAddress = recordAddress + BYTES_INT + key.size();
                    value.writeFrom(unsafe, valueAddress);
                    return true;
                }
            }
        }

        value.writeNull();
        return false;
    }

    public void set(Bin key, Bin value) {
        long index = index(key);
        long nodeAddress = unsafe.getAddress(tableAddress + index * addressSize);
        int nodeCount = nodeAddress == 0 ? 0 : unsafe.getInt(nodeAddress);

        if (nodeCount > 10) {
            System.out.println(nodeCount);
        }

        if (!update(key, value, nodeAddress, nodeCount)) {
            insert(key, value, index, nodeAddress, nodeCount);
            resize();
        }
    }

    private boolean update(Bin key, Bin value, long nodeAddress, int nodeCount) {
        for (long node = 0; node < nodeCount; node++) {
            long recordAddress = unsafe.getAddress(nodeAddress + BYTES_INT + node * addressSize);

            if (key.unsafeEquals(unsafe, recordAddress)) {
                long oldValueAddress = recordAddress + BYTES_INT + key.size();
                long oldValueSize = unsafe.getInt(oldValueAddress);
                if (value.size() == oldValueSize) {
                    value.copyTo(unsafe, oldValueAddress);
                } else {
                    recordAddress = allocator.reallocate(recordAddress, BYTES_INT + key.size() + BYTES_INT + value.size());
                    key.copyTo(unsafe, recordAddress);
                    long valueAddress = recordAddress + BYTES_INT + key.size();
                    value.copyTo(unsafe, valueAddress);
                    unsafe.putAddress(nodeAddress + BYTES_INT + node * addressSize, recordAddress);
                }
                return true;
            }
        }
        return false;
    }

    private void insert(Bin key, Bin value, long index, long nodeAddress, int nodeCount) {
        long recordAddress = allocator.allocate(BYTES_INT + key.size() + BYTES_INT + value.size());
        key.copyTo(unsafe, recordAddress);
        value.copyTo(unsafe, recordAddress + BYTES_INT + key.size());

        nodeAddress = nodeAddress == 0
                ? allocator.allocate(BYTES_INT + addressSize)
                : allocator.reallocate(nodeAddress, BYTES_INT + addressSize * (nodeCount + 1));

        unsafe.putAddress(nodeAddress + BYTES_INT + addressSize * nodeCount, recordAddress);
        unsafe.putInt(nodeAddress, nodeCount + 1);
        unsafe.putAddress(tableAddress + index * addressSize, nodeAddress);
        size++;
    }

    public void clear() {
        for (long node = 0; node < tableSize; node++) {
            long nodeAddress = unsafe.getAddress(tableAddress + node * addressSize);

            if (nodeAddress != 0) {
                int entryCount = unsafe.getInt(nodeAddress);
                for (long entry = 0; entry < entryCount; entry++) {
                    long recordAddress = unsafe.getAddress(nodeAddress + BYTES_INT + entry * addressSize);

                    if (recordAddress != 0) {
                        allocator.free(recordAddress);
                    }
                }

                unsafe.freeMemory(nodeAddress);
                unsafe.putAddress(tableAddress + node * addressSize, 0);
            }
        }

        size = 0;
    }
}
