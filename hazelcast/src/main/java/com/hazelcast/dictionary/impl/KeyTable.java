package com.hazelcast.dictionary.impl;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

public class KeyTable {
    private static final int SLOT_BYTES = INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;
    private final static Unsafe unsafe = UnsafeUtil.UNSAFE;

    private long address;
    private int length;
    private int slots;


    public KeyTable(int length){
        this.length = length;
        this.slots = length / SLOT_BYTES;
    }

    public void alloc(){
        this.address = unsafe.allocateMemory(length);
        initData();
    }

    private void initData() {
        long address = this.address;
        for (int k = 0; k < slots; k++) {
            unsafe.putInt(address, 0);
            address += SLOT_BYTES;
        }
    }

    /**
     * Gets the offset of entry.
     *
     * @param key           the key of the entry
     * @param partitionHash the hashcode of the entry (comes from Data).
     * @return the offset or -1 if the key isn't found in the segment.
     */
    public int offsetSearch(Object key, int partitionHash) {
        int i = 0;
        int hash = correctPartitionHash(partitionHash);
        for (; ; ) {
            int slot = slot(hash, i);
            int foundHash = unsafe.getInt(address + SLOT_BYTES * slot);
            ///System.out.println("hash in slot:" + foundHash);
            if (foundHash == 0) {
                return -1;
            } else if (foundHash == hash) {
                // System.out.println("reading offset");
                return unsafe.getInt(address + SLOT_BYTES * slot + INT_SIZE_IN_BYTES);
            }
            i++;
        }
    }

    public void offsetInsert(Object key, int partitionHash, int offset) {
        int i = 0;
        int hash = correctPartitionHash(partitionHash);
        // System.out.println("writing hash:" + hash);
        // System.out.println("writing offset:" + offset);
        for (; ; ) {
            int slot = slot(hash, i);
            int foundHash = unsafe.getInt(address + SLOT_BYTES * slot);

            if (foundHash == 0) {
                // empty slot
                unsafe.putInt(address + SLOT_BYTES * slot, hash);
                unsafe.putInt(address + SLOT_BYTES * slot + INT_SIZE_IN_BYTES, offset);
                return;
            }
            i++;
        }
    }

    private int correctPartitionHash(int partitionHash) {
        if (partitionHash > 0) {
            return partitionHash;
        } else if (partitionHash < 0) {
            return partitionHash == Integer.MIN_VALUE ? partitionHash - 1 : partitionHash;
        } else {
            return Integer.MAX_VALUE;
        }
    }

    private int slot(int partitionHash, int i) {
        return (Math.abs(partitionHash) + i) % slots;
    }

}
