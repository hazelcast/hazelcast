package com.hazelcast.dictionary.impl;

import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

public class DataRegion {
    private final static Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final DictionaryConfig config;
    private final EntryEncoder encoder;
    private final EntryModel model;

    // the number of bytes of memory in this segment.
    private int length;
    // the address of the first byte of memory where key/values are stored.
    private long address = 0;
    // the offset of the first free byes to store data (key/values)
    private int freeOffset;
    // the bytes available for writing key/values
    private int available;
    // contains the number of entries in this segment.
    // is volatile so it can be read by different threads concurrently
    // will never be modified concurrently
    private volatile int count;

    public DataRegion(DictionaryConfig config, EntryEncoder encoder, EntryModel model) {
        this.config = config;
        this.model = model;
        this.length = config.getInitialSegmentSize();
        this.encoder = encoder;
    }

    public void init() {
        this.address = unsafe.allocateMemory(length);
        this.available = length;
        this.freeOffset = 0;
    }

    public void clear() {
        this.freeOffset = 0;
        this.available = length;
    }

    public int insert(Object key, Object value) {
        for (; ; ) {
            int offset = freeOffset;
            int bytesWritten = encoder.writeEntry(key, value, address + offset, available);
            if (bytesWritten == -1) {
                expand();
                continue;
            }

            count++;

            //  System.out.println("bytes written:" + bytesWritten);
            available -= bytesWritten;
            this.freeOffset += bytesWritten;
            // System.out.println("address after value insert:" + dataFreeOffset);
            // System.out.println("count:" + count);
            // no item exists, so we need to allocate new
            return offset;
        }
    }

    public Object readValue(int offset){
       return encoder.readValue(address + offset + model.keyLength());
    }

    private void expand() {
        if (length == config.getMaxSegmentSize()) {
            throw new IllegalStateException(
                    "Can't grow segment beyond configured maxSegmentSize of " + config.getMaxSegmentSize());
        }

        long newSegmentLength = Math.min(config.getMaxSegmentSize(), length * 2L);

        System.out.println("expanding from:" + length + " to:" + newSegmentLength);

        if (newSegmentLength > Integer.MAX_VALUE) {
            throw new IllegalStateException("Can't grow beyond 2GB");
        }

        long newSegmentAddress = unsafe.allocateMemory(newSegmentLength);
        // copy the data
        unsafe.copyMemory(address, newSegmentAddress, freeOffset);

        unsafe.freeMemory(address);

        int dataConsumed = length - available;

        this.available = (int) (newSegmentLength - dataConsumed);
        this.length = (int) newSegmentLength;
        this.address = newSegmentAddress;
    }

    public void overwrite(Object value, long offset) {
        // System.out.println("put existing record found, overwriting value, found offset:" + offset);
        encoder.writeValue(value, address + offset);
    }

    public int count() {
        return count;
    }
}
