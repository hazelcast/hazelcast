package com.hazelcast.spi.impl.offheapmap;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import static com.hazelcast.internal.nio.Bits.BYTES_INT;

public class ExampleQuery implements Query {
    private final Unsafe unsafe = UnsafeUtil.UNSAFE;

    public long bogus;

    @Override
    public void process(long address) {
        long keyLength = unsafe.getInt(address);
        long keyValueAddress = address + BYTES_INT;
        long valueLength = unsafe.getInt(address + BYTES_INT + keyLength);
        long valueAddress = unsafe.getInt(address + BYTES_INT + keyLength + valueLength);

        bogus += valueLength + keyLength;
    }
}
