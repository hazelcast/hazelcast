package com.hazelcast.hazelfast;

import java.util.ArrayDeque;

/**
 * Class is not thread-safe.
 */
public final class ByteArrayPool {

    private final ArrayDeque<byte[]>[] array = new ArrayDeque[64];
    private final boolean enabled;

    public ByteArrayPool(boolean enabled) {
        this.enabled = enabled;
    }

    public byte[] takeFromPool(final int requestedSize) {
        if (!enabled) {
            return new byte[requestedSize];
        }
        if (requestedSize < 0) {
            throw new IllegalArgumentException("size can't be negative, size=" + requestedSize);
        }

        final int size = powerOfTwo(requestedSize);
        if(!isPowerOfTwo(size)){
            throw new RuntimeException("size isn't power of 2:"+size+" requestedSize:"+requestedSize);
        }
        int index = log2(size);
        //System.out.println("index:" + index);
        ArrayDeque<byte[]> deq = array[index];
        if (deq == null) {
            return new byte[size];
        }
        byte[] result = deq.pollFirst();
        return result != null ? result : new byte[size];
    }

    public void returnToPool(byte[] a) {
        if (!enabled) {
            return;
        }
        if (!isPowerOfTwo(a.length)) {
            throw new IllegalArgumentException("Only array with power of 2 length, " +
                    "found:" + a.length + " desired size:" + powerOfTwo(a.length));
        }

        int index = log2(a.length);
        ArrayDeque<byte[]> deq = array[index];
        if (deq == null) {
            deq = new ArrayDeque<>();
            array[index] = deq;
        }
        deq.addFirst(a);
    }

    private static boolean isPowerOfTwo(int number) {
        if (number == 0 || number == 1) {
            return true;
        }

        return number > 0 && ((number & (number - 1)) == 0);
    }

    private static int powerOfTwo(int value) {
        int highestOneBit = Integer.highestOneBit(value);
        if (value == highestOneBit) {
            return value;
        }
        return highestOneBit << 1;
    }

    /**
     * returns 0 for bits=0
     *
     * @param bits
     * @return
     */
    private static int log2(int bits) {
        int log = 0;
        if ((bits & 0xffff0000) != 0) {
            bits >>>= 16;
            log = 16;
        }
        if (bits >= 256) {
            bits >>>= 8;
            log += 8;
        }
        if (bits >= 16) {
            bits >>>= 4;
            log += 4;
        }
        if (bits >= 4) {
            bits >>>= 2;
            log += 2;
        }
        return log + (bits >>> 1);
    }
}
