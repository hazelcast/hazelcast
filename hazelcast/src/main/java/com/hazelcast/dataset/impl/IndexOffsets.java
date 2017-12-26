package com.hazelcast.dataset.impl;

import static com.hazelcast.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.util.QuickMath.modPowerOfTwo;
import static java.lang.Math.abs;

public final class IndexOffsets {

    private IndexOffsets() {
    }

    public static int offsetInIndex(boolean b) {
        return b ? 0 : 1;
    }

    public static int offsetInIndex(byte b) {
        return 128 + b;
    }

    public static int offsetInIndex(char c, int indexSizeDiv4) {
        return modPowerOfTwo(abs(c), indexSizeDiv4 );
    }

    public static int offsetInIndex(short s, int indexSizeDiv4) {
        return modPowerOfTwo(abs(s), indexSizeDiv4);
    }

    public static int offsetInIndex(int i, int indexSizeDiv4) {
        return modPowerOfTwo(abs(i), indexSizeDiv4);
    }

    public static int offsetInIndex(long l, int indexSizeDiv4) {
        return (int) modPowerOfTwo(abs(l), indexSizeDiv4 );
    }

    public static int offsetInIndex(float f, int indexSizeDiv4) {
        return modPowerOfTwo(abs(Float.floatToIntBits(f)), indexSizeDiv4);
    }

    public static int offsetInIndex(double d, int indexSizeDiv4) {
        return (int) modPowerOfTwo(abs(Double.doubleToLongBits(d)), indexSizeDiv4);
    }

}
