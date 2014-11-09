package com.hazelcast.cache.impl.record;

import com.hazelcast.util.QuickMath;

import java.util.Arrays;

/**
 * Sort area for record address as thread local usage.
 */
public class CacheRecordSortArea {

    private static final int NORMALIZE_FACTORY = 50;

    private long[] longArray;
    private int[] intArray;

    public int[] getIntArray(int len) {
        if (intArray == null || intArray.length < len) {
            len = QuickMath.normalize(len, NORMALIZE_FACTORY);
            intArray = new int[len];
        } else {
            Arrays.fill(intArray, 0, len, 0);
        }
        return intArray;
    }

    public long[] getLongArray(int len) {
        if (longArray == null || longArray.length < len) {
            len = QuickMath.normalize(len, NORMALIZE_FACTORY);
            longArray = new long[len];
        } else {
            Arrays.fill(longArray, 0, len, 0L);
        }
        return longArray;
    }

}
