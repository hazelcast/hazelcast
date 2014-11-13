package com.hazelcast.cache.impl.record;

import com.hazelcast.util.QuickMath;

import java.util.Arrays;

/**
 * Sort area for record address as thread local usage.
 */
public class CacheRecordSortArea {

    // TODO clear thread local at the end!
    public static final ThreadLocal<CacheRecordSortArea> SORT_AREA_THREAD_LOCAL =
            new ThreadLocal<CacheRecordSortArea>() {
                @Override
                protected CacheRecordSortArea initialValue() {
                    return new CacheRecordSortArea();
                }
            };

    private static final int NORMALIZE_FACTOR = 50;

    private long[] longArray;
    private int[] intArray;

    public int[] getIntArray(int len) {
        if (intArray == null || intArray.length < len) {
            len = QuickMath.normalize(len, NORMALIZE_FACTOR);
            intArray = new int[len];
        } else {
            Arrays.fill(intArray, 0, len, 0);
        }
        return intArray;
    }

    public long[] getLongArray(int len) {
        if (longArray == null || longArray.length < len) {
            len = QuickMath.normalize(len, NORMALIZE_FACTOR);
            longArray = new long[len];
        } else {
            Arrays.fill(longArray, 0, len, 0L);
        }
        return longArray;
    }

}
