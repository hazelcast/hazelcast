package com.hazelcast.test;

import java.util.Arrays;

/**
 * Backport of selected convenient classes from JDK 7.
 * It's intended to be used in tests only.
 */
public class ObjectTestUtils {
    public static boolean equals(Object a, Object b) {
        return (a == b) || (a != null && a.equals(b));
    }

    public static int hash(Object... values) {
        return Arrays.hashCode(values);
    }

    public static int hashCode(Object o) {
        return o != null ? o.hashCode() : 0;
    }
}
