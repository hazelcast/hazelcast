package com.hazelcast.internal.query;

public class QueryUtils {

    public static int compare(Object first, Object second) {
        // TODO: Proper NULL handling.
        // TODO: Proper collation and type comparisons for all the places!
        Comparable first0 = (Comparable)first;
        Comparable second0 = (Comparable)second;

        return first0.compareTo(second0);
    }

    private QueryUtils() {
        // No-op.
    }
}
