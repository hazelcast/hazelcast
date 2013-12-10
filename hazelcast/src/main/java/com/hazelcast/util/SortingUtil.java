package com.hazelcast.util;

import java.util.Comparator;

/**
 * @author ali 10/12/13
 */
public final class SortingUtil {

    public static int compare(Comparator comparator, Object value1, Object value2){
        if (comparator != null) {
            return comparator.compare(value1, value2);
        } else if (value1 instanceof Comparable && value2 instanceof Comparable) {
            return ((Comparable) value1).compareTo(value2);
        }
        return value1.hashCode() - value2.hashCode();
    }

}
