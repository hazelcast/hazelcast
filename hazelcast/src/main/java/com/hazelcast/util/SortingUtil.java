package com.hazelcast.util;

import com.hazelcast.query.PagingPredicate;

import java.util.Comparator;
import java.util.Map;

/**
 * @author ali 10/12/13
 */
public final class SortingUtil {

    private SortingUtil() {
    }

    public static int compare(Comparator comparator, IterationType iterationType, Map.Entry entry1, Map.Entry entry2) {
        Object comparable1 = entry1;
        Object comparable2 = entry2;
        if (iterationType == IterationType.KEY) {
            comparable1 = entry1.getKey();
            comparable2 = entry2.getKey();
        } else if (iterationType == IterationType.VALUE) {
            comparable1 = entry1.getValue();
            comparable2 = entry2.getValue();
        }
        if (comparator != null) {
            return comparator.compare(comparable1, comparable2);
        }

        if (iterationType == IterationType.ENTRY) {
            comparable1 = entry1.getValue();
            comparable2 = entry2.getValue();
        }

        if (comparable1 instanceof Comparable && comparable2 instanceof Comparable) {
            return ((Comparable) comparable1).compareTo(comparable2);
        }
        return comparable1.hashCode() - comparable2.hashCode();
    }

    public static Comparator newComparator(final Comparator comparator) {
        return new Comparator() {
            public int compare(Object comparable1, Object comparable2) {
                if (comparator != null) {
                    return comparator.compare(comparable1, comparable2);
                }
                if (comparable1 instanceof Comparable && comparable2 instanceof Comparable) {
                    return ((Comparable) comparable1).compareTo(comparable2);
                }
                return comparable1.hashCode() - comparable2.hashCode();
            }
        };
    }

    public static Comparator<Map.Entry> newComparator(final Comparator comparator, final IterationType iterationType) {
        return new Comparator<Map.Entry>() {
            public int compare(Map.Entry entry1, Map.Entry entry2) {
                return SortingUtil.compare(comparator, iterationType, entry1, entry2);
            }
        };
    }

    public static Comparator<Map.Entry> newComparator(final PagingPredicate pagingPredicate) {
        return new Comparator<Map.Entry>() {
            public int compare(Map.Entry entry1, Map.Entry entry2) {
                return SortingUtil.compare(pagingPredicate.getComparator(), pagingPredicate.getIterationType(), entry1, entry2);
            }
        };
    }

}
