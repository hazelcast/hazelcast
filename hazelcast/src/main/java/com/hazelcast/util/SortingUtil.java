package com.hazelcast.util;

import com.hazelcast.query.impl.predicate.PagingPredicate;

import java.util.Comparator;
import java.util.Map;

/**
 *  Utility class for generating Comparators to be used in sort methods specific to hazelcast classes.
 */
public final class SortingUtil {

    private SortingUtil() {
    }

    public static int compare(Comparator<Map.Entry> comparator, IterationType iterationType,
                              Map.Entry entry1, Map.Entry entry2) {
        if (comparator != null) {
            int result = comparator.compare(entry1, entry2);
            if (result != 0) {
                return result;
            }
            return entry1.getKey().hashCode() - entry2.getKey().hashCode();
        }

        Object comparable1;
        Object comparable2;
        switch (iterationType) {
            case KEY:
                comparable1 = entry1.getKey();
                comparable2 = entry2.getKey();
                break;
            case VALUE:
                comparable1 = entry1.getValue();
                comparable2 = entry2.getValue();
                break;
            default:
                comparable1 = entry1;
                comparable2 = entry2;
                break;
        }
        int result;
        if (comparable1 instanceof Comparable && comparable2 instanceof Comparable) {
            result = ((Comparable) comparable1).compareTo(comparable2);
        } else {
            result = comparable1.hashCode() - comparable2.hashCode();
        }

        if (result != 0) {
            return result;
        }
        return entry1.getKey().hashCode() - entry2.getKey().hashCode();
    }

    public static Comparator<Map.Entry> newComparator(final Comparator<Map.Entry> comparator,
                                                      final IterationType iterationType) {
        return new Comparator<Map.Entry>() {
            public int compare(Map.Entry entry1, Map.Entry entry2) {
                return SortingUtil.compare(comparator, iterationType, entry1, entry2);
            }
        };
    }

    public static Comparator<Map.Entry> newComparator(final PagingPredicate pagingPredicate) {
        return new Comparator<Map.Entry>() {
            public int compare(Map.Entry entry1, Map.Entry entry2) {
                return SortingUtil.compare(pagingPredicate.getComparator(),
                        pagingPredicate.getIterationType(), entry1, entry2);
            }
        };
    }

}
