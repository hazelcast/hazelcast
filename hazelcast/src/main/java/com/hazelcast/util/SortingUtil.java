/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.util;

import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.PagingPredicateAccessor;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.replicatedmap.impl.record.ResultSet;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.query.PagingPredicateAccessor.getNearestAnchorEntry;

/**
 * Utility class for generating Comparators to be used in sort methods specific to hazelcast classes.
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
            return compareIntegers(entry1.getKey().hashCode(), entry2.getKey().hashCode());
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
                // Possibly ENTRY
                // If entries are comparable, we can compare them
                if (entry1 instanceof Comparable && entry2 instanceof Comparable) {
                    comparable1 = entry1;
                    comparable2 = entry2;
                } else {
                    // Otherwise, comparing entries directly is not meaningful.
                    // So keys can be used instead of map entries.
                    comparable1 = entry1.getKey();
                    comparable2 = entry2.getKey();
                }
                break;
        }
        checkIfComparable(comparable1);
        checkIfComparable(comparable2);

        int result = ((Comparable) comparable1).compareTo(comparable2);
        if (result != 0) {
            return result;
        }
        return compareIntegers(entry1.getKey().hashCode(), entry2.getKey().hashCode());
    }

    private static void checkIfComparable(Object comparable) {
        if (comparable instanceof Comparable) {
            return;
        }
        throw new IllegalArgumentException("Not comparable " + comparable);
    }

    /**
     * Compares two integers by considering their signs.
     * <p/>
     * Suppose that
     * i1 = -500.000.000
     * i2 = 2.000.000.000
     * <p/>
     * Normally "i1 < i2", but if we use "i1 - i2" for comparison,
     * i1 - i2 = -500.000.000 - 2.000.000.000 and we may accept the result as "-2.500.000.000".
     * But the actual result is "1.794.967.296" because of overflow between
     * positive and negative integer bounds.
     * <p/>
     * So, if we use "i1 - i2" for comparison, since the result is greater than 0,
     * "i1" is accepted as bigger that "i2". But in fact "i1" is smaller than "i2".
     * Therefore, "i1 - i2" is not a good method for comparison between signed integers.
     *
     * @param i1 First number to compare with second one
     * @param i2 Second number to compare with first one
     * @return +1 if i1 > i2, -1 if i2 > i1, 0 if i1 and i2 are equals
     */
    private static int compareIntegers(int i1, int i2) {
        // i1 - i2 is not good way for comparison
        if (i1 > i2) {
            return +1;
        } else if (i2 > i1) {
            return -1;
        } else {
            return 0;
        }
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

    public static List<QueryableEntry> getSortedSubList(List<QueryableEntry> list, PagingPredicate pagingPredicate,
                                                        Map.Entry<Integer, Map.Entry> nearestAnchorEntry) {
        if (pagingPredicate == null || list.isEmpty()) {
            return list;
        }
        Comparator<Map.Entry> comparator = SortingUtil.newComparator(pagingPredicate);
        Collections.sort(list, comparator);
        int nearestPage = nearestAnchorEntry.getKey();
        int pageSize = pagingPredicate.getPageSize();
        int page = pagingPredicate.getPage();
        int totalSize = pageSize * (page - nearestPage);
        if (list.size() > totalSize) {
            list = list.subList(0, totalSize);
        }
        return list;
    }

    @SuppressWarnings("unchecked")
    public static ResultSet getSortedQueryResultSet(List<Map.Entry> list,
                                                    PagingPredicate pagingPredicate, IterationType iterationType) {
        if (list.isEmpty()) {
            return new ResultSet();
        }
        Comparator<Map.Entry> comparator = SortingUtil.newComparator(pagingPredicate.getComparator(), iterationType);
        Collections.sort(list, comparator);

        Map.Entry<Integer, Map.Entry> nearestAnchorEntry = getNearestAnchorEntry(pagingPredicate);
        int nearestPage = nearestAnchorEntry.getKey();
        int page = pagingPredicate.getPage();
        int pageSize = pagingPredicate.getPageSize();
        int begin = pageSize * (page - nearestPage - 1);
        int size = list.size();
        if (begin > size) {
            return new ResultSet();
        }
        int end = begin + pageSize;
        if (end > size) {
            end = size;
        }
        setAnchor(list, pagingPredicate, nearestPage);
        List<Map.Entry> subList = list.subList(begin, end);
        return new ResultSet(subList, iterationType);
    }

    public static boolean compareAnchor(PagingPredicate pagingPredicate, QueryableEntry queryEntry,
                                        Map.Entry<Integer, Map.Entry> nearestAnchorEntry) {
        if (pagingPredicate == null) {
            return true;
        }
        Map.Entry anchor = nearestAnchorEntry.getValue();
        if (anchor == null) {
            return true;
        }
        Comparator<Map.Entry> comparator = pagingPredicate.getComparator();
        IterationType iterationType = pagingPredicate.getIterationType();
        return SortingUtil.compare(comparator, iterationType, anchor, queryEntry) < 0;
    }

    private static void setAnchor(List<Map.Entry> list, PagingPredicate pagingPredicate, int nearestPage) {
        if (list.isEmpty()) {
            return;
        }
        int size = list.size();
        int pageSize = pagingPredicate.getPageSize();
        int page = pagingPredicate.getPage();
        for (int i = pageSize; i <= size && nearestPage < page; i += pageSize) {
            Map.Entry anchor = list.get(i - 1);
            nearestPage++;
            PagingPredicateAccessor.setAnchor(pagingPredicate, nearestPage, anchor);
        }
    }

}
