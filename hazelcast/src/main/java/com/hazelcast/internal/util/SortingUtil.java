/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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
     * <p>
     * Suppose that
     * i1 = -500.000.000
     * i2 = 2.000.000.000
     * <p>
     * Normally "i1 < i2", but if we use "i1 - i2" for comparison,
     * i1 - i2 = -500.000.000 - 2.000.000.000 and we may accept the result as "-2.500.000.000".
     * But the actual result is "1.794.967.296" because of overflow between
     * positive and negative integer bounds.
     * <p>
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
        return (entry1, entry2) -> SortingUtil.compare(comparator, iterationType, entry1, entry2);
    }

    private static Comparator<QueryableEntry> newComparator(final PagingPredicateImpl pagingPredicate) {
        return (entry1, entry2) ->
                SortingUtil.compare(pagingPredicate.getComparator(), pagingPredicate.getIterationType(), entry1, entry2);
    }

    public static List<QueryableEntry> getSortedSubList(List<QueryableEntry> list, PagingPredicate pagingPredicate,
                                                        Map.Entry<Integer, Map.Entry> nearestAnchorEntry) {
        if (pagingPredicate == null || list.isEmpty()) {
            return list;
        }
        PagingPredicateImpl pagingPredicateImpl = (PagingPredicateImpl) pagingPredicate;
        Comparator<QueryableEntry> comparator = newComparator(pagingPredicateImpl);
        Collections.sort(list, comparator);
        int nearestPage = nearestAnchorEntry.getKey();
        int pageSize = pagingPredicate.getPageSize();
        int page = pagingPredicate.getPage();
        long totalSize = pageSize * ((long) page - nearestPage);
        if (list.size() > totalSize) {
            // it's safe to cast totalSize back to int here since it's limited by the list size
            list = list.subList(0, (int) totalSize);
        }
        return list;
    }

    @SuppressWarnings("unchecked")
    public static ResultSet getSortedQueryResultSet(List<Map.Entry> list,
                                                    PagingPredicate pagingPredicate, IterationType iterationType) {
        List<? extends Map.Entry> subList = getSortedSubListAndUpdateAnchor(list, pagingPredicate, iterationType);
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
        PagingPredicateImpl pagingPredicateImpl = (PagingPredicateImpl) pagingPredicate;
        Comparator<Map.Entry> comparator = pagingPredicate.getComparator();
        IterationType iterationType = pagingPredicateImpl.getIterationType();
        return SortingUtil.compare(comparator, iterationType, anchor, queryEntry) < 0;
    }

    /**
     *
     * @param list The entry list to be sorted
     * @param pagingPredicate The predicate to be used for query. The anchor list in the predicate is also updated.
     * @return The list of Data for the requested page. If iteration type is KEY only key data list is returned,
     * else if iterationType is VALUE the value data list is returned. If iterationType is ENTRY, then list of entry of
     * (key data, value data) is returned.
     */
    public static List getSortedSubListData(List<QueryableEntry> list, PagingPredicateImpl pagingPredicate) {
        IterationType iterationType = pagingPredicate.getIterationType();
        Map.Entry<Integer, Integer> pageIndex = getPageIndexesAndUpdateAnchor(list, pagingPredicate,
                iterationType);
        int begin = pageIndex.getKey();
        int end = pageIndex.getValue();
        if (begin == -1) {
            return Collections.EMPTY_LIST;
        }
        List result = new ArrayList(end - begin);
        for (int i = begin; i < end; ++i) {
            CachedQueryEntry entry = (CachedQueryEntry) list.get(i);
            switch (iterationType) {
                case KEY:
                    result.add(entry.getKeyData());
                    break;
                case VALUE:
                    result.add(entry.getValueData());
                    break;
                default:
                    // iteration type is ENTRY
                    result.add(new AbstractMap.SimpleImmutableEntry<>(entry.getKeyData(), entry.getValueData()));
            }
        }

        return result;
    }

    private static List<? extends Map.Entry> getSortedSubListAndUpdateAnchor(List<? extends Map.Entry> list,
                                                                             PagingPredicate pagingPredicate,
                                                                             IterationType iterationType) {
        Map.Entry<Integer, Integer> pageIndex = getPageIndexesAndUpdateAnchor(list, pagingPredicate, iterationType);
        int begin = pageIndex.getKey();
        int end = pageIndex.getValue();
        if (begin == -1) {
            return Collections.EMPTY_LIST;
        }

        return list.subList(begin, end);
    }

    private static Map.Entry<Integer, Integer> getPageIndexesAndUpdateAnchor(List<? extends Map.Entry> list,
                                                                             PagingPredicate pagingPredicate,
                                                                             IterationType iterationType) {
        if (list.isEmpty()) {
            return new AbstractMap.SimpleImmutableEntry<Integer, Integer>(-1, -1);
        }
        PagingPredicateImpl pagingPredicateImpl = (PagingPredicateImpl) pagingPredicate;
        Comparator<Map.Entry> comparator = SortingUtil.newComparator(pagingPredicateImpl.getComparator(), iterationType);
        Collections.sort(list, comparator);

        Map.Entry<Integer, Map.Entry> nearestAnchorEntry = pagingPredicateImpl.getNearestAnchorEntry();
        int nearestPage = nearestAnchorEntry.getKey();
        int page = pagingPredicateImpl.getPage();
        int pageSize = pagingPredicateImpl.getPageSize();
        long begin = pageSize * ((long) page - nearestPage - 1);
        int size = list.size();
        if (begin > size) {
            return new AbstractMap.SimpleImmutableEntry<Integer, Integer>(-1, -1);
        }
        long end = begin + pageSize;
        if (end > size) {
            end = size;
        }
        setAnchor(list, pagingPredicateImpl, nearestPage);
        // it's safe to cast begin and end back to int here since they are limited by the list size
        return new AbstractMap.SimpleImmutableEntry<Integer, Integer>((int) begin, (int) end);
    }

    private static void setAnchor(List<? extends Map.Entry> list, PagingPredicateImpl pagingPredicate, int nearestPage) {
        if (list.isEmpty()) {
            return;
        }
        int size = list.size();
        int pageSize = pagingPredicate.getPageSize();
        int page = pagingPredicate.getPage();
        for (int i = pageSize; i <= size && nearestPage < page; i += pageSize) {
            Map.Entry anchor = list.get(i - 1);
            nearestPage++;
            pagingPredicate.setAnchor(nearestPage, anchor);
        }
    }

}
