/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.SortingUtil;

import java.io.IOException;
import java.util.*;

/**
 * This class is a special Predicate which helps to get a page-by-page result of a query
 * Can be constructed with a page-size, an inner predicate for filtering, A comparator for sorting  \
 * This class is not thread-safe and stateless. To be able to reuse for another query, one should call
 * {@link PagingPredicate#reset()}
 * <br/>
 * Example usage could be seen like below;
 * <pre>
 * Predicate lessEqualThanFour = Predicates.lessEqual("this", 4);
 *
 * // We are constructing our paging predicate with a predicate and page size. In this case query results fetched two
 * by two.
 * PagingPredicate predicate = new PagingPredicate(lessEqualThanFour, 2);
 *
 * // we are initializing our map with integers from 0 to 10 as keys and values.
 * IMap map = hazelcastInstance.getMap(...);
 * for (int i = 0; i < 10; i++) {
 * map.put(i, i);
 * }
 *
 * // invoking the query
 * Collection<Integer> values = map.values(predicate);
 * System.out.println("values = " + values) // will print 'values = [0, 1]'
 * predicate.nextPage(); // we are setting up paging predicate to fetch next page in the next call.
 * values = map.values(predicate);
 * System.out.println("values = " + values);// will print 'values = [2, 3]'
 * Entry anchor = predicate.getAnchor();
 * System.out.println("anchor -> " + anchor); // will print 'anchor -> 1=1',  since the anchor is the last entry of
 * the previous page.
 * predicate.previousPage(); // we are setting up paging predicate to fetch previous page in the next call
 * values = map.values(predicate);
 * System.out.println("values = " + values) // will print 'values = [0, 1]'
 * </pre>
 */
public class PagingPredicate implements IndexAwarePredicate, DataSerializable {

    private Predicate predicate;

    private Comparator<Map.Entry> comparator;

    private int pageSize;

    private int page;

    private final Map<Integer, Map.Entry> anchorMap = new LinkedHashMap<Integer, Map.Entry>();

    private IterationType iterationType;

    /**
     * Used for serialization internally
     */
    public PagingPredicate() {
    }

    /**
     * Construct with a pageSize
     * results will not be filtered
     * results will be natural ordered
     * throws {@link IllegalArgumentException} if pageSize is not greater than 0
     * @param pageSize
     */
    public PagingPredicate(int pageSize) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize should be greater than 0 !!!");
        }
        this.pageSize = pageSize;
    }

    /**
     * Construct with an inner predicate and pageSize
     * results will be filtered via inner predicate
     * results will be natural ordered
     * throws {@link IllegalArgumentException} if pageSize is not greater than 0
     * throws {@link IllegalArgumentException} if inner predicate is also {@link PagingPredicate}
     * @param predicate
     * @param pageSize
     */
    public PagingPredicate(Predicate predicate, int pageSize) {
        this(pageSize);
        setInnerPredicate(predicate);
    }

    /**
     * Construct with a comparator and pageSize
     * results will not be filtered
     * results will be ordered via comparator
     * throws {@link IllegalArgumentException} if pageSize is not greater than 0
     * @param comparator
     * @param pageSize
     */
    public PagingPredicate(Comparator<Map.Entry> comparator, int pageSize) {
        this(pageSize);
        this.comparator = comparator;
    }

    /**
     * Construct with an inner predicate, comparator and pageSize
     * results will be filtered via inner predicate
     * results will be ordered via comparator
     * throws {@link IllegalArgumentException} if pageSize is not greater than 0
     * throws {@link IllegalArgumentException} if inner predicate is also {@link PagingPredicate}
     * @param predicate
     * @param comparator
     * @param pageSize
     */
    public PagingPredicate(Predicate predicate, Comparator<Map.Entry> comparator, int pageSize) {
        this(pageSize);
        setInnerPredicate(predicate);
        this.comparator = comparator;
    }

    private void setInnerPredicate(Predicate predicate) {
        if (predicate instanceof PagingPredicate) {
            throw new IllegalArgumentException("Nested PagingPredicate is not supported!!!");
        }
        this.predicate = predicate;
    }

    /**
     * Used if inner predicate is instanceof {@link IndexAwarePredicate} for filtering
     *
     * @param queryContext
     * @return
     */
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        if (predicate instanceof IndexAwarePredicate) {
            Set<QueryableEntry> set = ((IndexAwarePredicate) predicate).filter(queryContext);
            if (set == null) {
                return null;
            }
            List<QueryableEntry> list = new LinkedList<QueryableEntry>();
            Map.Entry anchor = getAnchor();
            for (QueryableEntry entry : set) {
                // For comparison, objects to compare must be Comparable instance
                if (SortingUtil.isSuitableForCompare(comparator, iterationType, entry)) {
                    if (anchor != null &&
                            SortingUtil.compare(comparator, iterationType, anchor, entry) >= 0) {
                        continue;
                    }
                    list.add(entry);
                }
            }
            if (list.isEmpty()) {
                return null;
            }
            Collections.sort(list, SortingUtil.newComparator(this));
            if (list.size() > pageSize) {
                list = list.subList(0, pageSize);
            }
            return new LinkedHashSet<QueryableEntry>(list);
        }
        return null;
    }

    /**
     * Used if inner predicate is instanceof {@link IndexAwarePredicate} for checking if indexed
     *
     * @param queryContext
     * @return
     */
    public boolean isIndexed(QueryContext queryContext) {
        if (predicate instanceof IndexAwarePredicate) {
            return ((IndexAwarePredicate) predicate).isIndexed(queryContext);
        }
        return false;
    }

    /**
     * Used for delegating filtering to inner predicate
     *
     * @param mapEntry
     * @return
     */
    public boolean apply(Map.Entry mapEntry) {
        if (predicate != null) {
            return predicate.apply(mapEntry);
        }
        return true;
    }

    /**
     * After each query, an anchor entry is set for that page
     * anchor entry is the last entry of the query
     *
     * @param anchor
     */

    void setAnchor(Map.Entry anchor) {
        if (anchor == null) {
            previousPage();
            return;
        }
        anchorMap.put(page + 1, anchor);
    }

    /**
     * resets for reuse
     */
    public void reset() {
        iterationType = null;
        anchorMap.clear();
        page = 0;
    }

    /**
     * setting the page value to next page
     */
    public void nextPage() {
        page++;
    }

    /**
     * setting the page value to previous page
     */
    public void previousPage() {
        if (page != 0) {
            page--;
        }
    }

    public IterationType getIterationType() {
        return iterationType;
    }

    public void setIterationType(IterationType iterationType) {
        this.iterationType = iterationType;
    }

    public int getPage() {
        return page;
    }

    public int getPageSize() {
        return pageSize;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public Comparator<Map.Entry> getComparator() {
        return comparator;
    }

    public Map.Entry getAnchor() {
        return anchorMap.get(page);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(predicate);
        out.writeObject(comparator);
        out.writeInt(page);
        out.writeInt(pageSize);
        out.writeUTF(iterationType.name());
        out.writeInt(anchorMap.size());
        for (Map.Entry<Integer, Map.Entry> entry : anchorMap.entrySet()) {
            out.writeInt(entry.getKey());
            final Map.Entry anchorEntry = entry.getValue();
            out.writeObject(anchorEntry.getKey());
            out.writeObject(anchorEntry.getValue());
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        predicate = in.readObject();
        comparator = in.readObject();
        page = in.readInt();
        pageSize = in.readInt();
        iterationType = IterationType.valueOf(in.readUTF());
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            final int key = in.readInt();
            final Object anchorKey = in.readObject();
            final Object anchorValue = in.readObject();
            anchorMap.put(key, new AbstractMap.SimpleImmutableEntry(anchorKey, anchorValue));
        }
    }

}
