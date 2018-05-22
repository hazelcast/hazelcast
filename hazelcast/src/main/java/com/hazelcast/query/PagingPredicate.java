/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.PredicateDataSerializerHook;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.SortingUtil;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PREDICATE_DS_FACTORY_ID;

/**
 * This class is a special Predicate which helps to get a page-by-page result of a query.
 * It can be constructed with a page-size, an inner predicate for filtering, and a comparator for sorting.
 * This class is not thread-safe and stateless. To be able to reuse for another query, one should call
 * {@link PagingPredicate#reset()}
 * <br/>
 * Here is an example usage.
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
 *
 * @param <K>
 * @param <V>
 */
@BinaryInterface
public class PagingPredicate<K, V> implements IndexAwarePredicate<K, V>, IdentifiedDataSerializable {

    private static final Map.Entry<Integer, Map.Entry> NULL_ANCHOR = new SimpleImmutableEntry(-1, null);

    private List<Map.Entry<Integer, Map.Entry<K, V>>> anchorList;
    private Predicate<K, V> predicate;
    private Comparator<Map.Entry<K, V>> comparator;
    private int pageSize;
    private int page;
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
     *
     * @param pageSize page size
     */
    public PagingPredicate(int pageSize) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize should be greater than 0!");
        }
        this.pageSize = pageSize;
        anchorList = new ArrayList<Map.Entry<Integer, Map.Entry<K, V>>>();
    }

    /**
     * Construct with an inner predicate and pageSize
     * results will be filtered via inner predicate
     * results will be natural ordered
     * throws {@link IllegalArgumentException} if pageSize is not greater than 0
     * throws {@link IllegalArgumentException} if inner predicate is also {@link PagingPredicate}
     *
     * @param predicate the inner predicate through which results will be filtered
     * @param pageSize  the page size
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
     *
     * @param comparator the comparator through which results will be ordered
     * @param pageSize   the page size
     */
    public PagingPredicate(Comparator<Map.Entry<K, V>> comparator, int pageSize) {
        this(pageSize);
        this.comparator = comparator;
    }

    /**
     * Construct with an inner predicate, comparator and pageSize
     * results will be filtered via inner predicate
     * results will be ordered via comparator
     * throws {@link IllegalArgumentException} if pageSize is not greater than 0
     * throws {@link IllegalArgumentException} if inner predicate is also {@link PagingPredicate}
     *
     * @param predicate  the inner predicate through which results will be filtered
     * @param comparator the comparator through which results will be ordered
     * @param pageSize   the page size
     */
    public PagingPredicate(Predicate<K, V> predicate, Comparator<Map.Entry<K, V>> comparator, int pageSize) {
        this(pageSize);
        setInnerPredicate(predicate);
        this.comparator = comparator;
    }

    /**
     * Sets an inner predicate.
     * throws {@link IllegalArgumentException} if inner predicate is also {@link PagingPredicate}
     *
     * @param predicate the inner predicate through which results will be filtered
     */

    private void setInnerPredicate(Predicate<K, V> predicate) {
        if (predicate instanceof PagingPredicate) {
            throw new IllegalArgumentException("Nested PagingPredicate is not supported!");
        }
        this.predicate = predicate;
    }

    /**
     * Used if inner predicate is instanceof {@link IndexAwarePredicate} for filtering.
     *
     * @param queryContext
     * @return
     */
    @Override
    public Set<QueryableEntry<K, V>> filter(QueryContext queryContext) {
        if (!(predicate instanceof IndexAwarePredicate)) {
            return null;
        }

        Set<QueryableEntry<K, V>> set = ((IndexAwarePredicate<K, V>) predicate).filter(queryContext);
        if (set == null || set.isEmpty()) {
            return set;
        }
        List<QueryableEntry<K, V>> resultList = new ArrayList<QueryableEntry<K, V>>();
        Map.Entry<Integer, Map.Entry> nearestAnchorEntry = getNearestAnchorEntry();
        for (QueryableEntry<K, V> queryableEntry : set) {
            if (SortingUtil.compareAnchor(this, queryableEntry, nearestAnchorEntry)) {
                resultList.add(queryableEntry);
            }
        }

        List<QueryableEntry<K, V>> sortedSubList =
                (List) SortingUtil.getSortedSubList((List) resultList, this, nearestAnchorEntry);
        return new LinkedHashSet<QueryableEntry<K, V>>(sortedSubList);
    }


    /**
     * Used if inner predicate is instanceof {@link IndexAwarePredicate} for checking if indexed.
     *
     * @param queryContext
     * @return
     */
    @Override
    public boolean isIndexed(QueryContext queryContext) {
        if (predicate instanceof IndexAwarePredicate) {
            return ((IndexAwarePredicate) predicate).isIndexed(queryContext);
        }
        return false;
    }

    /**
     * Used for delegating filtering to inner predicate.
     *
     * @param mapEntry
     * @return
     */
    @Override
    public boolean apply(Map.Entry mapEntry) {
        if (predicate != null) {
            return predicate.apply(mapEntry);
        }
        return true;
    }

    /**
     * resets for reuse
     */
    public void reset() {
        iterationType = null;
        anchorList.clear();
        page = 0;
    }

    /**
     * sets the page value to next page
     */
    public void nextPage() {
        page++;
    }

    /**
     * sets the page value to previous page
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

    public void setPage(int page) {
        this.page = page;
    }

    public int getPageSize() {
        return pageSize;
    }

    public Predicate<K, V> getPredicate() {
        return predicate;
    }

    public Comparator<Map.Entry<K, V>> getComparator() {
        return comparator;
    }

    /**
     * Retrieve the anchor object which is the last value object on the previous page.
     * <p/>
     * Note: This method will return `null` on the first page of the query result.
     *
     * @return Map.Entry the anchor object which is the last value object on the previous page
     */
    public Map.Entry<K, V> getAnchor() {
        Map.Entry<Integer, Map.Entry<K, V>> anchorEntry = anchorList.get(page);
        return anchorEntry == null ? null : anchorEntry.getValue();
    }

    /**
     * After each query, an anchor entry is set for that page.
     * The anchor entry is the last entry of the query.
     *
     * @param anchor the last entry of the query
     */
    void setAnchor(int page, Map.Entry anchor) {
        SimpleImmutableEntry anchorEntry = new SimpleImmutableEntry(page, anchor);
        int anchorCount = anchorList.size();
        if (page < anchorCount) {
            anchorList.set(page, anchorEntry);
        } else if (page == anchorCount) {
            anchorList.add(anchorEntry);
        } else {
            throw new IllegalArgumentException("Anchor index is not correct, expected: " + page + " found: " + anchorCount);
        }
    }

    /**
     * After each query, an anchor entry is set for that page. see {@link #setAnchor(int, Map.Entry)}}
     * For the next query user may set an arbitrary page. see {@link #setPage(int)}
     * for example: user queried first 5 pages which means first 5 anchor is available
     * if the next query is for the 10th page then the nearest anchor belongs to page 5
     * but if the next query is for the 3nd page then the nearest anchor belongs to page 2
     *
     * @return nearest anchored entry for current page
     */
    Map.Entry<Integer, Map.Entry> getNearestAnchorEntry() {
        int anchorCount = anchorList.size();
        if (page == 0 || anchorCount == 0) {
            return NULL_ANCHOR;
        }

        Map.Entry anchoredEntry;
        if (page < anchorCount) {
            anchoredEntry = anchorList.get(page - 1);
        } else {
            anchoredEntry = anchorList.get(anchorCount - 1);
        }
        return anchoredEntry;
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(predicate);
        out.writeObject(comparator);
        out.writeInt(page);
        out.writeInt(pageSize);
        out.writeUTF(iterationType.name());
        out.writeInt(anchorList.size());
        for (Map.Entry<Integer, Map.Entry<K, V>> anchor : anchorList) {
            out.writeInt(anchor.getKey());
            Map.Entry<K, V> anchorEntry = anchor.getValue();
            out.writeObject(anchorEntry.getKey());
            out.writeObject(anchorEntry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        predicate = in.readObject();
        comparator = in.readObject();
        page = in.readInt();
        pageSize = in.readInt();
        iterationType = IterationType.valueOf(in.readUTF());
        int size = in.readInt();
        anchorList = new ArrayList<Map.Entry<Integer, Map.Entry<K, V>>>(size);
        for (int i = 0; i < size; i++) {
            int anchorPage = in.readInt();
            Object anchorKey = in.readObject();
            Object anchorValue = in.readObject();
            Map.Entry anchorEntry = new SimpleImmutableEntry(anchorKey, anchorValue);
            anchorList.add(new SimpleImmutableEntry<Integer, Map.Entry<K, V>>(anchorPage, anchorEntry));
        }
    }

    @Override
    public int getFactoryId() {
        return PREDICATE_DS_FACTORY_ID;
    }

    @Override
    public int getId() {
        return PredicateDataSerializerHook.PAGING_PREDICATE;
    }
}
