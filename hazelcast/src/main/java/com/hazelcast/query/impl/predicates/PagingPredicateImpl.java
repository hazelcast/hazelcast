/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.internal.namespace.impl.NodeEngineThreadLocalContext;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.internal.util.SortingUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexRegistry;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.cluster.Versions.V5_4;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PREDICATE_DS_FACTORY_ID;

/**
 * Implementation of {@link PagingPredicate}.
 *
 * @param <K> the entry key type
 * @param <V> the entry value type
 */
@BinaryInterface
public class PagingPredicateImpl<K, V>
        implements PagingPredicate<K, V>, IndexAwarePredicate<K, V>, VisitablePredicate, IdentifiedDataSerializable, Versioned {

    private static final Map.Entry<Integer, Map.Entry> NULL_ANCHOR = new SimpleImmutableEntry<>(-1, null);

    private List<Map.Entry<Integer, Map.Entry<K, V>>> anchorList;
    private Predicate<K, V> predicate;
    private Comparator<Map.Entry<K, V>> comparator;
    private int pageSize;
    private int page;
    private IterationType iterationType;
    private @Nullable String userCodeNamespace;

    /**
     * Used for serialization internally
     */
    public PagingPredicateImpl() {
    }

    /**
     * Construct with a pageSize
     * results will not be filtered
     * results will be natural ordered
     * throws {@link IllegalArgumentException} if pageSize is not greater than 0
     *
     * @param pageSize page size
     */
    public PagingPredicateImpl(int pageSize) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize must be greater than 0!");
        }
        this.pageSize = pageSize;
        anchorList = new ArrayList<>();
    }

    /**
     * Construct with an inner predicate and pageSize
     * results will be filtered via inner predicate
     * results will be natural ordered
     * throws {@link IllegalArgumentException} if pageSize is not greater than 0
     * throws {@link IllegalArgumentException} if inner predicate is also a {@link PagingPredicate}
     *
     * @param predicate the inner predicate through which results will be filtered
     * @param pageSize  the page size
     */
    public PagingPredicateImpl(Predicate<K, V> predicate, int pageSize) {
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
    public PagingPredicateImpl(Comparator<Map.Entry<K, V>> comparator, int pageSize) {
        this(pageSize);
        this.comparator = comparator;
    }

    /**
     * Construct with an inner predicate, comparator and pageSize
     * results will be filtered via inner predicate
     * results will be ordered via comparator
     * throws {@link IllegalArgumentException} if pageSize is not greater than 0
     * throws {@link IllegalArgumentException} if inner predicate is also a {@link PagingPredicate}
     *
     * @param predicate  the inner predicate through which results will be filtered
     * @param comparator the comparator through which results will be ordered
     * @param pageSize   the page size
     */
    public PagingPredicateImpl(Predicate<K, V> predicate, Comparator<Map.Entry<K, V>> comparator, int pageSize) {
        this(pageSize);
        setInnerPredicate(predicate);
        this.comparator = comparator;
    }

    @PrivateApi
    public PagingPredicateImpl(List<Map.Entry<Integer, Map.Entry<K, V>>> anchorList, Predicate<K, V> predicate,
                               Comparator<Map.Entry<K, V>> comparator, int pageSize, int page, IterationType iterationType,
                               @Nullable String userCodeNamespace) {
        this.anchorList = anchorList;
        this.predicate = predicate;
        this.comparator = comparator;
        this.pageSize = pageSize;
        this.page = page;
        this.iterationType = iterationType;
        this.userCodeNamespace = userCodeNamespace;
    }

    public PagingPredicateImpl(PagingPredicateImpl<K, V> original) {
        this(original, original.predicate);
    }

    /**
     * Creates a shallow copy of the given original paging predicate while
     * replacing its inner predicate with the given predicate.
     *
     * @param originalPagingPredicate the original paging predicate to copy.
     * @param predicateReplacement    the inner predicate replacement.
     */
    @SuppressWarnings("unchecked")
    private PagingPredicateImpl(PagingPredicateImpl<K, V> originalPagingPredicate, Predicate<K, V> predicateReplacement) {
        this.anchorList = originalPagingPredicate.anchorList;
        this.comparator = originalPagingPredicate.comparator;
        this.pageSize = originalPagingPredicate.pageSize;
        this.page = originalPagingPredicate.page;
        this.iterationType = originalPagingPredicate.iterationType;
        this.userCodeNamespace = originalPagingPredicate.userCodeNamespace;
        setInnerPredicate(predicateReplacement);
    }

    @Override
    public Predicate accept(Visitor visitor, IndexRegistry indexes) {
        if (predicate instanceof VisitablePredicate visitablePredicate) {
            return NamespaceUtil.callWithOwnClassLoader(predicate, () -> {
                Predicate transformed = visitablePredicate.accept(visitor, indexes);
                return transformed == predicate ? this : new PagingPredicateImpl<>(this, transformed);
            });
        }
        return this;
    }

    /**
     * Sets an inner predicate.
     * throws {@link IllegalArgumentException} if inner predicate is also a {@link PagingPredicate}
     *
     * @param predicate the inner predicate through which results will be filtered
     */
    private void setInnerPredicate(Predicate<K, V> predicate) {
        Preconditions.checkNotInstanceOf(PagingPredicate.class, predicate, "Nested PagingPredicate is not supported!");
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

        Set<QueryableEntry<K, V>> set = NamespaceUtil.callWithNamespace(userCodeNamespace,
                () -> ((IndexAwarePredicate<K, V>) predicate).filter(queryContext));
        if (set == null || set.isEmpty()) {
            return set;
        }
        List<QueryableEntry<K, V>> resultList = new ArrayList<>();
        Map.Entry<Integer, Map.Entry> nearestAnchorEntry = getNearestAnchorEntry();
        for (QueryableEntry<K, V> queryableEntry : set) {
            if (SortingUtil.compareAnchor(this, queryableEntry, nearestAnchorEntry)) {
                resultList.add(queryableEntry);
            }
        }

        List<QueryableEntry<K, V>> sortedSubList =
                (List) SortingUtil.getSortedSubList((List) resultList, this, nearestAnchorEntry);
        return new LinkedHashSet<>(sortedSubList);
    }


    /**
     * Used if inner predicate is instanceof {@link IndexAwarePredicate} for checking if indexed.
     *
     * @param queryContext
     * @return
     */
    @Override
    public boolean isIndexed(QueryContext queryContext) {
        if (predicate instanceof IndexAwarePredicate awarePredicate) {
            return NamespaceUtil.callWithNamespace(userCodeNamespace, () ->
                    awarePredicate.isIndexed(queryContext));
        }
        return false;
    }

    /**
     * Used for delegating filtering to inner predicate.
     *
     * @param mapEntry
     * @return
     */
    public boolean apply(Map.Entry mapEntry) {
        if (predicate != null) {
            return NamespaceUtil.callWithNamespace(userCodeNamespace, () -> predicate.apply(mapEntry));
        }
        return true;
    }

    @Override
    public void reset() {
        iterationType = null;
        anchorList.clear();
        page = 0;
    }

    @Override
    public void nextPage() {
        page++;
    }

    @Override
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

    @Override
    public int getPage() {
        return page;
    }

    @Override
    public void setPage(int page) {
        this.page = page;
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    public Predicate<K, V> getPredicate() {
        return predicate;
    }

    @Override
    public Comparator<Map.Entry<K, V>> getComparator() {
        return comparator;
    }

    @Override
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
    public void setAnchor(int page, Map.Entry anchor) {
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

    public void setAnchorList(List<Map.Entry<Integer, Map.Entry<K, V>>> anchorList) {
        this.anchorList = anchorList;
    }

    public List<Map.Entry<Integer, Map.Entry<K, V>>> getAnchorList() {
        return anchorList;
    }

    public String getUserCodeNamespace() {
        return userCodeNamespace;
    }

    public Map.Entry<Integer, Map.Entry> getNearestAnchorEntry() {
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
        // RU_COMPAT_5_3
        if (out.getVersion().isGreaterOrEqual(V5_4)) {
            out.writeString(userCodeNamespace);
        }

        out.writeObject(predicate);
        out.writeObject(comparator);
        out.writeInt(page);
        out.writeInt(pageSize);
        out.writeString(iterationType.name());
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
        // RU_COMPAT_5_3
        if (in.getVersion().isGreaterOrEqual(V5_4)) {
            userCodeNamespace = in.readString();
        } else {
            userCodeNamespace = null;
        }

        NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
        NamespaceUtil.setupNamespace(engine, userCodeNamespace);
        try {
            predicate = in.readObject();
            comparator = in.readObject();
        } finally {
            NamespaceUtil.cleanupNamespace(engine, userCodeNamespace);
        }
        page = in.readInt();
        pageSize = in.readInt();
        iterationType = IterationType.valueOf(in.readString());
        int size = in.readInt();
        anchorList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            int anchorPage = in.readInt();
            Object anchorKey = in.readObject();
            Object anchorValue = in.readObject();
            Map.Entry anchorEntry = new SimpleImmutableEntry<>(anchorKey, anchorValue);
            anchorList.add(new SimpleImmutableEntry<>(anchorPage, anchorEntry));
        }
    }

    @Override
    public int getFactoryId() {
        return PREDICATE_DS_FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.PAGING_PREDICATE;
    }
}
