/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Extends the basic query context to support the per-index stats tracking on
 * behalf of global indexes.
 */
public class GlobalQueryContextWithStats extends QueryContext {

    private final HashMap<String, QueryTrackingIndex> knownIndexes = new HashMap<>();

    private final HashSet<QueryTrackingIndex> trackedIndexes = new HashSet<>(8);

    @Override
    void attachTo(Indexes indexes, int ownedPartitionCount) {
        super.attachTo(indexes, ownedPartitionCount);
        for (QueryTrackingIndex trackedIndex : trackedIndexes) {
            trackedIndex.resetPerQueryStats();
        }
        trackedIndexes.clear();
    }

    @Override
    void applyPerQueryStats() {
        for (QueryTrackingIndex trackedIndex : trackedIndexes) {
            trackedIndex.incrementQueryCount();
        }
    }

    @Override
    public Index matchIndex(String pattern, IndexMatchHint matchHint) {
        InternalIndex delegate = indexes.matchIndex(pattern, matchHint, ownedPartitionCount);
        if (delegate == null) {
            return null;
        }

        QueryTrackingIndex trackingIndex = knownIndexes.get(pattern);
        if (trackingIndex == null) {
            trackingIndex = new QueryTrackingIndex();
            knownIndexes.put(pattern, trackingIndex);
        }

        trackingIndex.attachTo(delegate);
        trackedIndexes.add(trackingIndex);

        return trackingIndex;
    }

    private static class QueryTrackingIndex implements InternalIndex {

        private InternalIndex delegate;

        private boolean hasQueries;

        public void attachTo(InternalIndex delegate) {
            this.delegate = delegate;
        }

        public void resetPerQueryStats() {
            hasQueries = false;
        }

        public void incrementQueryCount() {
            if (hasQueries) {
                delegate.getPerIndexStats().incrementQueryCount();
            }
        }

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public String[] getComponents() {
            return delegate.getComponents();
        }

        @Override
        public IndexConfig getConfig() {
            return delegate.getConfig();
        }

        @Override
        public boolean isOrdered() {
            return delegate.isOrdered();
        }

        @Override
        public TypeConverter getConverter() {
            return delegate.getConverter();
        }

        @Override
        public void putEntry(QueryableEntry entry, Object oldValue, OperationSource operationSource) {
            delegate.putEntry(entry, oldValue, operationSource);
        }

        @Override
        public void removeEntry(Data key, Object value, OperationSource operationSource) {
            delegate.removeEntry(key, value, operationSource);
        }

        @Override
        public boolean isEvaluateOnly() {
            return delegate.isEvaluateOnly();
        }

        @Override
        public boolean canEvaluate(Class<? extends Predicate> predicateClass) {
            return delegate.canEvaluate(predicateClass);
        }

        @Override
        public Set<QueryableEntry> evaluate(Predicate predicate) {
            Set<QueryableEntry> result = delegate.evaluate(predicate);
            hasQueries = true;
            return result;
        }

        @Override
        public Set<QueryableEntry> getRecords(Comparable value) {
            Set<QueryableEntry> result = delegate.getRecords(value);
            hasQueries = true;
            return result;
        }

        @Override
        public Set<QueryableEntry> getRecords(Comparable[] values) {
            Set<QueryableEntry> result = delegate.getRecords(values);
            hasQueries = true;
            return result;
        }

        @Override
        public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
            Set<QueryableEntry> result = delegate.getRecords(from, fromInclusive, to, toInclusive);
            hasQueries = true;
            return result;
        }

        @Override
        public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
            Set<QueryableEntry> result = delegate.getRecords(comparison, value);
            hasQueries = true;
            return result;
        }

        @Override
        public void clear() {
            delegate.clear();
        }

        @Override
        public void destroy() {
            delegate.destroy();
        }

        @Override
        public Comparable canonicalizeQueryArgumentScalar(Comparable value) {
            return delegate.canonicalizeQueryArgumentScalar(value);
        }

        @Override
        public boolean hasPartitionIndexed(int partitionId) {
            return delegate.hasPartitionIndexed(partitionId);
        }

        @Override
        public boolean allPartitionsIndexed(int ownedPartitionCount) {
            return delegate.allPartitionsIndexed(ownedPartitionCount);
        }

        @Override
        public void markPartitionAsIndexed(int partitionId) {
            delegate.markPartitionAsIndexed(partitionId);
        }

        @Override
        public void markPartitionAsUnindexed(int partitionId) {
            delegate.markPartitionAsUnindexed(partitionId);
        }

        @Override
        public PerIndexStats getPerIndexStats() {
            return delegate.getPerIndexStats();
        }

    }

}
