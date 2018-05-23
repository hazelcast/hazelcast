package com.hazelcast.query.impl;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.monitor.impl.InternalIndexStats;
import com.hazelcast.nio.serialization.Data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Extends the basic query context to support the per-index stats tracking on
 * behalf of global indexes.
 */
public class GlobalQueryContextWithStats extends QueryContext {

    private final HashMap<String, QueryTrackingIndex> knownIndexes = new HashMap<String, QueryTrackingIndex>();

    private final HashSet<QueryTrackingIndex> trackedIndexes = new HashSet<QueryTrackingIndex>(8);

    @Override
    void attachTo(Indexes indexes) {
        super.attachTo(indexes);
        for (QueryTrackingIndex trackedIndex : trackedIndexes) {
            trackedIndex.resetPerQueryStats();
        }
        trackedIndexes.clear();
    }

    @Override
    public Index getIndex(String attributeName) {
        if (indexes == null) {
            return null;
        }

        InternalIndex delegate = indexes.getIndex(attributeName);
        if (delegate == null) {
            return null;
        }

        QueryTrackingIndex trackingIndex = knownIndexes.get(attributeName);
        if (trackingIndex == null) {
            trackingIndex = new QueryTrackingIndex();
            knownIndexes.put(attributeName, trackingIndex);
        }

        trackingIndex.attachTo(delegate);
        trackedIndexes.add(trackingIndex);

        return trackingIndex;
    }

    @Override
    void applyPerQueryStats() {
        for (QueryTrackingIndex trackedIndex : trackedIndexes) {
            trackedIndex.incrementQueryCount();
        }
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
                delegate.getIndexStats().incrementQueryCount();
            }
        }

        @Override
        public String getAttributeName() {
            return delegate.getAttributeName();
        }

        @Override
        public boolean isOrdered() {
            return delegate.isOrdered();
        }

        @Override
        public void saveEntryIndex(QueryableEntry entry, Object oldValue) {
            delegate.saveEntryIndex(entry, oldValue);
        }

        @Override
        public void removeEntryIndex(Data key, Object value) {
            delegate.removeEntryIndex(key, value);
        }

        @Override
        public TypeConverter getConverter() {
            return delegate.getConverter();
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
        public Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to) {
            Set<QueryableEntry> result = delegate.getSubRecordsBetween(from, to);
            hasQueries = true;
            return result;
        }

        @Override
        public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue) {
            Set<QueryableEntry> result = delegate.getSubRecords(comparisonType, searchedValue);
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
        public InternalIndexStats getIndexStats() {
            return delegate.getIndexStats();
        }

    }

}
