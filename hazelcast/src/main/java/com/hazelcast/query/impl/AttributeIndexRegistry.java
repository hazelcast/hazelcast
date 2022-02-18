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

package com.hazelcast.query.impl;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.GlobalIndexPartitionTracker.PartitionStamp;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static com.hazelcast.query.impl.Comparison.GREATER;
import static com.hazelcast.query.impl.Comparison.GREATER_OR_EQUAL;
import static com.hazelcast.query.impl.CompositeValue.NEGATIVE_INFINITY;
import static com.hazelcast.query.impl.CompositeValue.POSITIVE_INFINITY;
import static java.util.Collections.emptySet;

/**
 * Maintains a registry of single-attribute indexes.
 * <p>
 * The main purpose of this class is to maintain decorators around ordered
 * composite indexes to allow transparent querying on their first components.
 */
public class AttributeIndexRegistry {

    private final ConcurrentMap<String, Record> registry = new ConcurrentHashMap<>();

    /**
     * Registers the given index in this registry.
     * <p>
     * This method expects external thread access synchronization applied, so
     * there is no more than one writer at any given time.
     *
     * @param index the index to register.
     * @see Indexes#addOrGetIndex
     */
    public void register(InternalIndex index) {
        String[] components = index.getComponents();
        String attribute = components[0];

        Record record = registry.get(attribute);
        if (record == null) {
            record = new Record();
            registry.put(attribute, record);
        }

        if (index.isOrdered()) {
            if (record.orderedWorseThan(index)) {
                record.ordered = components.length == 1 ? index : new FirstComponentDecorator(index);
            }
        } else {
            if (record.unorderedWorseThan(index)) {
                record.unordered = index;
            }
        }
    }

    /**
     * Matches an index for the given attribute and match hint.
     *
     * @param attribute the attribute to match an index for.
     * @param matchHint the match hint; {@link QueryContext.IndexMatchHint#EXACT_NAME}
     *                  is not supported by this method.
     * @return the matched index or {@code null} if nothing matched.
     * @see QueryContext.IndexMatchHint
     */
    public InternalIndex match(String attribute, QueryContext.IndexMatchHint matchHint) {
        Record record = registry.get(attribute);
        if (record == null) {
            return null;
        }

        switch (matchHint) {
            case NONE:
                // Intentional fallthrough. We still prefer ordered indexes
                // under the cover since they are more universal in terms of
                // supported fast queries.
            case PREFER_ORDERED:
                InternalIndex ordered = record.ordered;
                return ordered == null ? record.unordered : ordered;
            case PREFER_UNORDERED:
                InternalIndex unordered = record.unordered;
                return unordered == null ? record.ordered : unordered;
            default:
                throw new IllegalStateException("unexpected match hint: " + matchHint);
        }
    }

    /**
     * Clears this registry by unregistering all previously registered indexes.
     */
    public void clear() {
        registry.clear();
    }

    private static class Record {

        // It's enough to have volatile non-atomic mutations here, just to ensure
        // the visibility, since only a single thread may mutate a record at any
        // given time. All invocations are coming from Indexes.addOrGetIndex
        // which is synchronized.

        volatile InternalIndex unordered;

        volatile InternalIndex ordered;

        public boolean unorderedWorseThan(InternalIndex candidate) {
            assert !candidate.isOrdered();

            // we have no index and the unordered candidate is not composite
            return unordered == null && candidate.getComponents().length == 1;
        }

        public boolean orderedWorseThan(InternalIndex candidate) {
            assert candidate.isOrdered();
            InternalIndex current = ordered;

            if (current == null) {
                // any ordered index is better than nothing
                return true;
            }

            if (current instanceof FirstComponentDecorator) {
                // the current index is composite

                String[] candidateComponents = candidate.getComponents();
                if (candidateComponents.length > 1) {
                    // if the current index has more components, replace it
                    FirstComponentDecorator currentDecorator = (FirstComponentDecorator) current;
                    return currentDecorator.width > candidateComponents.length;
                }

                // any non-composite candidate is better than composite index
                return true;
            }

            return false;
        }

    }

    /**
     * Decorates first component of ordered composite indexes to allow
     * transparent querying on it
     * <p>
     * Exposed as a package-private class only for testing purposes.
     */
    @SuppressWarnings({"rawtypes", "checkstyle:MethodCount"})
    static final class FirstComponentDecorator implements InternalIndex {

        // See CompositeValue docs for more details on what is going on in the
        // index querying methods.

        // exposed as a package-private field only for testing purposes
        final InternalIndex delegate;

        private final int width;
        private final String[] components;

        FirstComponentDecorator(InternalIndex delegate) {
            assert delegate.getComponents().length > 1;
            assert delegate.isOrdered();
            this.delegate = delegate;
            this.width = delegate.getComponents().length;

            components = new String[]{delegate.getComponents()[0]};
        }

        @Override
        public String getName() {
            throw newUnsupportedException();
        }

        @Override
        public String[] getComponents() {
            return components;
        }

        @Override
        public IndexConfig getConfig() {
            throw newUnsupportedException();
        }

        @Override
        public boolean isOrdered() {
            return delegate.isOrdered();
        }

        @Override
        public TypeConverter getConverter() {
            CompositeConverter converter = (CompositeConverter) delegate.getConverter();
            return converter == null ? null : converter.getComponentConverter(0);
        }

        @Override
        public void putEntry(CachedQueryEntry newEntry, CachedQueryEntry oldEntry, QueryableEntry entryToStore,
                             OperationSource operationSource) {
            throw newUnsupportedException();
        }

        @Override
        public void removeEntry(CachedQueryEntry entry, OperationSource operationSource) {
            throw newUnsupportedException();
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
            return delegate.evaluate(predicate);
        }

        @Override
        public Iterator<QueryableEntry> getSqlRecordIterator(boolean descending) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public Iterator<QueryableEntry> getSqlRecordIterator(Comparable value) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public Iterator<QueryableEntry> getSqlRecordIterator(Comparison comparison, Comparable value, boolean descending) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public Iterator<QueryableEntry> getSqlRecordIterator(
            Comparable from,
            boolean fromInclusive,
            Comparable to,
            boolean toInclusive,
            boolean descending
        ) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(Comparable value) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(boolean descending) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(
                Comparison comparison,
                Comparable value,
                boolean descending
        ) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(
                Comparable from,
                boolean fromInclusive,
                Comparable to,
                boolean toInclusive,
                boolean descending
        ) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public Set<QueryableEntry> getRecords(Comparable value) {
            Comparable from = new CompositeValue(width, value, NEGATIVE_INFINITY);
            Comparable to = new CompositeValue(width, value, POSITIVE_INFINITY);
            return delegate.getRecords(from, false, to, false);
        }

        @SuppressWarnings("checkstyle:npathcomplexity")
        @Override
        public Set<QueryableEntry> getRecords(Comparable[] values) {
            if (values.length == 0) {
                return emptySet();
            }

            TypeConverter converter = getConverter();
            if (converter == null) {
                return emptySet();
            }

            if (values.length == 1) {
                return getRecords(values[0]);
            }

            Set<Comparable> convertedValues = new HashSet<>();
            for (Comparable value : values) {
                Comparable converted = converter.convert(value);
                convertedValues.add(canonicalizeQueryArgumentScalar(converted));
            }

            if (convertedValues.size() == 1) {
                return getRecords(convertedValues.iterator().next());
            }

            Set<QueryableEntry> result = new HashSet<>();
            for (Comparable value : convertedValues) {
                result.addAll(getRecords(value));
            }

            return result;
        }

        @Override
        public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
            Comparable compositeFrom = new CompositeValue(width, from, fromInclusive ? NEGATIVE_INFINITY : POSITIVE_INFINITY);
            Comparable compositeTo = new CompositeValue(width, to, toInclusive ? POSITIVE_INFINITY : NEGATIVE_INFINITY);
            return delegate.getRecords(compositeFrom, false, compositeTo, false);
        }

        @Override
        public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
            switch (comparison) {
                case LESS:
                    CompositeValue lessFrom = new CompositeValue(width, NULL, POSITIVE_INFINITY);
                    CompositeValue lessTo = new CompositeValue(width, value, NEGATIVE_INFINITY);
                    return delegate.getRecords(lessFrom, false, lessTo, false);
                case GREATER:
                    return delegate.getRecords(GREATER, new CompositeValue(width, value, POSITIVE_INFINITY));
                case LESS_OR_EQUAL:
                    CompositeValue greaterOrEqualFrom = new CompositeValue(width, NULL, POSITIVE_INFINITY);
                    CompositeValue greaterOrEqualTo = new CompositeValue(width, value, POSITIVE_INFINITY);
                    return delegate.getRecords(greaterOrEqualFrom, false, greaterOrEqualTo, false);
                case GREATER_OR_EQUAL:
                    return delegate.getRecords(GREATER_OR_EQUAL, new CompositeValue(width, value, NEGATIVE_INFINITY));
                default:
                    throw new IllegalStateException("unexpected comparison: " + comparison);
            }
        }

        @Override
        public void clear() {
            throw newUnsupportedException();
        }

        @Override
        public void destroy() {
            throw newUnsupportedException();
        }

        @Override
        public Comparable canonicalizeQueryArgumentScalar(Comparable value) {
            return delegate.canonicalizeQueryArgumentScalar(value);
        }

        @Override
        public boolean hasPartitionIndexed(int partitionId) {
            throw newUnsupportedException();
        }

        @Override
        public boolean allPartitionsIndexed(int ownedPartitionCount) {
            return delegate.allPartitionsIndexed(ownedPartitionCount);
        }

        @Override
        public void beginPartitionUpdate() {
            throw newUnsupportedException();
        }

        @Override
        public void markPartitionAsIndexed(int partitionId) {
            throw newUnsupportedException();
        }

        @Override
        public void markPartitionAsUnindexed(int partitionId) {
            throw newUnsupportedException();
        }

        @Override
        public PerIndexStats getPerIndexStats() {
            return delegate.getPerIndexStats();
        }

        @Override
        public PartitionStamp getPartitionStamp() {
            throw newUnsupportedException();
        }

        @Override
        public boolean validatePartitionStamp(long stamp) {
            throw newUnsupportedException();
        }

        private RuntimeException newUnsupportedException() {
            return new UnsupportedOperationException("decorated composite indexes support only querying");
        }

    }

}
