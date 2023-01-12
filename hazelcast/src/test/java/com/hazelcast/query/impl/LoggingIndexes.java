/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.core.TypeConverter;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.query.impl.getters.Extractors;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

// decorates Indexes classes with logging
public class LoggingIndexes extends Indexes {

    private final ILogger logger;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public LoggingIndexes(Node node,
                    String mapName,
                    InternalSerializationService ss,
                    IndexCopyBehavior indexCopyBehavior,
                    Extractors extractors,
                    IndexProvider indexProvider,
                    boolean usesCachedQueryableEntries,
                    boolean statisticsEnabled,
                    boolean global,
                    InMemoryFormat inMemoryFormat,
                    int partitionCount,
                    Supplier<Predicate<QueryableEntry>> resultFilterFactory) {
        super(node, mapName, ss, indexCopyBehavior, extractors, indexProvider,
                usesCachedQueryableEntries, statisticsEnabled, global, inMemoryFormat,
                partitionCount, resultFilterFactory);
        logger = node.getLogger(LoggingIndexes.class);
    }

    @Override
    public InternalIndex[] getIndexes() {
        InternalIndex[] indexes = super.getIndexes();
        return Arrays.stream(indexes).map(LoggingInternalIndex::new)
                .collect(Collectors.toList()).toArray(new InternalIndex[0]);
    }

    private class LoggingInternalIndex implements InternalIndex {
        private final InternalIndex delegate;

        LoggingInternalIndex(InternalIndex delegate) {
            this.delegate = delegate;
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
        public void putEntry(CachedQueryEntry newEntry, CachedQueryEntry oldEntry, QueryableEntry entryToStore, OperationSource operationSource) {
            delegate.putEntry(newEntry, oldEntry, entryToStore, operationSource);
        }

        @Override
        public void removeEntry(CachedQueryEntry entry, OperationSource operationSource) {
            delegate.removeEntry(entry, operationSource);
        }

        @Override
        public boolean isEvaluateOnly() {
            return delegate.isEvaluateOnly();
        }

        @Override
        public boolean canEvaluate(Class<? extends com.hazelcast.query.Predicate> predicateClass) {
            return delegate.canEvaluate(predicateClass);
        }

        @Override
        public Set<QueryableEntry> evaluate(com.hazelcast.query.Predicate predicate) {
            return delegate.evaluate(predicate);
        }

        @Override
        public Iterator<QueryableEntry> getSqlRecordIterator(boolean descending) {
            return delegate.getSqlRecordIterator(descending);
        }

        @Override
        public Iterator<QueryableEntry> getSqlRecordIterator(Comparable value) {
            return delegate.getSqlRecordIterator(value);
        }

        @Override
        public Iterator<QueryableEntry> getSqlRecordIterator(Comparison comparison, Comparable value, boolean descending) {
            return delegate.getSqlRecordIterator(comparison, value, descending);
        }

        @Override
        public Iterator<QueryableEntry> getSqlRecordIterator(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive, boolean descending) {
            return delegate.getSqlRecordIterator(from, fromInclusive, to, toInclusive, descending);
        }

        @Override
        public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(Comparable value) {
            return delegate.getSqlRecordIteratorBatch(value);
        }

        @Override
        public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(boolean descending) {
            return delegate.getSqlRecordIteratorBatch(descending);
        }

        @Override
        public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(Comparison comparison, Comparable value, boolean descending) {
            return delegate.getSqlRecordIteratorBatch(comparison, value, descending);
        }

        @Override
        public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive, boolean descending) {
            return delegate.getSqlRecordIteratorBatch(from, fromInclusive, to, toInclusive, descending);
        }

        @Override
        public Set<QueryableEntry> getRecords(Comparable value) {
            return delegate.getRecords(value);
        }

        @Override
        public Set<QueryableEntry> getRecords(Comparable[] values) {
            return delegate.getRecords(values);
        }

        @Override
        public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
            return delegate.getRecords(from, fromInclusive, to, toInclusive);
        }

        @Override
        public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
            return delegate.getRecords(comparison, value);
        }

        @Override
        public void clear() {
            logger.info("Clear index " + delegate.getName());
            delegate.clear();
        }

        @Override
        public void destroy() {
            logger.info("Destroy index " + delegate.getName());
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
        public void beginPartitionUpdate() {
            logger.info("beginPartitionUpdate for index " + delegate.getName());
            delegate.beginPartitionUpdate();
        }

        @Override
        public void markPartitionAsIndexed(int partitionId) {
            logger.info("markPartitionAsIndexed for index " + delegate.getName()
                            + ", partitionId: " + partitionId);
            delegate.markPartitionAsIndexed(partitionId);
        }

        @Override
        public void markPartitionAsUnindexed(int partitionId) {
            logger.info("markPartitionAsUnindexed for index " + delegate.getName()
                    + ", partitionId: " + partitionId);
            delegate.markPartitionAsUnindexed(partitionId);
        }

        @Override
        public PerIndexStats getPerIndexStats() {
            return delegate.getPerIndexStats();
        }

        @Override
        public GlobalIndexPartitionTracker.PartitionStamp getPartitionStamp() {
            return delegate.getPartitionStamp();
        }

        @Override
        public boolean validatePartitionStamp(long stamp) {
            return delegate.validatePartitionStamp(stamp);
        }
    }
}
