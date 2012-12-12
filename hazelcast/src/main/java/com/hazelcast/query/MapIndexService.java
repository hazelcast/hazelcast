/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.Record;
import com.hazelcast.nio.Data;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.nio.IOUtil.toObject;

public class MapIndexService {
    private final ConcurrentMap<Long, Record> records = new ConcurrentHashMap<Long, Record>(10000, 0.75f, 1);
    private final Index indexValue;
    private final Map<Expression, Index> mapIndexes = new ConcurrentHashMap<Expression, Index>(4, 0.75f, 1);
    private final Object indexTypesLock = new Object();
    private volatile boolean hasIndexedAttributes = false;
    @SuppressWarnings("VolatileArrayField")
    private volatile byte[] indexTypes = null;
    private final AtomicInteger size = new AtomicInteger(0);

    public MapIndexService(boolean valueIndexed) {
        indexValue = (valueIndexed) ? new Index(null, false, -1) : null;
    }

    public void remove(Record record) {
        Record existingRecord = records.remove(record.getId());
        if (existingRecord != null) {
            for (Index index : mapIndexes.values()) {
                index.removeRecordIndex(record.getIndexes()[index.getAttributeIndex()], record.getId());
            }
            size.decrementAndGet();
        }
    }

    public void index(Record record) {
        final Long recordId = record.getId();
        if (record.isActive()) {
            final Record anotherRecord = records.putIfAbsent(recordId, record);
            if (anotherRecord != null) {
                record = anotherRecord;
            } else {
                size.incrementAndGet();
            }
        } else {
            remove(record);
        }
        if (indexValue != null) {
            Long newValueIndex = -1L;
            if (record.isActive() && record.hasValueData()) {
                newValueIndex = (long) record.getValueData().hashCode();
            }
            indexValue.index(newValueIndex, record);
        }
        Long[] indexValues = record.getIndexes();
        if (indexValues != null && hasIndexedAttributes) {
            byte[] indexTypes = record.getIndexTypes();
            if (indexTypes == null || indexValues.length != indexTypes.length) {
                throw new IllegalArgumentException("index and types don't match " + Arrays.toString(indexTypes));
            }
            Collection<Index> indexes = mapIndexes.values();
            for (Index index : indexes) {
                if (indexValues.length > index.getAttributeIndex()) {
                    Long newValue = indexValues[index.getAttributeIndex()];
                    index.index(newValue, record);
                }
            }
        }
    }

    public Collection<Record> getOwnedRecords() {
        return records.values();
    }

    public Long[] getIndexValues(Object value) {
        if (hasIndexedAttributes) {
            int indexCount = mapIndexes.size();
            Long[] newIndexes = new Long[indexCount];
            if (value instanceof Data) {
                value = toObject((Data) value);
            }
            Collection<Index> indexes = mapIndexes.values();
            for (Index index : indexes) {
                int attributedIndex = index.getAttributeIndex();
                newIndexes[attributedIndex] = index.extractLongValue(value);
            }
            byte[] _indexTypes = indexTypes;
            if (_indexTypes == null || _indexTypes.length != indexCount) {
                synchronized (indexTypesLock) {
                    _indexTypes = indexTypes;
                    if (_indexTypes == null || _indexTypes.length != indexCount) {
                        _indexTypes = new byte[indexCount];
                        for (Index index : indexes) {
                            int attributedIndex = index.getAttributeIndex();
                            _indexTypes[attributedIndex] = index.getIndexType();
                        }
                        indexTypes = _indexTypes;
                    }
                }
            }
            return newIndexes;
        }
        return null;
    }

    public byte[] getIndexTypes() {
        return indexTypes;
    }

    public Index addIndex(Expression expression, boolean ordered, int attributeIndex) {
        Index index = mapIndexes.get(expression);
        if (index == null) {
            if (size() > 0) {
                StringBuilder sb = new StringBuilder("Index can only be added before adding entries!");
                sb.append("\n");
                sb.append("Add indexes first and only once then put entries.");
                throw new RuntimeException(sb.toString());
            }
            if (attributeIndex == -1) {
                attributeIndex = mapIndexes.size();
            }
            index = new Index(expression, ordered, attributeIndex);
            mapIndexes.put(expression, index);
            indexTypes = null;
            //todo build the indexes
            hasIndexedAttributes = true;
        }
        return index;
    }

    public Set<MapEntry> doQuery(QueryContext queryContext) {
        boolean strong = false;
        Set<MapEntry> results;
        Predicate predicate = queryContext.getPredicate();
        try {
            if (predicate != null && mapIndexes != null && predicate instanceof IndexAwarePredicate) {
                List<IndexAwarePredicate> lsIndexAwarePredicates = new ArrayList<IndexAwarePredicate>();
                IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
                strong = iap.collectIndexAwarePredicates(lsIndexAwarePredicates, mapIndexes);
                if (strong) {
                    Set<Index> setAppliedIndexes = new HashSet<Index>(1);
                    iap.collectAppliedIndexes(setAppliedIndexes, mapIndexes);
                    if (setAppliedIndexes.size() > 0) {
                        for (Index index : setAppliedIndexes) {
                            if (strong) {
                                strong = index.isStrong();
                            }
                        }
                    }
                }
                int indexAwarePredicateCount = lsIndexAwarePredicates.size();
                if (indexAwarePredicateCount == 1) {
                    IndexAwarePredicate indexAwarePredicate = lsIndexAwarePredicates.get(0);
                    Set<MapEntry> sub = indexAwarePredicate.filter(queryContext);
                    if (sub == null || sub.size() == 0) {
                        return null;
                    } else {
                        return sub;
                    }
                } else if (indexAwarePredicateCount > 0) {
                    IndexAwarePredicate indexAwarePredicateFirst = lsIndexAwarePredicates.get(0);
                    Set<MapEntry> subFirst = indexAwarePredicateFirst.filter(queryContext);
                    if (subFirst != null && subFirst.size() < 11) {
                        strong = true;
                        Set<MapEntry> resultSet = new HashSet<MapEntry>(subFirst);
                        for (int i = 1; i < lsIndexAwarePredicates.size(); i++) {
                            IndexAwarePredicate p = lsIndexAwarePredicates.get(i);
                            Iterator<MapEntry> it = resultSet.iterator();
                            while (it.hasNext()) {
                                Record record = (Record) it.next();
                                if (!p.apply(record)) {
                                    it.remove();
                                }
                            }
                        }
                        return resultSet;
                    } else if (subFirst != null) {
                        List<Set<MapEntry>> lsSubResults = new ArrayList<Set<MapEntry>>(indexAwarePredicateCount);
                        lsSubResults.add(subFirst);
                        Set<MapEntry> smallestSet = subFirst;
                        for (int i = 1; i < indexAwarePredicateCount; i++) {
                            IndexAwarePredicate p = lsIndexAwarePredicates.get(i);
                            Set<MapEntry> sub = p.filter(queryContext);
                            if (sub == null) {
                                strong = false;
                            } else if (sub.size() == 0) {
                                strong = true;
                                return null;
                            } else {
                                if (sub.size() < smallestSet.size()) {
                                    smallestSet = sub;
                                }
                                lsSubResults.add(sub);
                            }
                        }
                        if (smallestSet == null) {
                            return null;
                        }
                        results = new HashSet<MapEntry>(smallestSet.size());
                        Iterator<MapEntry> it = smallestSet.iterator();
                        smallestLoop:
                        while (it.hasNext()) {
                            MapEntry entry = it.next();
                            for (Set<MapEntry> sub : lsSubResults) {
                                if (!sub.contains(entry)) {
                                    continue smallestLoop;
                                }
                            }
                            results.add(entry);
                        }
                        return results;
                    }
                }
            }
            // no matching condition yet!
            // return everything.
            return new SingleResultSet(records);
        } finally {
            queryContext.setStrong(strong);
        }
    }

    public Map<Expression, Index> getIndexes() {
        return mapIndexes;
    }

    public boolean hasIndexedAttributes() {
        return hasIndexedAttributes;
    }

    public Index getValueIndex() {
        return indexValue;
    }

    public Index[] getIndexesInOrder() {
        if (mapIndexes.size() == 0) return null;
        Index[] indexes = new Index[mapIndexes.size()];
        for (Index index : mapIndexes.values()) {
            indexes[index.getAttributeIndex()] = index;
        }
        return indexes;
    }

    public void appendState(StringBuffer sbState) {
        byte[] _indexTypes = indexTypes;
        sbState.append("\nIndex- records: " + records.size() + ", mapIndexes:"
                + mapIndexes.size() + ", indexTypes:" + ((_indexTypes == null) ? 0 : _indexTypes.length));
        for (Index index : mapIndexes.values()) {
            index.appendState(sbState);
        }
    }

    public void clear() {
        mapIndexes.clear();
        records.clear();
        size.set(0);
    }

    public int size() {
        return size.get();
    }
}
