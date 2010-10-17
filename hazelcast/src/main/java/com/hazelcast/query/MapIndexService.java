/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.query;

import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.Record;
import com.hazelcast.nio.Data;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.IOUtil.toObject;

public class MapIndexService {
    private final ConcurrentMap<Long, Record> records = new ConcurrentHashMap<Long, Record>(10000);
    private final Index indexValue;
    private final Map<Expression, Index> mapIndexes = new ConcurrentHashMap<Expression, Index>(2);
    private volatile boolean hasIndexedAttributes = false;
    private volatile byte[] indexTypes = null;
    private final Object indexTypesLock = new Object();

    public MapIndexService(boolean valueIndexed) {
        indexValue = (valueIndexed) ? new Index(null, false, -1) : null;
    }

    public void remove(Record record) {
        records.remove(record.getId());
    }

    public void index(Record record) {
        final Long recordId = record.getId();
        if (record.isActive()) {
            final Record anotherRecord = records.putIfAbsent(recordId, record);
            if (anotherRecord != null) {
                record = anotherRecord;
            }
        } else {
            remove(record);
        }
        Long newValueIndex = -1L;
        if (record.isActive() && record.getValue() != null) {
            newValueIndex = (long) record.getValue().hashCode();
        }
        if (indexValue != null) {
            indexValue.index(newValueIndex, record);
        }
        Long[] indexValues = record.getIndexes();
        byte[] indexTypes = record.getIndexTypes();
        if (indexValues != null && hasIndexedAttributes) {
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
            if (indexTypes == null || indexTypes.length != indexCount) {
                synchronized (indexTypesLock) {
                    if (indexTypes == null || indexTypes.length != indexCount) {
                        indexTypes = new byte[indexCount];
                        for (Index index : indexes) {
                            int attributedIndex = index.getAttributeIndex();
                            indexTypes[attributedIndex] = index.getIndexType();
                        }
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
        Set<MapEntry> results = null;
        Predicate predicate = queryContext.getPredicate();
        queryContext.setMapIndexes(mapIndexes);
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
                queryContext.setIndexedPredicateCount(lsIndexAwarePredicates.size());
                if (lsIndexAwarePredicates.size() == 1) {
                    IndexAwarePredicate indexAwarePredicate = lsIndexAwarePredicates.get(0);
                    Set<MapEntry> sub = indexAwarePredicate.filter(queryContext);
                    if (sub == null || sub.size() == 0) {
                        return null;
                    } else {
                        results = sub;
//                        results =  new HashSet<MapEntry>(sub.size());
//                        for (MapEntry entry : sub) {
//                            Record record = (Record) entry;
//                            if (record.isActive()) {
//                                results.add(record);
//                            }
//                        }
                    }
                } else if (lsIndexAwarePredicates.size() > 0) {
                    Set<MapEntry> smallestSet = null;
                    List<Set<MapEntry>> lsSubResults = new ArrayList<Set<MapEntry>>(lsIndexAwarePredicates.size());
                    for (IndexAwarePredicate indexAwarePredicate : lsIndexAwarePredicates) {
                        Set<MapEntry> sub = indexAwarePredicate.filter(queryContext);
                        if (sub == null) {
                            strong = false;
                        } else if (sub.size() == 0) {
                            strong = true;
                            return null;
                        } else {
                            if (smallestSet == null) {
                                smallestSet = sub;
                            } else {
                                if (sub.size() < smallestSet.size()) {
                                    lsSubResults.add(smallestSet);
                                    smallestSet = sub;
                                } else {
                                    lsSubResults.add(sub);
                                }
                            }
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
                } else {
                    results = new SingleResultSet(records);
                }
            } else {
                results = new SingleResultSet(records);
            }
        } finally {
            queryContext.setStrong(strong);
        }
        return results;
    }

    public Map<Expression, Index> getIndexes() {
        return mapIndexes;
    }

    public boolean hasIndexedAttributes() {
        return hasIndexedAttributes;
    }

    public boolean containsValue(Data value) {
        if (indexValue != null) {
            Set<MapEntry> results = indexValue.getRecords((long)value.hashCode());
            if (results == null || results.size() == 0) return false;
            for (MapEntry entry : results) {
                Record record = (Record) entry;
                if (record.containsValue(value)) {
                    return true;
                }
            }
        } else {
            for (Record record : records.values()) {
                if (record.containsValue(value)) {
                    return true;
                }
            }
        }
        return false;
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
        sbState.append("\nIndex- records: " + records.size() + ", mapIndexes:"
                + mapIndexes.size() + ", indexTypes:" + ((indexTypes == null) ? 0 : indexTypes.length));
        for (Index index : mapIndexes.values()) {
            index.appendState(sbState);
        }
    }

    public void clear() {
        mapIndexes.clear();
        records.clear();
    }

    public int size() {
        return records.size(); //indexValue.getRecordValues().size();
    }
}
