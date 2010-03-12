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
    private final MapIndex indexValue = new MapIndex(null, false, -1);
    private final Map<Expression, MapIndex> mapIndexes = new ConcurrentHashMap<Expression, MapIndex>(6);
    private volatile boolean hasIndexedAttributes = false;
    private volatile byte[] indexTypes = null;
    private final Object indexTypesLock = new Object();
    private Collection<MapIndex> indexesInOrder;

    public void index(Record record) {
        final long recordId = record.getId();
        if (!record.isActive()) {
            records.remove(recordId);
        } else {
            records.put(recordId, record);
        }
        indexValue.index(record.getValueHash(), record);
        long[] indexValues = record.getIndexes();
        byte[] indexTypes = record.getIndexTypes();
        if (indexValues != null && hasIndexedAttributes) {
            if (indexTypes == null || indexValues.length != indexTypes.length) {
                throw new IllegalArgumentException("index and types don't match " + indexTypes);
            }
            Collection<MapIndex> indexes = mapIndexes.values();
            for (MapIndex index : indexes) {
                if (indexValues.length > index.getAttributeIndex()) {
                    long newValue = indexValues[index.getAttributeIndex()];
                    index.index(newValue, record);
                }
            }
        }
    }

    public long[] getIndexValues(Object value) {
        if (hasIndexedAttributes) {
            int indexCount = mapIndexes.size();
            long[] newIndexes = new long[indexCount];
            if (value instanceof Data) {
                value = toObject((Data) value);
            }
            Collection<MapIndex> indexes = mapIndexes.values();
            for (MapIndex mapIndex : indexes) {
                int attributedIndex = mapIndex.getAttributeIndex();
                newIndexes[attributedIndex] = mapIndex.extractLongValue(value);
            }
            if (indexTypes == null || indexTypes.length != indexCount) {
                synchronized (indexTypesLock) {
                    if (indexTypes == null || indexTypes.length != indexCount) {
                        indexTypes = new byte[indexCount];
                        for (MapIndex mapIndex : indexes) {
                            int attributedIndex = mapIndex.getAttributeIndex();
                            indexTypes[attributedIndex] = mapIndex.getIndexType();
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

    public MapIndex addIndex(Expression expression, boolean ordered, int attributeIndex) {
        MapIndex index = mapIndexes.get(expression);
        if (index == null) {
            if (attributeIndex == -1) {
                attributeIndex = mapIndexes.size();
            }
            index = new MapIndex(expression, ordered, attributeIndex);
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
                    Set<MapIndex> setAppliedIndexes = new HashSet<MapIndex>(1);
                    iap.collectAppliedIndexes(setAppliedIndexes, mapIndexes);
                    if (setAppliedIndexes.size() > 0) {
                        for (MapIndex index : setAppliedIndexes) {
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
                        results = new HashSet<MapEntry>(sub.size());
                        for (MapEntry entry : sub) {
                            Record record = (Record) entry;
                            if (record.isActive()) {
                                results.add(record);
                            }
                        }
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
                    for (MapEntry entry : smallestSet) {
                        Record record = (Record) entry;
                        if (record.isActive()) {
                            results.add(record);
                        }
                    }
                    Iterator<MapEntry> it = results.iterator();
                    smallestLoop:
                    while (it.hasNext()) {
                        MapEntry entry = it.next();
                        for (Set<MapEntry> sub : lsSubResults) {
                            if (!sub.contains(entry)) {
                                it.remove();
                                continue smallestLoop;
                            }
                        }
                    }
                } else {
                    results = new HashSet<MapEntry>(records.size());
                    Collection<Record> colRecords = records.values();
                    for (MapEntry entry : colRecords) {
                        Record record = (Record) entry;
                        if (record.isActive()) {
                            results.add(record);
                        }
                    }
                }
            } else {
                results = new HashSet<MapEntry>(records.size());
                Collection<Record> colRecords = records.values();
                for (MapEntry entry : colRecords) {
                    Record record = (Record) entry;
                    if (record.isActive()) {
                        results.add(record);
                    }
                }
            }
        } finally {
            queryContext.setStrong(strong);
        }
        return results;
    }

    public Map<Expression, MapIndex> getIndexes() {
        return mapIndexes;
    }

    public boolean hasIndexedAttributes() {
        return hasIndexedAttributes;
    }

    MapIndex getIndexValue() {
        return indexValue;
    }

    public Map<Long, Record> getRecords() {
        return records;
    }

    public boolean containsValue(Data value) {
        return false;
    }

    public MapIndex[] getIndexesInOrder() {
        if (mapIndexes.size() ==0) return null;
        MapIndex[] indexes = new MapIndex[mapIndexes.size()];
        for (MapIndex index  : mapIndexes.values()) {
            indexes[index.getAttributeIndex()] = index;
        }
        return indexes;
    }
}
