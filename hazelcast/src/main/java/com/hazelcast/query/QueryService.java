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

import com.hazelcast.core.Instance;
import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.*;
import com.hazelcast.nio.Data;
import com.hazelcast.util.ResponseQueueFactory;
import com.hazelcast.util.UnboundedBlockingQueue;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryService implements Runnable {

    private final Logger logger = Logger.getLogger(QueryService.class.getName());

    private volatile boolean running = true;

    private final Node node;

    private final BlockingQueue<Runnable> queryQ = new UnboundedBlockingQueue<Runnable>();

    private final Map<String, IndexRegion> regions = new HashMap<String, IndexRegion>(10);

    private long lastStatePublishTime = 0;

    public QueryService(Node node) {
        this.node = node;
    }

    public void run() {
        while (running) {
            Runnable runnable;
            try {
                runnable = queryQ.poll(1, TimeUnit.SECONDS);
                long now = System.currentTimeMillis();
                if (now - lastStatePublishTime > 1000) {
                    publishState();
                    lastStatePublishTime = now;
                }
                if (runnable != null) {
                    runnable.run();
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        try {
            final CountDownLatch l = new CountDownLatch(1);
            queryQ.put(new Runnable() {
                public void run() {
                    running = false;
                    regions.clear();
                    l.countDown();
                }
            });
            l.await();
        } catch (InterruptedException ignored) {
        }
    }

    void publishState() {
        QueryServiceState queryServiceState = new QueryServiceState(node.concurrentMapManager);
        queryServiceState.setQueueSize(queryQ.size());
        Collection<IndexRegion> colRegions = regions.values();
        for (IndexRegion region : colRegions) {
            QueryServiceState.IndexState[] indexStates = null;
            if (region.indexes != null) {
                int size = region.indexes.length;
                indexStates = new QueryServiceState.IndexState[size];
                for (int i = 0; i < size; i++) {
                    Index index = region.indexes[i];
                    indexStates[i] = new QueryServiceState.IndexState(index.mapIndex.size());
                }
            }
            QueryServiceState.IndexRegionState state = new QueryServiceState.IndexRegionState
                    (region.name, region.ownedRecords.size(), region.mapValueIndex.size(), indexStates);
            queryServiceState.addIndexRegionState(state);
        }
        node.concurrentMapManager.enqueueAndReturn(queryServiceState);
    }

    class IndexRegion {
        final String name;
        final Set<Record> ownedRecords = new HashSet<Record>(CMap.DEFAULT_MAP_SIZE);
        final Map<Integer, Set<Record>> mapValueIndex = new HashMap<Integer, Set<Record>>(1000);
        private Map<Expression, Index<MapEntry>> mapIndexes = null;
        private Index<MapEntry>[] indexes = null;
        final Instance.InstanceType instanceType;

        IndexRegion(String name) {
            this.name = name;
            this.instanceType = BaseManager.getInstanceType(name);
        }

        void reset() {
            ownedRecords.clear();
            mapValueIndex.clear();
            if (mapIndexes != null) {
                mapIndexes.clear();
            }
        }

        void addNewValueIndex(int newValueHash, Record record) {
            if (instanceType != Instance.InstanceType.MAP) return;
            Set<Record> lsRecords = mapValueIndex.get(newValueHash);
            if (lsRecords == null) {
                lsRecords = new LinkedHashSet<Record>();
                mapValueIndex.put(newValueHash, lsRecords);
            }
            lsRecords.add(record);
        }

        @Override
        public String toString() {
            return "IndexRegion{" +
                    "name='" + name + '\'' +
                    ", ownedRecords='" + ownedRecords.size() + '\'' +
                    ", mapValueIndexes='" + mapValueIndex.size() + '\'' +
                    '}';
        }

        void updateValueIndex(int newValueHash, Record record) {
            if (instanceType != Instance.InstanceType.MAP) return;
            int oldHash = record.getValueHash();
            if (oldHash == Integer.MIN_VALUE) {
                addNewValueIndex(newValueHash, record);
            } else {
                if (oldHash != newValueHash) {
                    removeValueIndex(record);
                    addNewValueIndex(newValueHash, record);
                }
            }
            record.setValueHash(newValueHash);
        }

        void removeValueIndex(Record record) {
            if (instanceType != Instance.InstanceType.MAP) return;
            int oldHash = record.getValueHash();
            Set<Record> lsRecords = mapValueIndex.get(oldHash);
            if (lsRecords != null && lsRecords.size() > 0) {
                lsRecords.remove(record);
                if (lsRecords.size() == 0) {
                    mapValueIndex.remove(oldHash);
                }
            }
            record.setValueHash(Integer.MIN_VALUE);
        }

        boolean searchValueIndex(Data value) {
            if (instanceType == Instance.InstanceType.MULTIMAP) {
                for (Record record : ownedRecords) {
                    Set<Data> multiValues = record.getMultiValues();
                    for (Data v : multiValues) {
                        if (v.equals(value)) return true;
                    }
                }
            } else {
                Set<Record> lsRecords = mapValueIndex.get(value.hashCode());
                if (lsRecords == null || lsRecords.size() == 0) {
                    return false;
                } else {
                    for (Record rec : lsRecords) {
                        if (rec.isValid()) {
                            if (value.equals(rec.getValue())) {
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }

        public void doUpdateIndex(final long[] newValues, final byte[] types, final Record record, final boolean active, final int valueHash) {
            if (active) {
                updateValueIndex(valueHash, record);
                ownedRecords.add(record);
            } else {
                removeValueIndex(record);
                ownedRecords.remove(record);
            }
            if (indexes != null) {
                long[] oldValues = record.getIndexes();
                int indexCount = indexes.length;
                if (active && newValues != null && indexCount != newValues.length) {
                    throw new RuntimeException(indexCount + " is expected but newValues " + newValues.length);
                }
                for (int i = 0; i < indexCount; i++) {
                    Index index = indexes[i];
                    long oldValue = (oldValues == null) ? Long.MIN_VALUE : oldValues[i];
                    if (active) {
                        if (oldValue == Long.MIN_VALUE) {
                            index.addNewIndex(newValues[i], types[i], record);
                        } else {
                            index.updateIndex(oldValue, newValues[i], types[i], record);
                        }
                    } else {
                        index.removeIndex(oldValue, record);
                    }
                }
                record.setIndexes(newValues, types);
            }
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
                        results = new HashSet<MapEntry>(ownedRecords.size());
                        for (MapEntry entry : ownedRecords) {
                            Record record = (Record) entry;
                            if (record.isActive()) {
                                results.add(record);
                            }
                        }
                    }
                } else {
                    results = new HashSet<MapEntry>(ownedRecords.size());
                    for (MapEntry entry : ownedRecords) {
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
    }

    public static long getLongValue(Object value) {
        if (value == null) return Long.MAX_VALUE;
        if (value instanceof Double) {
            return Double.doubleToLongBits((Double) value);
        } else if (value instanceof Float) {
            return Float.floatToIntBits((Float) value);
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof Boolean) {
            return (Boolean.TRUE.equals(value)) ? 1 : -1;
        } else {
            return value.hashCode();
        }
    }

    public boolean containsValue(final String name, final Data value) {
        try {
            final BlockingQueue<Boolean> resultQ = ResponseQueueFactory.newResponseQueue();
            queryQ.put(new Runnable() {
                public void run() {
                    IndexRegion indexRegion = getIndexRegion(name);
                    resultQ.offer(indexRegion.searchValueIndex(value));
                }
            });
            return resultQ.take();
        } catch (InterruptedException ignore) {
        }
        return false;
    }

    public QueryContext query(final QueryContext queryContext) {
        try {
            final BlockingQueue<QueryContext> resultQ = ResponseQueueFactory.newResponseQueue();
            queryQ.put(new Runnable() {
                public void run() {
                    IndexRegion indexRegion = getIndexRegion(queryContext.getMapName());
                    Set<MapEntry> results = indexRegion.doQuery(queryContext);
                    if (results == null) {
                        results = new HashSet<MapEntry>(0);
                    }
                    queryContext.setResults(results);
                    resultQ.offer(queryContext);
                }
            });
            return resultQ.take();
        } catch (InterruptedException ignore) {
        }
        return null;
    }

    public void updateIndex(final String name, final long[] newValues, final byte[] types, final Record record, final int valueHash) {
        try {
            final boolean active = record.isActive();
            queryQ.put(new Runnable() {
                public void run() {
                    try {
                        IndexRegion indexRegion = getIndexRegion(name);
                        indexRegion.doUpdateIndex(newValues, types, record, active, valueHash);
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.log(Level.SEVERE, "Indexing error. " + e);
                    }
                }
            });
        } catch (InterruptedException ignore) {
        }
    }

    public void setIndexes(final String name, final Index[] indexes, final Map<Expression, Index<MapEntry>> mapIndexes) {
        try {
            queryQ.put(new Runnable() {
                public void run() {
                    IndexRegion indexRegion = getIndexRegion(name);
                    indexRegion.indexes = indexes;
                    indexRegion.mapIndexes = mapIndexes;
                }
            });
        } catch (InterruptedException ignore) {
        }
    }

    public void reset(final String name) {
        try {
            queryQ.put(new Runnable() {
                public void run() {
                    IndexRegion indexRegion = getIndexRegion(name);
                    indexRegion.reset();
                }
            });
        } catch (InterruptedException ignore) {
        }
    }

    public IndexRegion getIndexRegion(String name) {
        IndexRegion indexRegion = regions.get(name);
        if (indexRegion == null) {
            indexRegion = new IndexRegion(name);
            regions.put(name, indexRegion);
        }
        return indexRegion;
    }
}
