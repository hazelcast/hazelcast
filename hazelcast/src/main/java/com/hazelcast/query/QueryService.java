/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
import static com.hazelcast.impl.ConcurrentMapManager.Record;
import com.hazelcast.impl.Node;
import com.hazelcast.nio.Data;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class QueryService implements Runnable {

    private final Logger logger = Logger.getLogger(QueryService.class.getName());
    final Node node;
    private volatile boolean running = true;

    final BlockingQueue<Runnable> queryQ = new LinkedBlockingQueue<Runnable>();

    final Map<String, IndexRegion> regions = new HashMap<String, IndexRegion>(10);

    public QueryService(Node node) {
        this.node = node;
    }

    public void run() {
        while (running) {
            Runnable run;
            try {
                run = queryQ.take();
                run.run();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    class IndexRegion {
        final String name;
        final Set<Record> ownedRecords = new HashSet<Record>(1000);
        final Map<Integer, Set<Record>> mapValueIndex = new HashMap(1000);


        IndexRegion(String name) {
            this.name = name;
        }

        void addNewValueIndex(int newValueHash, Record record) {
            Set<Record> lsRecords = mapValueIndex.get(newValueHash);
            if (lsRecords == null) {
                lsRecords = new LinkedHashSet<Record>();
                mapValueIndex.put(newValueHash, lsRecords);
            }
            lsRecords.add(record);
        }

        void updateValueIndex(int newValueHash, Record record) {
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
            return false;
        }

        public void doUpdateIndex(final Index[] indexes, final long[] newValues, final Record record, final int valueHash) {
            if (record.isActive()) {
                updateValueIndex(valueHash, record);
                ownedRecords.add(record);
            } else {
                removeValueIndex(record);
                ownedRecords.remove(record);
            }
            if (indexes != null) {
                long[] oldValues = record.getIndexes();
                int indexCount = indexes.length;
                if (indexCount != newValues.length) {
                    throw new RuntimeException(indexCount + " is expected but newValues " + newValues.length);
                }
                for (int i = 0; i < indexCount; i++) {
                    Index index = indexes[i];
                    long oldValue = (oldValues == null) ? Long.MIN_VALUE : oldValues[i];
                    if (oldValue == Long.MIN_VALUE) {
                        index.addNewIndex(newValues[i], record);
                    } else {
                        index.updateIndex(oldValue, newValues[i], record);
                    }
                }
                record.setIndexes(newValues);
            }
        }

        public Set<MapEntry> doQuery(AtomicBoolean strongRef, Map<Expression, Index<MapEntry>> mapIndexes, Predicate predicate) {
            boolean strong = false;
            Set<MapEntry> results = null;
            try {
                if (predicate != null && predicate instanceof IndexAwarePredicate) {
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
                    System.out.println("indexare " + lsIndexAwarePredicates);
                    if (lsIndexAwarePredicates.size() == 1) {
                        IndexAwarePredicate indexAwarePredicate = lsIndexAwarePredicates.get(0);
                        return indexAwarePredicate.filter(mapIndexes);
                    } else if (lsIndexAwarePredicates.size() > 0) {
                        Set<MapEntry> smallestSet = null;
                        List<Set<MapEntry>> lsSubResults = new ArrayList<Set<MapEntry>>(lsIndexAwarePredicates.size());
                        for (IndexAwarePredicate indexAwarePredicate : lsIndexAwarePredicates) {
                            Set<MapEntry> sub = indexAwarePredicate.filter(mapIndexes);
                            if (sub == null || sub.size() == 0) {
                                return null;
                            } else {
                                if (smallestSet == null) {
                                    smallestSet = sub;
                                } else {
                                    if (sub.size() < smallestSet.size()) {
                                        smallestSet = sub;
                                    }
                                }
                                lsSubResults.add(sub);
                            }
                        }
                        System.out.println("smallest set size " + smallestSet.size());
                        results = new HashSet<MapEntry>();
                        results.addAll(smallestSet);
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
                        System.out.println("adding all " + ownedRecords.size());
                        results = new HashSet<MapEntry>();
                        results.addAll(ownedRecords);
                        System.out.println("result cont " + results.size());
                    }
                } else {
                    results = new HashSet<MapEntry>();
                    results.addAll(ownedRecords);
                }
            } finally {
                strongRef.set(strong);
            }
            return results;
        }

    }

    public static long getLongValue(Object value) {
        if (value == null) return Long.MAX_VALUE;
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof Boolean) {
            return (Boolean.TRUE.equals(value)) ? 1 : -1;
        } else {
            return value.hashCode();
        }
    }

    public Set<MapEntry> query(final String name, final AtomicBoolean strongRef, final Map<Expression, Index<MapEntry>> mapIndexes, final Predicate predicate) {
        try {
            final BlockingQueue<Set<MapEntry>> resultQ = new ArrayBlockingQueue<Set<MapEntry>>(1);
            queryQ.put(new Runnable() {
                public void run() {
                    IndexRegion indexRegion = getIndexRegion(name);
                    Set<MapEntry> results = indexRegion.doQuery(strongRef, mapIndexes, predicate);
                    if (results == null) {
                        results = new HashSet(0);
                    }
                    resultQ.offer(results);
                }
            });
            return resultQ.take();
        } catch (InterruptedException ignore) {
        }
        return null;
    }

    public void updateIndex(final String name, final Index[] indexes, final long[] newValues, final Record record, final int valueHash) {
        try {
            queryQ.put(new Runnable() {
                public void run() {
                    IndexRegion indexRegion = getIndexRegion(name);
                    indexRegion.doUpdateIndex(indexes, newValues, record, valueHash);
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
