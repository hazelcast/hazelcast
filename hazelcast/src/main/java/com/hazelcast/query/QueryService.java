package com.hazelcast.query;

import com.hazelcast.core.MapEntry;
import static com.hazelcast.impl.ConcurrentMapManager.Record;
import com.hazelcast.impl.Node;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.omg.CORBA.OBJECT_NOT_EXIST;

public class QueryService implements Runnable {

    private final Logger logger = Logger.getLogger(QueryService.class.getName());
    final Node node;
    private volatile boolean running = true;

    final BlockingQueue<Runnable> queryQ = new LinkedBlockingQueue<Runnable>();
    final Set<Record> ownedRecords = new HashSet<Record>(1000);

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

    public void updateIndex(final Index[] indexes, final long[] newValues, final Record record) {
        try {
            queryQ.put(new Runnable() {
                public void run() {
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
                            ownedRecords.add(record);
                        } else {
                            index.updateIndex(oldValue, newValues[i], record);
                        }
                    }
                    record.setIndexes(newValues);
                }
            });
        } catch (InterruptedException ignore) {
        }
    }

    public void removeIndex(final Index<Record> index, final long value, final Record record) {
        try {
            queryQ.put(new Runnable() {
                public void run() {
                    index.removeIndex(value, record);
                }
            });
        } catch (InterruptedException ignore) {
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

    public Set<MapEntry> query(final AtomicBoolean strongRef, final Map<String, Index<MapEntry>> namedIndexes, final Predicate predicate) {
        try {
            final BlockingQueue<Set<MapEntry>> resultQ = new ArrayBlockingQueue<Set<MapEntry>>(1);
            queryQ.put(new Runnable() {
                public void run() {
                    Set<MapEntry> results = doQuery(strongRef, namedIndexes, predicate);
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

    public Set<MapEntry> doQuery(AtomicBoolean strongRef, Map<String, Index<MapEntry>> namedIndexes, Predicate predicate) {
        boolean strong = false;
        Set<MapEntry> results = null;
        try {
            if (predicate != null && predicate instanceof IndexAwarePredicate) {
                List<IndexAwarePredicate> lsIndexAwarePredicates = new ArrayList<IndexAwarePredicate>();
                IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
                strong = iap.collectIndexAwarePredicates(lsIndexAwarePredicates, namedIndexes);
                if (strong) {
                    Set<Index> setAppliedIndexes = new HashSet<Index>(1);
                    iap.collectAppliedIndexes(setAppliedIndexes, namedIndexes);
                    if (setAppliedIndexes.size() > 0) {
                        for (Index index : setAppliedIndexes) {
                            if (strong) {
                                strong = index.isStrong();
                            }
                        }
                    }
                }
                if (lsIndexAwarePredicates.size() == 1) {
                    IndexAwarePredicate indexAwarePredicate = lsIndexAwarePredicates.get(0);
                    return indexAwarePredicate.filter(namedIndexes);
                } else if (lsIndexAwarePredicates.size() > 0) {
                    Set<MapEntry> smallestSet = null;
                    List<Set<MapEntry>> lsSubResults = new ArrayList<Set<MapEntry>>(lsIndexAwarePredicates.size());
                    for (IndexAwarePredicate indexAwarePredicate : lsIndexAwarePredicates) {
                        Set<MapEntry> sub = indexAwarePredicate.filter(namedIndexes);
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
                    results = new HashSet<MapEntry>();
                    results.addAll(ownedRecords);
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
