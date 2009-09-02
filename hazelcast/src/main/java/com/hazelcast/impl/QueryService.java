package com.hazelcast.impl;

import static com.hazelcast.impl.ConcurrentMapManager.Record;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.IndexedPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.RangedPredicate;
import static com.hazelcast.query.RangedPredicate.RangeType.*;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueryService implements Runnable {

    final Node node;
    volatile boolean running = true;

    final BlockingQueue<Runnable> queryQ = new LinkedBlockingQueue();

    public QueryService(Node node) {
        this.node = node;
    }

    public void run() {
        while (running) {
            Runnable run = null;
            try {
                run = queryQ.take();
                run.run();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public void addNewIndex(final Index index, final long value, final Record record) {
        try {
            queryQ.put(new Runnable() {
                public void run() {
                    index.addNewIndex(value, record);
                }
            });
        } catch (InterruptedException e) {
        }
    }

    public void updateIndex(final Index index, final long oldValue, final long newValue, final Record record) {
        try {
            queryQ.put(new Runnable() {
                public void run() {
                    index.updateIndex(oldValue, newValue, record);
                }
            });
        } catch (InterruptedException e) {
        }
    }

    public void removeIndex(final Index index, final long value, final Record record) {
        try {
            queryQ.put(new Runnable() {
                public void run() {
                    index.removeIndex(value, record);
                }
            });
        } catch (InterruptedException e) {
        }
    }

    public static long getLongValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof Boolean) {
            return (Boolean.TRUE.equals(value)) ? 1 : -1;
        } else {
            return value.hashCode();
        }
    }

    public Collection<Record> query(final Collection<Record> allRecords, final Map<String, Index> namedIndexes, final Predicate predicate) {
        try {
            final BlockingQueue<Collection<Record>> resultQ = new ArrayBlockingQueue(1);
            queryQ.put(new Runnable() {
                public void run() {
                    resultQ.offer(doQuery(allRecords, namedIndexes, predicate));
                }
            });
            return resultQ.take();
        } catch (InterruptedException e) {
        }
        return new ArrayList();
    }

    public Collection<Record> doQuery(Collection<Record> allRecords, Map<String, Index> namedIndexes, Predicate predicate) {
        Collection<Record> records = null;
        if (predicate != null && predicate instanceof IndexAwarePredicate) {
            List<IndexedPredicate> lsIndexPredicates = new ArrayList();
            IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
            iap.collectIndexedPredicates(lsIndexPredicates);
            for (IndexedPredicate indexedPredicate : lsIndexPredicates) {
                Index index = namedIndexes.get(indexedPredicate.getIndexName());
                if (index != null) {
                    Collection<Record> sub = null;
                    if (!(indexedPredicate instanceof RangedPredicate)) {
                        sub = index.getRecords(getLongValue(indexedPredicate.getValue()));
                    } else {
                        RangedPredicate rangedPredicate = (RangedPredicate) indexedPredicate;
                        RangedPredicate.RangeType type = rangedPredicate.getRangeType();
                        if (rangedPredicate.getRangeType() == RangedPredicate.RangeType.BETWEEN) {
                            sub = index.getSubRecords(getLongValue(rangedPredicate.getFrom()), getLongValue(rangedPredicate.getTo()));
                        } else {
                            boolean equal = (type == LESS_EQUAL || type == GREATER_EQUAL);
                            if (type == LESS || type == LESS_EQUAL) {
                                sub = index.getSubRecords(equal, true, getLongValue(indexedPredicate.getValue()));
                            } else {
                                sub = index.getSubRecords(equal, false, getLongValue(indexedPredicate.getValue()));
                            }
                        }
                    }
                    if (records == null) {
                        records = sub;
                    } else {
                        Iterator itCurrentEntries = records.iterator();
                        while (itCurrentEntries.hasNext()) {
                            if (!sub.contains(itCurrentEntries.next())) {
                                itCurrentEntries.remove();
                            }
                        }
                    }
                }
            }
        }
        if (records == null) {
            records = allRecords;
        }
        return records;
    }


}
