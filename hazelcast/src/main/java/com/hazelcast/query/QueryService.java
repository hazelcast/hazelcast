package com.hazelcast.query;

import com.hazelcast.core.MapEntry;
import static com.hazelcast.impl.ConcurrentMapManager.Record;
import com.hazelcast.impl.Node;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

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
                    for (int i=0; i < indexCount; i++) {
                        Index index = indexes[i];
                        long oldValue = (oldValues == null) ? Long.MIN_VALUE : oldValues[i];
                        if (oldValue == Long.MIN_VALUE){
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

    public boolean query(final Set<MapEntry> results, final Map<String, Index<MapEntry>> namedIndexes, final Predicate predicate) {
        try {
            final BlockingQueue<Boolean> resultQ = new ArrayBlockingQueue<Boolean>(1);
            queryQ.put(new Runnable() {
                public void run() {
                    resultQ.offer(doQuery(results, namedIndexes, predicate));
                }
            });
            return resultQ.take();
        } catch (InterruptedException ignore) {
        }
        return false;
    }

    public boolean doQuery(Set<MapEntry> results, Map<String, Index<MapEntry>> namedIndexes, Predicate predicate) {
        boolean strong = false;
        if (predicate != null && predicate instanceof IndexAwarePredicate) {
            List<IndexAwarePredicate> lsIndexAwarePredicates = new ArrayList<IndexAwarePredicate>();
            IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
            strong = iap.collectIndexedPredicates(lsIndexAwarePredicates);
            for (IndexAwarePredicate indexAwarePredicate : lsIndexAwarePredicates) {
                boolean stillStrong = indexAwarePredicate.filter(results, namedIndexes);
                if (strong) {
                    strong = stillStrong;
                }
            }
        }
        return strong;
    }

//    public Collection<Record> doQuery2(Collection<Record> allRecords, Map<String, Index<Record>> namedIndexes, Predicate predicate) {
//        Collection<Record> records = null;
//        boolean strong = false;
//        if (predicate != null && predicate instanceof IndexAwarePredicate) {
//            List<IndexedPredicate> lsIndexPredicates = new ArrayList<IndexedPredicate>();
//            IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
////            strong = iap.collectIndexedPredicates(lsIndexPredicates);
//            for (IndexedPredicate indexedPredicate : lsIndexPredicates) {
//                Index<Record> index = namedIndexes.get(indexedPredicate.getIndexName());
//                if (index != null) {
//                    if (strong) {
//                        strong = index.strong;
//                    }
//                    Collection<Record> sub;
//                    if (!(indexedPredicate instanceof RangedPredicate)) {
//                        sub = index.getRecords(getLongValue(indexedPredicate.getValue()));
//                    } else {
//                        RangedPredicate rangedPredicate = (RangedPredicate) indexedPredicate;
//                        RangedPredicate.RangeType type = rangedPredicate.getRangeType();
//                        if (rangedPredicate.getRangeType() == RangedPredicate.RangeType.BETWEEN) {
//                            sub = index.getSubRecords(getLongValue(rangedPredicate.getFrom()), getLongValue(rangedPredicate.getTo()));
//                        } else {
//                            boolean equal = (type == LESS_EQUAL || type == GREATER_EQUAL);
//                            if (type == LESS || type == LESS_EQUAL) {
//                                sub = index.getSubRecords(equal, true, getLongValue(indexedPredicate.getValue()));
//                            } else {
//                                sub = index.getSubRecords(equal, false, getLongValue(indexedPredicate.getValue()));
//                            }
//                        }
//                    }
//                    if (sub != null) {
//                        logger.log(Level.FINEST, node.getName() + " index sub.size " + sub.size());
//                        System.out.println(node.getName() + " index sub.size " + sub.size());
//                    }
//                    if (records == null) {
//                        records = sub;
//                    } else {
//                        Iterator itCurrentEntries = records.iterator();
//                        while (itCurrentEntries.hasNext()) {
//                            if (!sub.contains(itCurrentEntries.next())) {
//                                itCurrentEntries.remove();
//                            }
//                        }
//                        System.out.println(node.getName() + " after join " + records.size());
//                    }
//                }
//            }
//        }
//        if (records == null) {
//            records = allRecords;
//        }
//        return records;
//    }


}
