package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.IndexStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MapIndexStats {
    private AtomicLongArray indexStats;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public void incrementIndexUsage(int index) {
        lock.readLock().lock();
        try {
            this.indexStats.incrementAndGet(index);
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getIndexUsageCount(int index) {
        lock.readLock().lock();
        try {
            return indexStats.get(index);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void addIndex(int index) {
        lock.writeLock().lock();
        try {
            int length = this.indexStats == null ? 0 : this.indexStats.length();
            AtomicLongArray newStats = new AtomicLongArray(length + 1);
            for (int i = 0; i < length; i++) {
                if (i < index) {
                    newStats.set(i, this.indexStats.get(i));
                } else if (i >= index) {
                    newStats.set(i + 1, this.indexStats.get(i + 1));
                }
            }
            indexStats = newStats;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public synchronized void removeIndex(int index) {
        lock.writeLock().lock();
        try {
            AtomicLongArray newStats = new AtomicLongArray(this.indexStats.length() - 1);
            for (int i = 0; i < this.indexStats.length() - 1; i++) {
                if (i < index) {
                    newStats.set(i, this.indexStats.get(i));
                } else {
                    newStats.set(i + 1, this.indexStats.get(i + 1));
                }
            }
            indexStats = newStats;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public IndexUsageIncrementer createIndexUsageIncrementer(int index) {
        return new IndexUsageIncrementer(this, index);
    }

    public static class IndexUsageIncrementer {
        private final MapIndexStats indexStats;
        private final int index;

        public IndexUsageIncrementer(MapIndexStats indexStats, int index) {
            this.indexStats = indexStats;
            this.index = index;
        }

        public void incrementIndexUsage() {
            indexStats.incrementIndexUsage(index);
        }
    }

    public static class IndexStatsImpl implements IndexStats {
        private long indexItemCount;
        private long usageCount;
        private Predicate predicate;
        private String attributeName;

        public IndexStatsImpl() {
        }

        public IndexStatsImpl(long indexItemCount, long usageCount, Predicate predicate, String attributeName) {
            this.indexItemCount = indexItemCount;
            this.usageCount = usageCount;
            this.predicate = predicate;
            this.attributeName = attributeName;
        }

        @Override
        public long getIndexEntryCount() {
            return indexItemCount;
        }

        @Override
        public long getUsageCount() {
            return usageCount;
        }

        @Override
        public Predicate getPredicate() {
            return predicate;
        }

        @Override
        public String getAttributeName() {
            return attributeName;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(indexItemCount);
            out.writeLong(usageCount);
            out.writeObject(predicate);
            out.writeUTF(attributeName);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            indexItemCount = in.readLong();
            usageCount = in.readLong();
            predicate = in.readObject();
            attributeName = in.readUTF();
        }

        @Override
        public JsonObject toJson() {
            return null;
        }

        @Override
        public void fromJson(JsonObject json) {

        }
    }
}
