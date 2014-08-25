package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.IndexStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Created by buremba <Burak Emre Kabakcı> on 23/08/14 22:49.
 */
public class MapIndexStats {
    private AtomicLongArray indexStats;

    public void incrementIndexUsage(int index) {
        this.indexStats.incrementAndGet(index);
    }

    public long getIndexUsageCount(int index) {
        return indexStats.get(index);
    }

    public void addIndex(int index) {
//        synchronized (indexStats) {
            int length = this.indexStats == null ? 0 : this.indexStats.length();
            AtomicLongArray newStats = new AtomicLongArray(length + 1);
            for (int i = 0; i < length; i++) {
                if(i < index) {
                    newStats.set(i, this.indexStats.get(i));
                }else
                if(i > index) {
                    newStats.set(i+1, this.indexStats.get(i+1));
                }
            }
            indexStats = newStats;
//        }
    }

    public synchronized void removeIndex(int index) {
//        synchronized (indexStats) {
            AtomicLongArray newStats = new AtomicLongArray(this.indexStats.length() - 1);
            for (int i = 0; i < this.indexStats.length() - 1; i++) {
                if (i < index) {
                    newStats.set(i, this.indexStats.get(i));
                } else {
                    newStats.set(i + 1, this.indexStats.get(i + 1));
                }
            }
            indexStats = newStats;
//        }
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
        public long getIndexItemCount() {
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
