package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheStats;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.Counter;

import java.io.IOException;

/**
 * @author enesakar 2/19/14
 */
public class CacheStatsImpl implements DataSerializable, CacheStats {

    private final Counter hits = new Counter();
    private final Counter misses = new Counter();

    private final Counter puts = new Counter();
    private final Counter gets = new Counter();
    private final Counter removes = new Counter();
    private final Counter others = new Counter();

    private volatile long creationTime;
    private volatile long lastUpdateTime;
    private volatile long lastAccessTime;

    private volatile Counter totalPutLatency = new Counter();
    private final Counter totalGetLatency = new Counter();
    private final Counter totalRemoveLatency = new Counter();
    private volatile NearCacheStats nearCacheStats;

    public CacheStatsImpl() {
    }

    public CacheStatsImpl(long creationTime) {
        this.creationTime = creationTime;
    }

    @Override
    public long getHits() {
        return hits.get();
    }

    @Override
    public long getMisses() {
        return misses.get();
    }

    @Override
    public long getPuts() {
        return puts.get();
    }

    @Override
    public long getGets() {
        return gets.get();
    }

    @Override
    public long getRemoves() {
        return removes.get();
    }

    @Override
    public double getAveragePutLatency() {
        long puts = getPuts();
        return puts <= 0 ? 0 : (double) totalPutLatency.get() / puts;
    }

    @Override
    public double getAverageGetLatency() {
        long gets = getGets();
        return gets <= 0 ? 0 : (double) totalGetLatency.get() / gets;
    }

    @Override
    public double getAverageRemoveLatency() {
        long removes = getRemoves();
        return removes <= 0 ? 0 : (double) totalRemoveLatency.get() / removes;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        return nearCacheStats;
    }

    public void setNearCacheStats(NearCacheStats nearCacheStats) {
        this.nearCacheStats = nearCacheStats;
    }

    public void updatePutStats(long startTime) {
        puts.increment();
        long currentTime = System.currentTimeMillis();
        lastUpdateTime = currentTime;
        totalPutLatency.add(currentTime - startTime);
    }

    public void updateRemoveStats(long startTime) {
        removes.increment();
        long currentTime = System.currentTimeMillis();
        lastUpdateTime = currentTime;
        totalRemoveLatency.add(currentTime - startTime);
    }

    public void updateGetStats(boolean hit, long startTime) {
        gets.increment();
        long currentTime = System.currentTimeMillis();
        lastAccessTime = currentTime;
        if (hit) {
            hits.increment();
        } else {
            misses.increment();
        }
        totalGetLatency.add(currentTime - startTime);
    }

    public void updateGetStats() {
        gets.increment();
        lastAccessTime = System.currentTimeMillis();
    }

    public void updateGetStats(boolean hit) {
        gets.increment();
        lastAccessTime = System.currentTimeMillis();
        if (hit) {
            hits.increment();
        } else {
            misses.increment();
        }
    }

    public void updatePutStats() {
        puts.increment();
        lastUpdateTime = System.currentTimeMillis();
    }

    public void updateRemoveStats() {
        removes.increment();
        lastUpdateTime = System.currentTimeMillis();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(misses.get());
        out.writeLong(hits.get());
        out.writeLong(puts.get());
        out.writeLong(gets.get());
        out.writeLong(removes.get());
        out.writeLong(others.get());
        out.writeLong(totalGetLatency.get());
        out.writeLong(totalPutLatency.get());
        out.writeLong(totalRemoveLatency.get());
        out.writeLong(creationTime);
        out.writeLong(lastAccessTime);
        out.writeLong(lastUpdateTime);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        misses.set(in.readLong());
        hits.set(in.readLong());
        puts.set(in.readLong());
        gets.set(in.readLong());
        removes.set(in.readLong());
        others.set(in.readLong());
        totalGetLatency.set(in.readLong());
        totalPutLatency.set(in.readLong());
        totalRemoveLatency.set(in.readLong());
        creationTime = in.readLong();
        lastAccessTime = in.readLong();
        lastUpdateTime = in.readLong();
    }
}
