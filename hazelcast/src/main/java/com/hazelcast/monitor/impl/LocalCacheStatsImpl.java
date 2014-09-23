package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.monitor.LocalCacheStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Clock;

import java.io.IOException;

import static com.hazelcast.util.JsonUtil.getFloat;
import static com.hazelcast.util.JsonUtil.getLong;

/**
 * Default implementation of {@link com.hazelcast.monitor.LocalCacheStats}
 *
 * This class just provides serialization/deserialization methods to be used in
 * {@link com.hazelcast.monitor.MemberState} implementation while sending/receiving statistics to/from
 * Management center.
 *
 * There are no calculations are done in this class, all statistics gathered from
 * {@link com.hazelcast.cache.CacheStatistics}
 *
 * No setter methods are provided, all class fields supposed to be populated either
 * by a {@link com.hazelcast.cache.CacheStatistics} or while deserialization process
 * ({@link #fromJson(com.eclipsesource.json.JsonObject)}, or {@link #readData(com.hazelcast.nio.ObjectDataInput)}).
 *
 * @see com.hazelcast.cache.CacheStatistics
 */
public class LocalCacheStatsImpl implements LocalCacheStats {

    private long creationTime;
    private long cacheHits;
    private float cacheHitPercentage;
    private long cacheMisses;
    private float cacheMissPercentage;
    private long cacheGets;
    private long cachePuts;
    private long cacheRemovals;
    private long cacheEvictions;
    private float averageGetTime;
    private float averagePutTime;
    private float averageRemoveTime;

    public LocalCacheStatsImpl() { }

    public LocalCacheStatsImpl(CacheStatistics cacheStatistics) {
        creationTime = Clock.currentTimeMillis();
        cacheHits = cacheStatistics.getCacheHits();
        cacheHitPercentage = cacheStatistics.getCacheHitPercentage();
        cacheMisses = cacheStatistics.getCacheMisses();
        cacheMissPercentage = cacheStatistics.getCacheMissPercentage();
        cacheGets = cacheStatistics.getCacheGets();
        cachePuts = cacheStatistics.getCachePuts();
        cacheRemovals = cacheStatistics.getCacheRemovals();
        cacheEvictions = cacheStatistics.getCacheEvictions();
        averageGetTime = cacheStatistics.getAverageGetTime();
        averagePutTime = cacheStatistics.getAveragePutTime();
        averageRemoveTime = cacheStatistics.getAverageRemoveTime();
    }

    @Override
    public long getCacheHits() {
        return cacheHits;
    }

    @Override
    public float getCacheHitPercentage() {
        return cacheHitPercentage;
    }

    @Override
    public long getCacheMisses() {
        return cacheMisses;
    }

    @Override
    public float getCacheMissPercentage() {
        return cacheMissPercentage;
    }

    @Override
    public long getCacheGets() {
        return cacheGets;
    }

    @Override
    public long getCachePuts() {
        return cachePuts;
    }

    @Override
    public long getCacheRemovals() {
        return cacheRemovals;
    }

    @Override
    public long getCacheEvictions() {
        return cacheEvictions;
    }

    @Override
    public float getAverageGetTime() {
        return averageGetTime;
    }

    @Override
    public float getAveragePutTime() {
        return averagePutTime;
    }

    @Override
    public float getAverageRemoveTime() {
        return averageRemoveTime;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(creationTime);
        out.writeLong(cacheHits);
        out.writeFloat(cacheHitPercentage);
        out.writeLong(cacheMisses);
        out.writeFloat(cacheMissPercentage);
        out.writeLong(cacheGets);
        out.writeLong(cachePuts);
        out.writeLong(cacheRemovals);
        out.writeLong(cacheEvictions);
        out.writeFloat(averageGetTime);
        out.writeFloat(averagePutTime);
        out.writeFloat(averageRemoveTime);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        creationTime = in.readLong();
        cacheHits = in.readLong();
        cacheHitPercentage = in.readFloat();
        cacheMisses = in.readLong();
        cacheMissPercentage = in.readFloat();
        cacheGets = in.readLong();
        cachePuts = in.readLong();
        cacheRemovals = in.readLong();
        cacheEvictions = in.readLong();
        averageGetTime = in.readFloat();
        averagePutTime = in.readFloat();
        averageRemoveTime = in.readFloat();
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("creationTime", creationTime);
        root.add("cacheHits", cacheHits);
        root.add("cacheHitPercentage", cacheHitPercentage);
        root.add("cacheMisses", cacheMisses);
        root.add("cacheMissPercentage", cacheMissPercentage);
        root.add("cacheGets", cacheGets);
        root.add("cachePuts", cachePuts);
        root.add("cacheRemovals", cacheRemovals);
        root.add("cacheEvictions", cacheEvictions);
        root.add("averageGetTime", averageGetTime);
        root.add("averagePutTime", averagePutTime);
        root.add("averageRemoveTime", averageRemoveTime);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = getLong(json, "creationTime", -1L);
        cacheHits = getLong(json, "cacheHits", -1L);
        cacheHitPercentage = getFloat(json, "cacheHitPercentage", -1f);
        cacheMisses = getLong(json, "cacheMisses", -1L);
        cacheMissPercentage = getFloat(json, "cacheMissPercentage", -1f);
        cacheGets = getLong(json, "cacheGets", -1L);
        cachePuts = getLong(json, "cachePuts", -1L);
        cacheRemovals = getLong(json, "cacheRemovals", -1L);
        cacheEvictions = getLong(json, "cacheEvictions", -1L);
        averageGetTime = getFloat(json, "averageGetTime", -1f);
        averagePutTime = getFloat(json, "averagePutTime", -1f);
        averageRemoveTime = getFloat(json, "averageRemoveTime", -1f);
    }

    @Override
    public String toString() {
        return "LocalCacheStatsImpl{"
                + "creationTime=" + creationTime
                + ", cacheHits=" + cacheHits
                + ", cacheHitPercentage=" + cacheHitPercentage
                + ", cacheMisses=" + cacheMisses
                + ", cacheMissPercentage=" + cacheMissPercentage
                + ", cacheGets=" + cacheGets
                + ", cachePuts=" + cachePuts
                + ", cacheRemovals=" + cacheRemovals
                + ", cacheEvictions=" + cacheEvictions
                + ", averageGetTime=" + averageGetTime
                + ", averagePutTime=" + averagePutTime
                + ", averageRemoveTime=" + averageRemoveTime
                + '}';
    }

}
