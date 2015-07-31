/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
 */

package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.monitor.LocalCacheStats;
import com.hazelcast.util.Clock;

import static com.hazelcast.util.JsonUtil.getFloat;
import static com.hazelcast.util.JsonUtil.getLong;

/**
 * Default implementation of {@link com.hazelcast.monitor.LocalCacheStats}
 * <p/>
 * This class just provides serialization/deserialization methods to be used in
 * {@link com.hazelcast.monitor.MemberState} implementation while sending/receiving statistics to/from
 * Management center.
 * <p/>
 * There are no calculations are done in this class, all statistics gathered from
 * {@link com.hazelcast.cache.CacheStatistics}
 * <p/>
 * No setter methods are provided, all class fields supposed to be populated either
 * by a {@link com.hazelcast.cache.CacheStatistics} or while deserialization process
 * ({@link #fromJson(com.eclipsesource.json.JsonObject)}.
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

    public LocalCacheStatsImpl() {
    }

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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LocalCacheStatsImpl that = (LocalCacheStatsImpl) o;

        if (creationTime != that.creationTime) {
            return false;
        }
        if (cacheHits != that.cacheHits) {
            return false;
        }
        if (Float.compare(that.cacheHitPercentage, cacheHitPercentage) != 0) {
            return false;
        }
        if (cacheMisses != that.cacheMisses) {
            return false;
        }
        if (Float.compare(that.cacheMissPercentage, cacheMissPercentage) != 0) {
            return false;
        }
        if (cacheGets != that.cacheGets) {
            return false;
        }
        if (cachePuts != that.cachePuts) {
            return false;
        }
        if (cacheRemovals != that.cacheRemovals) {
            return false;
        }
        if (cacheEvictions != that.cacheEvictions) {
            return false;
        }
        if (Float.compare(that.averageGetTime, averageGetTime) != 0) {
            return false;
        }
        if (Float.compare(that.averagePutTime, averagePutTime) != 0) {
            return false;
        }
        if (Float.compare(that.averageRemoveTime, averageRemoveTime) != 0) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (cacheHits ^ (cacheHits >>> 32));
        result = 31 * result + (cacheHitPercentage != +0.0f ? Float.floatToIntBits(cacheHitPercentage) : 0);
        result = 31 * result + (int) (cacheMisses ^ (cacheMisses >>> 32));
        result = 31 * result + (cacheMissPercentage != +0.0f ? Float.floatToIntBits(cacheMissPercentage) : 0);
        result = 31 * result + (int) (cacheGets ^ (cacheGets >>> 32));
        result = 31 * result + (int) (cachePuts ^ (cachePuts >>> 32));
        result = 31 * result + (int) (cacheRemovals ^ (cacheRemovals >>> 32));
        result = 31 * result + (int) (cacheEvictions ^ (cacheEvictions >>> 32));
        result = 31 * result + (averageGetTime != +0.0f ? Float.floatToIntBits(averageGetTime) : 0);
        result = 31 * result + (averagePutTime != +0.0f ? Float.floatToIntBits(averagePutTime) : 0);
        result = 31 * result + (averageRemoveTime != +0.0f ? Float.floatToIntBits(averageRemoveTime) : 0);
        return result;
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
