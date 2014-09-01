/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import javax.cache.management.CacheStatisticsMXBean;
import java.io.Serializable;

/**
 * Implementation of the {@link javax.cache.management.CacheStatisticsMXBean}
 */
public class CacheStatisticsMXBeanImpl
        implements CacheStatisticsMXBean, Serializable {

    private static final long NANOSECONDS_IN_A_MICROSECOND = 1000L;
    private static final float FLOAT_HUNDRED = 100.0f;

    private CacheStatistics statistics;

    public CacheStatisticsMXBeanImpl(CacheStatistics statistics) {
        this.statistics = statistics;
    }

    @Override
    public void clear() {
        statistics.clear();
    }

    @Override
    public long getCacheHits() {
        return statistics.getHits();
    }

    @Override
    public float getCacheHitPercentage() {
        Long hits = getCacheHits();
        if (hits == 0) {
            return 0;
        }
        return (float) hits / getCacheGets() * FLOAT_HUNDRED;
    }

    @Override
    public long getCacheMisses() {
        return statistics.getMisses();
    }

    @Override
    public float getCacheMissPercentage() {
        Long misses = getCacheMisses();
        if (misses == 0) {
            return 0;
        }
        return (float) misses / getCacheGets() * FLOAT_HUNDRED;
    }

    @Override
    public long getCacheGets() {
        return statistics.getHits() + statistics.getMisses();
    }

    @Override
    public long getCachePuts() {
        return statistics.getPuts();
    }

    @Override
    public long getCacheRemovals() {
        return statistics.getRemovals();
    }

    @Override
    public long getCacheEvictions() {
        return statistics.getEvictions();
    }

    @Override
    public float getAverageGetTime() {
        if (statistics.getGetTimeTakenNanos() == 0 || getCacheGets() == 0) {
            return 0;
        }
        return (statistics.getGetTimeTakenNanos() / getCacheGets()) / NANOSECONDS_IN_A_MICROSECOND;
    }

    @Override
    public float getAveragePutTime() {
        if (statistics.getPutTimeTakenNanos() == 0 || getCacheGets() == 0) {
            return 0;
        }
        return (statistics.getPutTimeTakenNanos() / getCacheGets()) / NANOSECONDS_IN_A_MICROSECOND;
    }

    @Override
    public float getAverageRemoveTime() {
        if (statistics.getRemoveTimeTakenNanos() == 0 || getCacheGets() == 0) {
            return 0;
        }
        return (statistics.getRemoveTimeTakenNanos() / getCacheGets()) / NANOSECONDS_IN_A_MICROSECOND;
    }
}
