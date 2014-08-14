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

package com.hazelcast.cache.jsr107;

import javax.cache.management.CacheStatisticsMXBean;
import java.io.Serializable;

public class CacheStatisticsMXBeanImpl implements CacheStatisticsMXBean, Serializable {

    private static final long NANOSECONDS_IN_A_MICROSECOND = 1000L;

    private CacheProxy proxy;

    public CacheStatisticsMXBeanImpl(CacheProxy proxy) {
        this.proxy = proxy;
    }

    @Override
    public void clear() {
        proxy.getCacheStatistics().clear();
    }

    @Override
    public long getCacheHits() {
        return proxy.getCacheStatistics().getHits();
    }

    @Override
    public float getCacheHitPercentage() {
        Long hits = getCacheHits();
        if (hits == 0) {
            return 0;
        }
        return (float) hits / getCacheGets() * 100.0f;
    }

    @Override
    public long getCacheMisses() {
        return proxy.getCacheStatistics().getMisses();
    }

    @Override
    public float getCacheMissPercentage() {
        Long misses = getCacheMisses();
        if (misses == 0) {
            return 0;
        }
        return (float) misses / getCacheGets() * 100.0f;
    }

    @Override
    public long getCacheGets() {
        return proxy.getCacheStatistics().getHits() + proxy.getCacheStatistics().getMisses();
    }

    @Override
    public long getCachePuts() {
        return proxy.getCacheStatistics().getPuts();
    }

    @Override
    public long getCacheRemovals() {
        return proxy.getCacheStatistics().getRemovals();
    }

    @Override
    public long getCacheEvictions() {
        return proxy.getCacheStatistics().getEvictions();
    }

    @Override
    public float getAverageGetTime() {
        if (proxy.getCacheStatistics().getGetTimeTakenNanos() == 0 || getCacheGets() == 0) {
            return 0;
        }
        return (proxy.getCacheStatistics().getGetTimeTakenNanos() / getCacheGets()) / NANOSECONDS_IN_A_MICROSECOND;
    }

    @Override
    public float getAveragePutTime() {
        if (proxy.getCacheStatistics().getPutTimeTakenNanos() == 0 || getCacheGets() == 0) {
            return 0;
        }
        return (proxy.getCacheStatistics().getPutTimeTakenNanos() / getCacheGets()) / NANOSECONDS_IN_A_MICROSECOND;
    }

    @Override
    public float getAverageRemoveTime() {
        if (proxy.getCacheStatistics().getRemoveTimeTakenNanos() == 0 || getCacheGets() == 0) {
            return 0;
        }
        return (proxy.getCacheStatistics().getRemoveTimeTakenNanos() / getCacheGets()) / NANOSECONDS_IN_A_MICROSECOND;
    }
}
