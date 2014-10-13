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
 * <p>
 *     In a multi node cluster, the total cluster statistics can be accumulated <br/>
 *     into one by accessing each node statistics through JMX
 * </p>
 */
public class CacheStatisticsMXBeanImpl
        implements CacheStatisticsMXBean, Serializable {

    private static final long serialVersionUID = -1;

    private transient CacheStatisticsImpl statistics;

    public CacheStatisticsMXBeanImpl(CacheStatisticsImpl statistics) {
        this.statistics = statistics;
    }

    @Override
    public void clear() {
        statistics.clear();
    }

    @Override
    public long getCacheHits() {
        return statistics.getCacheHits();
    }

    @Override
    public float getCacheHitPercentage() {
        return statistics.getCacheHitPercentage();
    }

    @Override
    public long getCacheMisses() {
        return statistics.getCacheMisses();
    }

    @Override
    public float getCacheMissPercentage() {
        return statistics.getCacheMissPercentage();
    }

    @Override
    public long getCacheGets() {
        return statistics.getCacheGets();
    }

    @Override
    public long getCachePuts() {
        return statistics.getCachePuts();
    }

    @Override
    public long getCacheRemovals() {
        return statistics.getCacheRemovals();
    }

    @Override
    public long getCacheEvictions() {
        return statistics.getCacheEvictions();
    }

    @Override
    public float getAverageGetTime() {
        return statistics.getAverageGetTime();
    }

    @Override
    public float getAveragePutTime() {
        return statistics.getAveragePutTime();
    }

    @Override
    public float getAverageRemoveTime() {
        return statistics.getAverageRemoveTime();
    }
}
