/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.collection.LocalCollectionStats;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.json.internal.JsonSerializable;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.COLLECTION_METRIC_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.COLLECTION_METRIC_LAST_ACCESS_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.COLLECTION_METRIC_LAST_UPDATE_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.COLLECTION_METRIC_NUMBER_OF_HITS;
import static com.hazelcast.internal.util.ConcurrencyUtil.setMax;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class LocalCollectionStatsImpl implements LocalCollectionStats, JsonSerializable {

    public static final String LAST_ACCESS_TIME = "lastAccessTime";
    public static final String LAST_UPDATE_TIME = "lastUpdateTime";
    public static final String NUMBER_OF_HITS = "numberOfHits";
    public static final String CREATION_TIME = "creationTime";

    private static final AtomicLongFieldUpdater<LocalCollectionStatsImpl> LAST_ACCESS_TIME_UPDATER =
            newUpdater(LocalCollectionStatsImpl.class, LAST_ACCESS_TIME);
    private static final AtomicLongFieldUpdater<LocalCollectionStatsImpl> LAST_UPDATE_TIME_UPDATER =
            newUpdater(LocalCollectionStatsImpl.class, LAST_UPDATE_TIME);
    private static final AtomicLongFieldUpdater<LocalCollectionStatsImpl> NUMBER_OF_HITS_UPDATER =
            newUpdater(LocalCollectionStatsImpl.class, NUMBER_OF_HITS);

    @Probe(name = COLLECTION_METRIC_LAST_ACCESS_TIME)
    private volatile long lastAccessTime;
    @Probe(name = COLLECTION_METRIC_LAST_UPDATE_TIME)
    private volatile long lastUpdateTime;
    @Probe(name = COLLECTION_METRIC_NUMBER_OF_HITS)
    private volatile long numberOfHits;
    @Probe(name = COLLECTION_METRIC_CREATION_TIME)
    private volatile long creationTime;

    public LocalCollectionStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        setMax(this, LAST_ACCESS_TIME_UPDATER, lastAccessTime);
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        setMax(this, LAST_UPDATE_TIME_UPDATER, lastUpdateTime);
    }

    @Override
    public long getNumberOfHits() {
        return numberOfHits;
    }

    public void incrementNumberOfHits() {
        NUMBER_OF_HITS_UPDATER.incrementAndGet(this);
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add(LAST_ACCESS_TIME, lastAccessTime);
        root.add(LAST_UPDATE_TIME, lastUpdateTime);
        root.add(NUMBER_OF_HITS, numberOfHits);
        root.add(CREATION_TIME, creationTime);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        LAST_ACCESS_TIME_UPDATER.set(this, json.getLong(LAST_ACCESS_TIME, -1));
        LAST_UPDATE_TIME_UPDATER.set(this, json.getLong(LAST_UPDATE_TIME, -1));
        NUMBER_OF_HITS_UPDATER.set(this, json.getLong(NUMBER_OF_HITS, -1));
        creationTime = json.getLong(CREATION_TIME, -1);
    }

    @Override
    public String toString() {
        return "LocalListStatsImpl{"
                + "lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", numberOfHits=" + numberOfHits
                + ", creationTime=" + creationTime
                + '}';
    }
}
