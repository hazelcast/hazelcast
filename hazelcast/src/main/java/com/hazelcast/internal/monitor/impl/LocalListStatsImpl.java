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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.hazelcast.collection.LocalListStats;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.Clock;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.LIST_METRIC_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.LIST_METRIC_LAST_ACCESS_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.LIST_METRIC_LAST_UPDATE_TIME;
import static com.hazelcast.internal.util.ConcurrencyUtil.setMax;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class LocalListStatsImpl extends LocalCollectionStatsImpl implements LocalListStats {

    protected static final AtomicLongFieldUpdater<LocalListStatsImpl> LAST_ACCESS_TIME_UPDATER =
            newUpdater(LocalListStatsImpl.class, LAST_ACCESS_TIME);
    protected static final AtomicLongFieldUpdater<LocalListStatsImpl> LAST_UPDATE_TIME_UPDATER =
            newUpdater(LocalListStatsImpl.class, LAST_UPDATE_TIME);

    @Probe(name = LIST_METRIC_LAST_ACCESS_TIME)
    protected volatile long lastAccessTime;
    @Probe(name = LIST_METRIC_LAST_UPDATE_TIME)
    protected volatile long lastUpdateTime;
    @Probe(name = LIST_METRIC_CREATION_TIME)
    protected volatile long creationTime;

    public LocalListStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        setMax(this, LAST_ACCESS_TIME_UPDATER, lastAccessTime);
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public void setLastUpdateTime(long lastUpdateTime) {
        setMax(this, LAST_UPDATE_TIME_UPDATER, lastUpdateTime);
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
        root.add(CREATION_TIME, creationTime);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        LAST_ACCESS_TIME_UPDATER.set(this, json.getLong(LAST_ACCESS_TIME, -1));
        LAST_UPDATE_TIME_UPDATER.set(this, json.getLong(LAST_UPDATE_TIME, -1));
        creationTime = json.getLong(CREATION_TIME, -1);
    }

    @Override
    public String toString() {
        return "LocalListStatsImpl{"
                + "lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", creationTime=" + creationTime
                + '}';
    }

}
