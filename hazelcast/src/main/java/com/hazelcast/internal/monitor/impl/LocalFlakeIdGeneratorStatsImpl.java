/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.monitor.LocalFlakeIdGeneratorStats;
import com.hazelcast.internal.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.util.JsonUtil.getLong;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class LocalFlakeIdGeneratorStatsImpl implements LocalFlakeIdGeneratorStats {

    private static final AtomicLongFieldUpdater<LocalFlakeIdGeneratorStatsImpl> BATCH_COUNT =
            newUpdater(LocalFlakeIdGeneratorStatsImpl.class, "batchCount");

    private static final AtomicLongFieldUpdater<LocalFlakeIdGeneratorStatsImpl> ID_COUNT =
            newUpdater(LocalFlakeIdGeneratorStatsImpl.class, "idCount");

    @Probe
    private volatile long creationTime;
    @Probe
    private volatile long batchCount;
    @Probe
    private volatile long idCount;

    public LocalFlakeIdGeneratorStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getBatchCount() {
        return batchCount;
    }

    public long getIdCount() {
        return idCount;
    }

    public void update(int batchSize) {
        BATCH_COUNT.incrementAndGet(this);
        ID_COUNT.addAndGet(this, batchSize);
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("creationTime", creationTime);
        root.add("batchCount", batchCount);
        root.add("idCount", idCount);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = getLong(json, "creationTime", -1L);
        batchCount = getLong(json, "batchCount", 0);
        idCount = getLong(json, "idCount", 0);
    }

    @Override
    public String toString() {
        return "LocalFlakeIdStatsImpl{" + "creationTime=" + creationTime + ", batchCount="
                + batchCount + ", idCount=" + idCount + '}';
    }
}
