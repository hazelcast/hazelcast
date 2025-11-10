/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.LocalSetStats;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.tpcengine.util.ReflectionUtil;
import com.hazelcast.internal.util.Clock;

import java.lang.invoke.VarHandle;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.SET_METRIC_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.SET_METRIC_LAST_ACCESS_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.SET_METRIC_LAST_UPDATE_TIME;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.util.ConcurrencyUtil.setMax;

public class LocalSetStatsImpl extends AbstractLocalCollectionStats implements LocalSetStats {

    private static final VarHandle LAST_ACCESS_TIME_VARHANDLE = ReflectionUtil.findVarHandle(LAST_ACCESS_TIME, long.class);
    private static final VarHandle LAST_UPDATE_TIME_VARHANDLE = ReflectionUtil.findVarHandle(LAST_UPDATE_TIME, long.class);

    @Probe(name = SET_METRIC_LAST_ACCESS_TIME, unit = MS)
    protected volatile long lastAccessTime;
    @Probe(name = SET_METRIC_LAST_UPDATE_TIME, unit = MS)
    protected volatile long lastUpdateTime;
    @Probe(name = SET_METRIC_CREATION_TIME, unit = MS)
    protected final long creationTime;

    public LocalSetStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        setMax(this, LAST_ACCESS_TIME_VARHANDLE, lastAccessTime);
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public void setLastUpdateTime(long lastUpdateTime) {
        setMax(this, LAST_UPDATE_TIME_VARHANDLE, lastUpdateTime);
    }

    @Override
    public long getCreationTime() {
        return creationTime;
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
