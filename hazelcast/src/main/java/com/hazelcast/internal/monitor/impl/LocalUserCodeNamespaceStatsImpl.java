/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.Clock;

import java.util.Map;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.USER_CODE_NAMESPACE_UPDATE_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.USER_CODE_NAMESPACE_RESOURCE_COUNT;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;

public class LocalUserCodeNamespaceStatsImpl implements LocalUserCodeNamespaceStats {

    @Probe(name = USER_CODE_NAMESPACE_UPDATE_TIME, unit = MS)
    private final long updateTime;
    private Map<String, LocalUserCodeNamespaceResourceStats> localUserCodeNamespaceResourceStats;

    public LocalUserCodeNamespaceStatsImpl() {
        updateTime = Clock.currentTimeMillis();
    }

    @Override
    public long getCreationTime() {
        return updateTime;
    }

    @Probe(name = USER_CODE_NAMESPACE_RESOURCE_COUNT, unit = COUNT)
    @Override
    public long getResourcesCount() {
        return localUserCodeNamespaceResourceStats.size();
    }

    public void setLocalUserCodeNamespaceResourceStats(Map<String, LocalUserCodeNamespaceResourceStats>
                                                               localUserCodeNamespaceResourceStats) {
        this.localUserCodeNamespaceResourceStats = localUserCodeNamespaceResourceStats;
    }

    @Override
    public Map<String, LocalUserCodeNamespaceResourceStats> getResources() {
        return localUserCodeNamespaceResourceStats;
    }
}
