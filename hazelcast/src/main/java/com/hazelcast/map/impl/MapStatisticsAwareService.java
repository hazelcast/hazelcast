/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.map.LocalMapStats;
import com.hazelcast.internal.services.StatisticsAwareService;

import java.util.Map;

/**
 * {@link StatisticsAwareService} implementation for {@link MapService}.
 * Mainly responsible for creating {@link LocalMapStats} for the maps
 * on a local Hazelcast node. Used for node-monitoring purposes in management centre.
 *
 * @see StatisticsAwareService
 */
class MapStatisticsAwareService implements StatisticsAwareService<LocalMapStats> {

    private final MapServiceContext mapServiceContext;

    MapStatisticsAwareService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public Map<String, LocalMapStats> getStats() {
        LocalMapStatsProvider localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
        return localMapStatsProvider.createAllLocalMapStats();
    }
}
