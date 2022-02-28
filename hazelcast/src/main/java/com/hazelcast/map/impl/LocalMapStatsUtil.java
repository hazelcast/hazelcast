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

import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;

/**
 * Utility methods for {@link LocalMapStatsImpl}
 */
public final class LocalMapStatsUtil {

    private LocalMapStatsUtil() {

    }

    /**
     * Increments other operations count statistic in local map statistics.
     * @param service
     * @param mapName
     */
    public static void incrementOtherOperationsCount(MapService service, String mapName) {
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            LocalMapStatsImpl localMapStats = mapServiceContext.getLocalMapStatsProvider().getLocalMapStatsImpl(mapName);
            localMapStats.incrementOtherOperations();
        }
    }
}
