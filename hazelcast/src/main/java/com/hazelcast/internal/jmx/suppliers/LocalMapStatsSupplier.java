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

package com.hazelcast.internal.jmx.suppliers;

import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;

/**
 * Implementation of {@link StatsSupplier} for {@link LocalMapStats}
 */
public class LocalMapStatsSupplier implements StatsSupplier<LocalMapStats> {

    private final IMap map;

    public LocalMapStatsSupplier(IMap map) {
        this.map = map;
    }

    @Override
    public LocalMapStats getEmpty() {
        return new LocalMapStatsImpl();
    }

    @Override
    public LocalMapStats get() {
        return map.getLocalMapStats();
    }
}
