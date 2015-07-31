/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapKeySet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;
import java.util.Set;

public class MapKeySetOperation extends AbstractMapOperation implements PartitionAwareOperation, ReadonlyOperation {
    private Set<Data> keySet;

    public MapKeySetOperation(String name) {
        super(name);
    }

    public MapKeySetOperation() {
    }

    @Override
    public void run() {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(), name);
        keySet = recordStore.keySet();
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            LocalMapStatsProvider localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
            LocalMapStatsImpl localMapStatsImpl = localMapStatsProvider.getLocalMapStatsImpl(name);
            localMapStatsImpl.incrementOtherOperations();
        }
    }

    @Override
    public Object getResponse() {
        return new MapKeySet(keySet);
    }
}
