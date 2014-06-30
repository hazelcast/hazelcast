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

package com.hazelcast.map.operation;

import com.hazelcast.map.MapService;
import com.hazelcast.map.RecordStore;
import com.hazelcast.spi.PartitionAwareOperation;

public class MapSizeOperation extends AbstractMapOperation implements PartitionAwareOperation {

    private int size;

    public MapSizeOperation(String name) {
        super(name);
    }

    public MapSizeOperation() {
    }

    public void run() {
        RecordStore recordStore = mapService.getMapServiceContext().getRecordStore(getPartitionId(), name);
        recordStore.checkIfLoaded();
        size = recordStore.size();
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            ((MapService) getService()).getMapServiceContext()
                    .getLocalMapStatsProvider().getLocalMapStatsImpl(name).incrementOtherOperations();
        }
    }

    @Override
    public Object getResponse() {
        return size;
    }

}
