/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.operation;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.config.MapConfigReadOnly;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.impl.IndexRegistry;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Operation to fetch Map configuration.
 */
public class GetMapConfigOperation extends AbstractLocalOperation {

    private final String mapName;
    private MapConfig mapConfig;

    public GetMapConfigOperation(String mapName) {
        this.mapName = mapName;
    }

    @Override
    public void run() throws Exception {
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        MapConfig readOnlyMapConfig = mapContainer.getMapConfig();
        List<IndexConfig> indexConfigs = getIndexConfigsFromContainer(mapContainer);
        if (indexConfigs.isEmpty()) {
            mapConfig = readOnlyMapConfig;
        } else {
            MapConfig enrichedConfig = new MapConfig(readOnlyMapConfig);
            enrichedConfig.setIndexConfigs(indexConfigs);
            mapConfig = new MapConfigReadOnly(enrichedConfig);
        }
    }

    @Override
    public Object getResponse() {
        return mapConfig;
    }

    private List<IndexConfig> getIndexConfigsFromContainer(MapContainer mapContainer) {
        IndexRegistry indexRegistry = mapContainer.getGlobalIndexRegistry();
        if (indexRegistry != null) {
            return Arrays.stream(indexRegistry.getIndexes())
                    .map(InternalIndex::getConfig)
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }
}
