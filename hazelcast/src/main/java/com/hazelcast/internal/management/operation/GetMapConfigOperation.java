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
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;

import java.util.function.Consumer;

/**
 * Operation to fetch Map configuration.
 */
public class GetMapConfigOperation
        extends AbstractLocalOperation implements Consumer<IndexConfig> {

    private final String mapName;

    private MapConfig mapConfig;
    private MapConfig mapConfigWithIndexes;

    public GetMapConfigOperation(String mapName) {
        this.mapName = mapName;
    }

    @Override
    public void run() throws Exception {
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        mapConfig = mapContainer.getMapConfig();
        mapContainer.consumeIndexConfigs(this);
    }

    @Override
    public Object getResponse() {
        return mapConfigWithIndexes != null
                ? new MapConfigReadOnly(mapConfigWithIndexes) : mapConfig;
    }

    @Override
    public void accept(IndexConfig indexConfig) {
        if (mapConfigWithIndexes == null) {
            mapConfigWithIndexes = new MapConfig(mapConfig);
            // first clear all existing index-configs
            // here, not to have duplicate index-configs
            mapConfigWithIndexes.getIndexConfigs().clear();
        }

        mapConfigWithIndexes.addIndexConfig(indexConfig);
    }
}
