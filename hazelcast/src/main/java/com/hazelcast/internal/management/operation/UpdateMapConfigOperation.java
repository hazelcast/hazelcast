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

package com.hazelcast.internal.management.operation;

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.internal.management.dto.MapConfigDTO;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Operation to update map configuration from Management Center.
 */
public class UpdateMapConfigOperation extends AbstractManagementOperation {

    private String mapName;
    private MapConfig mapConfig;

    @SuppressWarnings("unused")
    public UpdateMapConfigOperation() {
    }

    public UpdateMapConfigOperation(String mapName, MapConfig mapConfig) {
        this.mapName = mapName;
        this.mapConfig = mapConfig;
    }

    @Override
    public void run() throws Exception {
        MapService service = getService();
        MapConfig oldConfig = service.getMapServiceContext().getMapContainer(mapName).getMapConfig();
        MapConfig newConfig = new MapConfig(oldConfig);
        newConfig.setTimeToLiveSeconds(mapConfig.getTimeToLiveSeconds());
        newConfig.setMaxIdleSeconds(mapConfig.getMaxIdleSeconds());
        newConfig.setEvictionPolicy(mapConfig.getEvictionPolicy());
        newConfig.setReadBackupData(mapConfig.isReadBackupData());
        newConfig.setMaxSizeConfig(mapConfig.getMaxSizeConfig());
        MapContainer mapContainer = service.getMapServiceContext().getMapContainer(mapName);
        mapContainer.setMapConfig(newConfig.getAsReadOnly());
        mapContainer.initEvictor();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        new MapConfigDTO(mapConfig).writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        MapConfigDTO adapter = new MapConfigDTO();
        adapter.readData(in);
        mapConfig = adapter.getMapConfig();
    }

    @Override
    public int getClassId() {
        return ManagementDataSerializerHook.UPDATE_MAP_CONFIG;
    }
}
