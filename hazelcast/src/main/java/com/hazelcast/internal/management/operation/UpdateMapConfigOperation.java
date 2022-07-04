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

package com.hazelcast.internal.management.operation;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.internal.config.MapConfigReadOnly;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Operation to update map configuration from Management Center.
 */
public class UpdateMapConfigOperation extends AbstractManagementOperation {

    private boolean readBackupData;
    private int timeToLiveSeconds;
    private int maxIdleSeconds;
    private int maxSize;
    private int maxSizePolicyId;
    private int evictionPolicyId;
    private String mapName;

    public UpdateMapConfigOperation() {
    }

    public UpdateMapConfigOperation(String mapName, int timeToLiveSeconds, int maxIdleSeconds,
                                    int maxSize, int maxSizePolicyId, boolean readBackupData,
                                    int evictionPolicyId) {
        this.mapName = mapName;
        this.timeToLiveSeconds = timeToLiveSeconds;
        this.maxIdleSeconds = maxIdleSeconds;
        this.maxSize = maxSize;
        this.maxSizePolicyId = maxSizePolicyId;
        this.readBackupData = readBackupData;
        this.evictionPolicyId = evictionPolicyId;
    }

    @Override
    public void run() throws Exception {
        MapService service = getService();
        MapConfig oldConfig = service.getMapServiceContext().getMapContainer(mapName).getMapConfig();
        MapConfig newConfig = new MapConfig(oldConfig);
        newConfig.setTimeToLiveSeconds(timeToLiveSeconds);
        newConfig.setMaxIdleSeconds(maxIdleSeconds);
        newConfig.setReadBackupData(readBackupData);

        EvictionConfig evictionConfig = newConfig.getEvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.getById(evictionPolicyId));
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.getById(maxSizePolicyId));
        evictionConfig.setSize(maxSize);

        MapContainer mapContainer = service.getMapServiceContext().getMapContainer(mapName);
        mapContainer.setMapConfig(new MapConfigReadOnly(newConfig));
        mapContainer.initEvictor();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        out.writeInt(timeToLiveSeconds);
        out.writeInt(maxIdleSeconds);
        out.writeInt(maxSize);
        out.writeInt(maxSizePolicyId);
        out.writeBoolean(readBackupData);
        out.writeInt(evictionPolicyId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        mapName = in.readString();
        timeToLiveSeconds = in.readInt();
        maxIdleSeconds = in.readInt();
        maxSize = in.readInt();
        maxSizePolicyId = in.readInt();
        readBackupData = in.readBoolean();
        evictionPolicyId = in.readInt();
    }

    @Override
    public int getClassId() {
        return ManagementDataSerializerHook.UPDATE_MAP_CONFIG;
    }
}
