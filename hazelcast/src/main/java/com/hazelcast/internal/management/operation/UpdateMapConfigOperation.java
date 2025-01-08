/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.internal.config.MapConfigReadOnly;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;

import static com.hazelcast.internal.cluster.Versions.V5_4;

/**
 * Operation to update map configuration from Management Center.
 */
public class UpdateMapConfigOperation extends AbstractManagementOperation implements Versioned {

    private boolean readBackupData;
    private int timeToLiveSeconds;
    private int maxIdleSeconds;
    private int maxSize;
    private int maxSizePolicyId;
    private int evictionPolicyId;
    private WanReplicationRef wanReplicationRef;
    private String mapName;

    // RU_COMPAT 5.3: Required for backwards compatibility
    private transient boolean applyWanReplicationRef;

    public UpdateMapConfigOperation() {
    }

    public UpdateMapConfigOperation(String mapName, int timeToLiveSeconds, int maxIdleSeconds,
                                    int maxSize, int maxSizePolicyId, boolean readBackupData,
                                    int evictionPolicyId, boolean applyWanReplicationRef,
                                    WanReplicationRef wanReplicationRef) {
        this.mapName = mapName;
        this.timeToLiveSeconds = timeToLiveSeconds;
        this.maxIdleSeconds = maxIdleSeconds;
        this.maxSize = maxSize;
        this.maxSizePolicyId = maxSizePolicyId;
        this.readBackupData = readBackupData;
        this.evictionPolicyId = evictionPolicyId;
        this.applyWanReplicationRef = applyWanReplicationRef;
        this.wanReplicationRef = wanReplicationRef;
    }

    @Override
    public void run() throws Exception {
        MapService service = getService();
        MapConfig oldConfig = service.getMapServiceContext().getMapContainer(mapName).getMapConfig();
        MapConfig newConfig = new MapConfig(oldConfig);
        newConfig.setTimeToLiveSeconds(timeToLiveSeconds);
        newConfig.setMaxIdleSeconds(maxIdleSeconds);
        newConfig.setReadBackupData(readBackupData);
        if (applyWanReplicationRef) {
            newConfig.setWanReplicationRef(wanReplicationRef);
        }

        EvictionConfig evictionConfig = newConfig.getEvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.getById(evictionPolicyId));
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.getById(maxSizePolicyId));
        evictionConfig.setSize(maxSize);

        MapContainer mapContainer = service.getMapServiceContext().getMapContainer(mapName);
        // It is possible for applying the config to fail (i.e. due to invalid WanReplicationRef merge policy),
        //  so it's important we restore the config to its previous version in this scenario
        try {
            MapConfigReadOnly readOnlyConfig = new MapConfigReadOnly(newConfig);
            applyMapConfig(mapContainer, readOnlyConfig);
        } catch (Exception ex) {
            applyMapConfig(mapContainer, oldConfig);
            throw new InvalidConfigurationException("Applying the MapConfig failed: " + ex.getMessage(), ex);
        }
    }

    private void applyMapConfig(MapContainer mapContainer, MapConfig config) {
        mapContainer.setMapConfig(config);
        mapContainer.initEvictor();
        if (applyWanReplicationRef) {
            mapContainer.getWanContext().setMapConfig(config);
            mapContainer.getWanContext().start();
        }
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

        // RU_COMPAT_5_3
        if (out.getVersion().isGreaterOrEqual(V5_4)) {
            out.writeObject(wanReplicationRef);
        }
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

        // RU_COMPAT_5_3
        if (in.getVersion().isGreaterOrEqual(V5_4)) {
            wanReplicationRef = in.readObject();
            applyWanReplicationRef = true;
        } else {
            wanReplicationRef = null;
            applyWanReplicationRef = false;
        }
    }

    @Override
    public int getClassId() {
        return ManagementDataSerializerHook.UPDATE_MAP_CONFIG;
    }
}
