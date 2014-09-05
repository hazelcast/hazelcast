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

package com.hazelcast.management.operation;

import com.hazelcast.config.MapConfig;
import com.hazelcast.management.MapConfigAdapter;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import java.io.IOException;

/**
 * Operation to update map configuration from Management Center.
 */
public class UpdateMapConfigOperation extends Operation {

    private String mapName;
    private MapConfig mapConfig;

    public UpdateMapConfigOperation() {
    }

    public UpdateMapConfigOperation(String mapName, MapConfig mapConfig) {
        this.mapName = mapName;
        this.mapConfig = mapConfig;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        MapService service = getService();
        MapConfig oldConfig = service.getMapServiceContext().getMapContainer(mapName).getMapConfig();
        MapConfig newConfig = new MapConfig(oldConfig);
        newConfig.setTimeToLiveSeconds(mapConfig.getTimeToLiveSeconds());
        newConfig.setMaxIdleSeconds(mapConfig.getMaxIdleSeconds());
        newConfig.setEvictionPolicy(mapConfig.getEvictionPolicy());
        newConfig.setEvictionPercentage(mapConfig.getEvictionPercentage());
        newConfig.setMinEvictionCheckMillis(mapConfig.getMinEvictionCheckMillis());
        newConfig.setReadBackupData(mapConfig.isReadBackupData());
        newConfig.setBackupCount(mapConfig.getBackupCount());
        newConfig.setAsyncBackupCount(mapConfig.getAsyncBackupCount());
        newConfig.setMaxSizeConfig(mapConfig.getMaxSizeConfig());
        service.getMapServiceContext().getMapContainer(mapName).setMapConfig(newConfig.getAsReadOnly());
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        new MapConfigAdapter(mapConfig).writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        MapConfigAdapter adapter = new MapConfigAdapter();
        adapter.readData(in);
        mapConfig = adapter.getMapConfig();
    }
}
