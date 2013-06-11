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

package com.hazelcast.management;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * @mdogan 6/11/13
 */
public class MapConfigAdapter implements DataSerializable {

    private MapConfig config;

    public MapConfigAdapter() {
    }

    public MapConfigAdapter(MapConfig mapConfig) {
        this.config = mapConfig;
    }

    public void readData(ObjectDataInput in) throws IOException {
        config = new MapConfig();
        config.setName(in.readUTF());
        config.setInMemoryFormat(MapConfig.InMemoryFormat.valueOf(in.readUTF()));
        config.setBackupCount(in.readInt());
        config.setAsyncBackupCount(in.readInt());
        config.setEvictionPercentage(in.readInt());
        config.setTimeToLiveSeconds(in.readInt());
        config.setMaxIdleSeconds(in.readInt());
        config.setMaxSizeConfig(new MaxSizeConfig().setSize(in.readInt())
                .setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.valueOf(in.readUTF())));
        config.setReadBackupData(in.readBoolean());
        config.setEvictionPolicy(MapConfig.EvictionPolicy.valueOf(in.readUTF()));
        config.setMergePolicy(in.readUTF());
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(config.getName());
        out.writeUTF(config.getInMemoryFormat().toString());
        out.writeInt(config.getBackupCount());
        out.writeInt(config.getAsyncBackupCount());
        out.writeInt(config.getEvictionPercentage());
        out.writeInt(config.getTimeToLiveSeconds());
        out.writeInt(config.getMaxIdleSeconds());
        out.writeInt(config.getMaxSizeConfig().getSize());
        out.writeUTF(config.getMaxSizeConfig().getMaxSizePolicy().toString());
        out.writeBoolean(config.isReadBackupData());
        out.writeUTF(config.getEvictionPolicy().name());
        out.writeUTF(config.getMergePolicy());
    }

    public MapConfig getMapConfig() {
        return config;
    }
}
